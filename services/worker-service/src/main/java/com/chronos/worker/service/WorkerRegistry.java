package com.chronos.worker.service;

import com.chronos.worker.domain.WorkerMetadata;
import com.chronos.worker.domain.WorkerStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service for managing worker registration, heartbeats, and state in Redis.
 * 
 * Redis Key Structure:
 * - worker:metadata:{workerId} -> JSON of WorkerMetadata
 * - worker:heartbeat:{workerId} -> timestamp with TTL
 * - worker:index:status:{status} -> Set of workerIds
 * - worker:index:taskType:{taskType} -> Set of workerIds
 */
@Service
public class WorkerRegistry {
    
    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);
    
    private static final String METADATA_KEY_PREFIX = "worker:metadata:";
    private static final String HEARTBEAT_KEY_PREFIX = "worker:heartbeat:";
    private static final String STATUS_INDEX_PREFIX = "worker:index:status:";
    private static final String TASK_TYPE_INDEX_PREFIX = "worker:index:taskType:";
    
    // Heartbeat TTL: 30 seconds (workers should heartbeat every 10 seconds)
    private static final long HEARTBEAT_TTL_SECONDS = 30;
    
    /**
     * Tasks currently executing per worker; the worker is BUSY while this is above zero.
     */
    private final Map<String, Integer> inFlightTasks = new ConcurrentHashMap<>();
    
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    
    public WorkerRegistry(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Register a new worker.
     * Creates metadata entry, heartbeat key with TTL, and index entries.
     */
    public void registerWorker(WorkerMetadata worker) {
        if (worker.getStatus() != WorkerStatus.REGISTERING) {
            throw new IllegalArgumentException("Worker must be in REGISTERING status");
        }
        
        log.info("Registering worker: {} with task types: {}", 
                worker.getWorkerId(), worker.getSupportedTaskTypes());
        
        try {
            // Mark worker as available
            worker.markAvailable();
            
            // Store metadata
            String metadataKey = METADATA_KEY_PREFIX + worker.getWorkerId();
            String metadataJson = objectMapper.writeValueAsString(worker);
            redisTemplate.opsForValue().set(metadataKey, metadataJson);
            
            // Create heartbeat key with TTL
            updateHeartbeat(worker.getWorkerId());
            
            // Update indexes
            updateIndexes(worker);
            
            log.info("Worker registered successfully: {}", worker.getWorkerId());
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize worker metadata for {}", worker.getWorkerId(), e);
            throw new RuntimeException("Failed to register worker", e);
        }
    }
    
    /**
     * Update worker heartbeat timestamp and refresh TTL.
     */
    public void updateHeartbeat(String workerId) {
        String heartbeatKey = HEARTBEAT_KEY_PREFIX + workerId;
        String timestamp = Instant.now().toString();
        
        redisTemplate.opsForValue().set(heartbeatKey, timestamp, HEARTBEAT_TTL_SECONDS, TimeUnit.SECONDS);
        
        // Update lastHeartbeat in metadata
        WorkerMetadata worker = getWorker(workerId);
        if (worker != null) {
            worker.updateHeartbeat();
            saveWorkerMetadata(worker);
        }
        
        log.debug("Heartbeat updated for worker: {}", workerId);
    }
    
    /**
     * Mark worker as busy (executing a task).
     */
    public void markWorkerBusy(String workerId, String executionId, String taskId) {
        int inFlight = inFlightTasks.merge(workerId, 1, Integer::sum);
        
        WorkerMetadata worker = getWorker(workerId);
        if (worker == null) {
            log.warn("Cannot mark non-existent worker as busy: {}", workerId);
            return;
        }
        
        // Remove from AVAILABLE index
        removeFromStatusIndex(workerId, WorkerStatus.AVAILABLE);
        
        // Update status
        worker.markBusy(executionId, taskId);
        saveWorkerMetadata(worker);
        
        // Add to BUSY index
        addToStatusIndex(workerId, WorkerStatus.BUSY);
        
        log.debug("Worker marked as busy: {} (executionId={}, taskId={}, inFlight={})",
                workerId, executionId, taskId, inFlight);
    }
    
    /**
     * Mark worker as available (task completed).
     */
    public void markWorkerAvailable(String workerId) {
        // Stay BUSY while other tasks are still running on this worker
        int inFlight = inFlightTasks.merge(workerId, -1, (current, delta) -> Math.max(0, current + delta));
        if (inFlight > 0) {
            log.debug("Worker {} still running {} task(s), staying BUSY", workerId, inFlight);
            return;
        }
        
        WorkerMetadata worker = getWorker(workerId);
        if (worker == null) {
            log.warn("Cannot mark non-existent worker as available: {}", workerId);
            return;
        }
        
        // Remove from BUSY index
        removeFromStatusIndex(workerId, WorkerStatus.BUSY);
        
        // Update status
        worker.markAvailable();
        saveWorkerMetadata(worker);
        
        // Add to AVAILABLE index
        addToStatusIndex(workerId, WorkerStatus.AVAILABLE);
        
        log.debug("Worker marked as available: {}", workerId);
    }
    
    /**
     * Mark worker as unavailable (heartbeat expired or error).
     */
    public void markWorkerUnavailable(String workerId) {
        WorkerMetadata worker = getWorker(workerId);
        if (worker == null) {
            log.warn("Cannot mark non-existent worker as unavailable: {}", workerId);
            return;
        }
        
        WorkerStatus previousStatus = worker.getStatus();
        
        // Remove from previous status index
        removeFromStatusIndex(workerId, previousStatus);
        
        // Update status
        worker.markUnavailable();
        saveWorkerMetadata(worker);
        
        // Add to UNAVAILABLE index
        addToStatusIndex(workerId, WorkerStatus.UNAVAILABLE);
        
        log.warn("Worker marked as unavailable: {} (was {})", workerId, previousStatus);
    }
    
    /**
     * Deregister worker (graceful shutdown).
     */
    public void deregisterWorker(String workerId) {
        WorkerMetadata worker = getWorker(workerId);
        if (worker == null) {
            log.warn("Cannot deregister non-existent worker: {}", workerId);
            return;
        }
        
        log.info("Deregistering worker: {}", workerId);
        
        // Remove from indexes
        removeFromStatusIndex(workerId, worker.getStatus());
        for (String taskType : worker.getSupportedTaskTypes()) {
            removeFromTaskTypeIndex(workerId, taskType);
        }
        
        // Mark as stopped
        worker.markStopped();
        saveWorkerMetadata(worker);
        
        // Delete heartbeat key
        String heartbeatKey = HEARTBEAT_KEY_PREFIX + workerId;
        redisTemplate.delete(heartbeatKey);
        
        log.info("Worker deregistered: {}", workerId);
    }
    
    /**
     * Get worker metadata.
     */
    public WorkerMetadata getWorker(String workerId) {
        String metadataKey = METADATA_KEY_PREFIX + workerId;
        String metadataJson = redisTemplate.opsForValue().get(metadataKey);
        
        if (metadataJson == null) {
            return null;
        }
        
        try {
            return objectMapper.readValue(metadataJson, WorkerMetadata.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize worker metadata for {}", workerId, e);
            return null;
        }
    }
    
    /**
     * Get all workers with a specific status.
     */
    public List<WorkerMetadata> getWorkersByStatus(WorkerStatus status) {
        String statusIndexKey = STATUS_INDEX_PREFIX + status.name();
        Set<String> workerIds = redisTemplate.opsForSet().members(statusIndexKey);
        
        if (workerIds == null || workerIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        return workerIds.stream()
                .map(this::getWorker)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    /**
     * Get all workers supporting a specific task type.
     */
    public List<WorkerMetadata> getWorkersByTaskType(String taskType) {
        String taskTypeIndexKey = TASK_TYPE_INDEX_PREFIX + taskType;
        Set<String> workerIds = redisTemplate.opsForSet().members(taskTypeIndexKey);
        
        if (workerIds == null || workerIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        return workerIds.stream()
                .map(this::getWorker)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    /**
     * Get all available workers for a specific task type.
     */
    public List<WorkerMetadata> getAvailableWorkers(String taskType) {
        return getWorkersByTaskType(taskType).stream()
                .filter(w -> w.getStatus() == WorkerStatus.AVAILABLE)
                .collect(Collectors.toList());
    }
    
    /**
     * Check if worker heartbeat has expired.
     */
    public boolean isHeartbeatExpired(String workerId) {
        String heartbeatKey = HEARTBEAT_KEY_PREFIX + workerId;
        return !Boolean.TRUE.equals(redisTemplate.hasKey(heartbeatKey));
    }
    
    /**
     * Get all workers with expired heartbeats.
     */
    public List<WorkerMetadata> getWorkersWithExpiredHeartbeats() {
        // Get all active workers (not STOPPED or UNAVAILABLE)
        List<WorkerMetadata> activeWorkers = new ArrayList<>();
        activeWorkers.addAll(getWorkersByStatus(WorkerStatus.AVAILABLE));
        activeWorkers.addAll(getWorkersByStatus(WorkerStatus.BUSY));
        
        // Filter for expired heartbeats
        return activeWorkers.stream()
                .filter(w -> isHeartbeatExpired(w.getWorkerId()))
                .collect(Collectors.toList());
    }
    
    /**
     * Get all registered workers.
     */
    public List<WorkerMetadata> getAllWorkers() {
        Set<String> keys = redisTemplate.keys(METADATA_KEY_PREFIX + "*");
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        
        return keys.stream()
                .map(key -> key.substring(METADATA_KEY_PREFIX.length()))
                .map(this::getWorker)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    // Private helper methods
    
    private void saveWorkerMetadata(WorkerMetadata worker) {
        try {
            String metadataKey = METADATA_KEY_PREFIX + worker.getWorkerId();
            String metadataJson = objectMapper.writeValueAsString(worker);
            redisTemplate.opsForValue().set(metadataKey, metadataJson);
        } catch (JsonProcessingException e) {
            log.error("Failed to save worker metadata for {}", worker.getWorkerId(), e);
            throw new RuntimeException("Failed to save worker metadata", e);
        }
    }
    
    private void updateIndexes(WorkerMetadata worker) {
        // Add to status index
        addToStatusIndex(worker.getWorkerId(), worker.getStatus());
        
        // Add to task type indexes
        for (String taskType : worker.getSupportedTaskTypes()) {
            addToTaskTypeIndex(worker.getWorkerId(), taskType);
        }
    }
    
    private void addToStatusIndex(String workerId, WorkerStatus status) {
        String statusIndexKey = STATUS_INDEX_PREFIX + status.name();
        redisTemplate.opsForSet().add(statusIndexKey, workerId);
    }
    
    private void removeFromStatusIndex(String workerId, WorkerStatus status) {
        String statusIndexKey = STATUS_INDEX_PREFIX + status.name();
        redisTemplate.opsForSet().remove(statusIndexKey, workerId);
    }
    
    private void addToTaskTypeIndex(String workerId, String taskType) {
        String taskTypeIndexKey = TASK_TYPE_INDEX_PREFIX + taskType;
        redisTemplate.opsForSet().add(taskTypeIndexKey, workerId);
    }
    
    private void removeFromTaskTypeIndex(String workerId, String taskType) {
        String taskTypeIndexKey = TASK_TYPE_INDEX_PREFIX + taskType;
        redisTemplate.opsForSet().remove(taskTypeIndexKey, workerId);
    }
}
