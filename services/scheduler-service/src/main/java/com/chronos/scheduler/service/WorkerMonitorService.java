package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.WorkerMetadata;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Monitors worker heartbeats and marks workers as unavailable when heartbeats expire.
 * Runs periodically to detect failed workers.
 */
@Service
public class WorkerMonitorService {
    
    private static final Logger log = LoggerFactory.getLogger(WorkerMonitorService.class);
    
    private static final String METADATA_KEY_PREFIX = "worker:metadata:";
    private static final String HEARTBEAT_KEY_PREFIX = "worker:heartbeat:";
    
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final LeaderElectionService leaderElectionService;
    private final WorkerFailureHandler workerFailureHandler;
    
    public WorkerMonitorService(RedisTemplate<String, String> redisTemplate,
                                ObjectMapper objectMapper,
                                LeaderElectionService leaderElectionService,
                                WorkerFailureHandler workerFailureHandler) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.leaderElectionService = leaderElectionService;
        this.workerFailureHandler = workerFailureHandler;
    }
    
    /**
     * Check for expired worker heartbeats every 15 seconds.
     * Marks workers as unavailable if their heartbeat has expired.
     */
    @Scheduled(fixedRateString = "${worker.monitor.interval:15000}")
    public void monitorWorkerHeartbeats() {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        try {
            List<WorkerMetadata> expiredWorkers = findWorkersWithExpiredHeartbeats();
            
            if (expiredWorkers.isEmpty()) {
                log.debug("No workers with expired heartbeats");
                return;
            }
            
            log.warn("Found {} workers with expired heartbeats", expiredWorkers.size());
            
            for (WorkerMetadata worker : expiredWorkers) {
                markWorkerUnavailable(worker.getWorkerId());
            }
            
        } catch (Exception e) {
            log.error("Error monitoring worker heartbeats", e);
        }
    }
    
    /**
     * Find all workers with expired heartbeats.
     * A worker is considered expired if:
     * 1. It has metadata in Redis
     * 2. Its heartbeat key doesn't exist or has expired
     * 3. It's currently in AVAILABLE or BUSY status
     */
    private List<WorkerMetadata> findWorkersWithExpiredHeartbeats() {
        List<WorkerMetadata> expiredWorkers = new ArrayList<>();
        
        // Get all worker metadata keys
        Set<String> metadataKeys = redisTemplate.keys(METADATA_KEY_PREFIX + "*");
        if (metadataKeys == null || metadataKeys.isEmpty()) {
            return expiredWorkers;
        }
        
        for (String metadataKey : metadataKeys) {
            try {
                String workerId = metadataKey.substring(METADATA_KEY_PREFIX.length());
                WorkerMetadata worker = getWorkerMetadata(workerId);
                
                if (worker == null) {
                    continue;
                }
                
                // Only check workers that should be sending heartbeats
                if (!shouldHaveHeartbeat(worker)) {
                    continue;
                }
                
                // Check if heartbeat has expired
                if (isHeartbeatExpired(workerId)) {
                    expiredWorkers.add(worker);
                }
                
            } catch (Exception e) {
                log.error("Error checking worker heartbeat for key: {}", metadataKey, e);
            }
        }
        
        return expiredWorkers;
    }
    
    /**
     * Check if worker should have an active heartbeat.
     * Workers in AVAILABLE or BUSY status should have heartbeats.
     */
    private boolean shouldHaveHeartbeat(WorkerMetadata worker) {
        return worker.getStatus() == WorkerMetadata.WorkerStatus.AVAILABLE ||
               worker.getStatus() == WorkerMetadata.WorkerStatus.BUSY;
    }
    
    /**
     * Check if heartbeat has expired for a worker.
     */
    private boolean isHeartbeatExpired(String workerId) {
        String heartbeatKey = HEARTBEAT_KEY_PREFIX + workerId;
        return !Boolean.TRUE.equals(redisTemplate.hasKey(heartbeatKey));
    }
    
    /**
     * Get worker metadata from Redis.
     */
    private WorkerMetadata getWorkerMetadata(String workerId) {
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
     * Mark worker as unavailable.
     * Updates status in Redis metadata and requeues the tasks it was running.
     */
    private void markWorkerUnavailable(String workerId) {
        try {
            WorkerMetadata worker = getWorkerMetadata(workerId);
            if (worker == null) {
                return;
            }
            
            log.warn("Marking worker as unavailable due to heartbeat expiry: {} (was {})", 
                    workerId, worker.getStatus());
            
            // Update status to UNAVAILABLE
            worker.markUnavailable();
            
            // Save updated metadata
            String metadataKey = METADATA_KEY_PREFIX + workerId;
            String metadataJson = objectMapper.writeValueAsString(worker);
            redisTemplate.opsForValue().set(metadataKey, metadataJson);
            
            // Tasks the dead worker was running will never report back: run them again
            workerFailureHandler.handleWorkerLost(workerId);

        } catch (Exception e) {
            log.error("Failed to mark worker as unavailable: {}", workerId, e);
        }
    }
}
