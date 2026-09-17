package com.chronos.worker.lock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service for managing task execution leases.
 * 
 * A lease is an extended lock that tracks:
 * - Task ownership (which worker is executing)
 * - Execution metadata (executionId, attempt number)
 * - Expiration time (for recovery)
 * 
 * Leases enable safe task recovery when workers crash.
 * 
 * Redis Key Structure:
 * - chronos:lease:task:{taskId} → JSON of TaskLease with TTL
 */
@Service
public class TaskLeaseService {
    
    private static final Logger log = LoggerFactory.getLogger(TaskLeaseService.class);
    
    private static final String LEASE_KEY_PREFIX = "chronos:lease:task:";
    
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${chronos.task.lease-duration:5m}")
    private Duration defaultLeaseDuration;
    
    public TaskLeaseService(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Acquire a task execution lease.
     * This should be called after successfully claiming the task lock.
     * 
     * @param taskId the task identifier
     * @param executionId the workflow execution identifier
     * @param workerId the worker identifier
     * @param lockToken the lock token proving ownership
     * @param attemptNumber the current attempt number
     * @return the created lease
     */
    public TaskLease acquireLease(String taskId, String executionId, String workerId,
                                  String lockToken, int attemptNumber) {
        return acquireLease(taskId, executionId, workerId, lockToken, attemptNumber, defaultLeaseDuration);
    }
    
    /**
     * Acquire a task execution lease with custom duration.
     */
    public TaskLease acquireLease(String taskId, String executionId, String workerId,
                                  String lockToken, int attemptNumber, Duration leaseDuration) {
        TaskLease lease = new TaskLease(taskId, executionId, workerId, lockToken, attemptNumber, leaseDuration);
        
        String leaseKey = buildLeaseKey(taskId);
        
        try {
            String leaseJson = objectMapper.writeValueAsString(lease);
            redisTemplate.opsForValue().set(leaseKey, leaseJson, leaseDuration.toMillis(), TimeUnit.MILLISECONDS);
            
            log.info("Task lease acquired: taskId={}, workerId={}, executionId={}, attempt={}, duration={}",
                    taskId, workerId, executionId, attemptNumber, leaseDuration);
            
            return lease;
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize task lease: taskId={}", taskId, e);
            throw new RuntimeException("Failed to acquire task lease", e);
        }
    }
    
    /**
     * Release a task execution lease.
     * Should be called when task completes or fails.
     * 
     * @param taskId the task identifier
     * @param lockToken the lock token proving ownership
     * @return true if released, false if not owned or already released
     */
    public boolean releaseLease(String taskId, String lockToken) {
        String leaseKey = buildLeaseKey(taskId);
        
        // Get current lease
        TaskLease currentLease = getLease(taskId);
        if (currentLease == null) {
            log.debug("No lease found for task: {}", taskId);
            return false;
        }
        
        // Verify ownership
        if (!currentLease.hasToken(lockToken)) {
            log.warn("Cannot release lease: token mismatch. taskId={}, expected={}, provided={}",
                    taskId, currentLease.getLockToken(), lockToken);
            return false;
        }
        
        // Delete lease
        Boolean deleted = redisTemplate.delete(leaseKey);
        boolean success = Boolean.TRUE.equals(deleted);
        
        if (success) {
            log.info("Task lease released: taskId={}, workerId={}", taskId, currentLease.getWorkerId());
        }
        
        return success;
    }
    
    /**
     * Get the current lease for a task.
     * 
     * @param taskId the task identifier
     * @return the lease if it exists, null otherwise
     */
    public TaskLease getLease(String taskId) {
        String leaseKey = buildLeaseKey(taskId);
        String leaseJson = redisTemplate.opsForValue().get(leaseKey);
        
        if (leaseJson == null) {
            return null;
        }
        
        try {
            return objectMapper.readValue(leaseJson, TaskLease.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize task lease: taskId={}", taskId, e);
            return null;
        }
    }
    
    /**
     * Check if a task has an active (non-expired) lease.
     * 
     * @param taskId the task identifier
     * @return true if the task has an active lease
     */
    public boolean hasActiveLease(String taskId) {
        TaskLease lease = getLease(taskId);
        return lease != null && !lease.isExpired();
    }
    
    /**
     * Extend a task execution lease.
     * Useful for long-running tasks.
     * 
     * @param taskId the task identifier
     * @param lockToken the lock token proving ownership
     * @param additionalDuration the additional time to add
     * @return true if extended, false if not owned or expired
     */
    public boolean extendLease(String taskId, String lockToken, Duration additionalDuration) {
        TaskLease currentLease = getLease(taskId);
        
        if (currentLease == null) {
            log.debug("Cannot extend lease: lease not found. taskId={}", taskId);
            return false;
        }
        
        if (!currentLease.hasToken(lockToken)) {
            log.warn("Cannot extend lease: token mismatch. taskId={}", taskId);
            return false;
        }
        
        if (currentLease.isExpired()) {
            log.warn("Cannot extend lease: lease already expired. taskId={}", taskId);
            return false;
        }
        
        // Update expiration time
        currentLease.setExpiresAt(currentLease.getExpiresAt().plus(additionalDuration));
        
        // Save updated lease
        String leaseKey = buildLeaseKey(taskId);
        try {
            String leaseJson = objectMapper.writeValueAsString(currentLease);
            Duration newTtl = currentLease.getTimeUntilExpiration();
            redisTemplate.opsForValue().set(leaseKey, leaseJson, newTtl.toMillis(), TimeUnit.MILLISECONDS);
            
            log.info("Task lease extended: taskId={}, additionalDuration={}, newExpiration={}",
                    taskId, additionalDuration, currentLease.getExpiresAt());
            
            return true;
            
        } catch (JsonProcessingException e) {
            log.error("Failed to extend task lease: taskId={}", taskId, e);
            return false;
        }
    }
    
    /**
     * Find all expired leases.
     * Used by TaskRecoveryService to identify abandoned tasks.
     * 
     * @return list of expired leases
     */
    public List<TaskLease> findExpiredLeases() {
        Set<String> leaseKeys = redisTemplate.keys(LEASE_KEY_PREFIX + "*");
        
        if (leaseKeys == null || leaseKeys.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<TaskLease> expiredLeases = new ArrayList<>();
        
        for (String leaseKey : leaseKeys) {
            String leaseJson = redisTemplate.opsForValue().get(leaseKey);
            if (leaseJson == null) {
                continue; // Expired between KEYS and GET
            }
            
            try {
                TaskLease lease = objectMapper.readValue(leaseJson, TaskLease.class);
                if (lease.isExpired()) {
                    expiredLeases.add(lease);
                }
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize lease from key: {}", leaseKey, e);
            }
        }
        
        log.debug("Found {} expired leases", expiredLeases.size());
        return expiredLeases;
    }
    
    /**
     * Force-release a lease (admin operation).
     * WARNING: Only use for administrative cleanup.
     * 
     * @param taskId the task identifier
     * @return true if the lease was removed
     */
    public boolean forceReleaseLease(String taskId) {
        String leaseKey = buildLeaseKey(taskId);
        
        TaskLease currentLease = getLease(taskId);
        if (currentLease == null) {
            return false;
        }
        
        log.warn("Force-releasing task lease: taskId={}, workerId={}", 
                taskId, currentLease.getWorkerId());
        
        Boolean deleted = redisTemplate.delete(leaseKey);
        return Boolean.TRUE.equals(deleted);
    }
    
    /**
     * Get all active leases for a specific worker.
     * 
     * @param workerId the worker identifier
     * @return list of active leases held by the worker
     */
    public List<TaskLease> getWorkerLeases(String workerId) {
        Set<String> leaseKeys = redisTemplate.keys(LEASE_KEY_PREFIX + "*");
        
        if (leaseKeys == null || leaseKeys.isEmpty()) {
            return Collections.emptyList();
        }
        
        return leaseKeys.stream()
                .map(key -> redisTemplate.opsForValue().get(key))
                .filter(Objects::nonNull)
                .map(json -> {
                    try {
                        return objectMapper.readValue(json, TaskLease.class);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .filter(lease -> lease.belongsTo(workerId))
                .collect(Collectors.toList());
    }
    
    private String buildLeaseKey(String taskId) {
        return LEASE_KEY_PREFIX + taskId;
    }
}
