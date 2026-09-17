package com.chronos.scheduler.service;

import com.chronos.scheduler.config.LockConfiguration;
import com.chronos.scheduler.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Service for managing task assignment with distributed locking.
 * Ensures that only one worker can claim a task at a time, preventing race conditions.
 */
@Service
public class TaskAssignmentService {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskAssignmentService.class);
    
    private final DistributedLock distributedLock;
    private final LockConfiguration.LockProperties lockProperties;
    
    public TaskAssignmentService(
            DistributedLock distributedLock,
            LockConfiguration.LockProperties lockProperties) {
        this.distributedLock = distributedLock;
        this.lockProperties = lockProperties;
    }
    
    /**
     * Attempts to claim a task for execution by acquiring a distributed lock.
     * 
     * @param taskId the unique identifier of the task to claim
     * @param workerId the identifier of the worker attempting to claim the task
     * @return TaskClaimResult indicating success or failure with reason
     */
    public TaskClaimResult tryClaimTask(String taskId, String workerId) {
        String lockKey = lockProperties.buildTaskLockKey(taskId);
        String lockValue = buildLockValue(workerId);
        
        logger.debug("Attempting to claim task: taskId={}, workerId={}, lockKey={}", 
                taskId, workerId, lockKey);
        
        // Check if task is already locked
        if (distributedLock.isLocked(lockKey)) {
            Optional<String> currentHolder = distributedLock.getLockHolder(lockKey);
            String holder = currentHolder.orElse("unknown");
            
            logger.debug("Task already claimed: taskId={}, holder={}", taskId, holder);
            return TaskClaimResult.alreadyClaimed(taskId, holder);
        }
        
        // Try to acquire lock with retry
        boolean acquired = tryAcquireWithRetry(lockKey, lockValue);
        
        if (acquired) {
            logger.info("Task claimed successfully: taskId={}, workerId={}", taskId, workerId);
            return TaskClaimResult.success(taskId, workerId, lockValue);
        } else {
            logger.warn("Failed to claim task after retries: taskId={}, workerId={}", taskId, workerId);
            return TaskClaimResult.failed(taskId, "Failed to acquire lock after retries");
        }
    }
    
    /**
     * Releases a task lock when execution is complete or failed.
     * 
     * @param taskId the unique identifier of the task
     * @param lockValue the lock value (must match the one used during acquisition)
     * @return true if the lock was successfully released, false otherwise
     */
    public boolean releaseTaskLock(String taskId, String lockValue) {
        String lockKey = lockProperties.buildTaskLockKey(taskId);
        
        logger.debug("Attempting to release task lock: taskId={}, lockKey={}", taskId, lockKey);
        
        boolean released = distributedLock.release(lockKey, lockValue);
        
        if (released) {
            logger.info("Task lock released: taskId={}", taskId);
        } else {
            logger.warn("Failed to release task lock: taskId={} (may have expired or not owned)", taskId);
        }
        
        return released;
    }
    
    /**
     * Extends the TTL of a task lock during long-running execution.
     * Should be called periodically to prevent lock expiration.
     * 
     * @param taskId the unique identifier of the task
     * @param lockValue the lock value (must match the one used during acquisition)
     * @return true if the lock TTL was extended, false otherwise
     */
    public boolean extendTaskLock(String taskId, String lockValue) {
        String lockKey = lockProperties.buildTaskLockKey(taskId);
        Duration extension = lockProperties.getTaskLockTtl();
        
        logger.debug("Attempting to extend task lock: taskId={}, extension={}", taskId, extension);
        
        boolean extended = distributedLock.extend(lockKey, lockValue, extension);
        
        if (extended) {
            logger.debug("Task lock extended: taskId={}, extension={}", taskId, extension);
        } else {
            logger.warn("Failed to extend task lock: taskId={} (may have expired or not owned)", taskId);
        }
        
        return extended;
    }
    
    /**
     * Checks if a task is currently locked.
     * 
     * @param taskId the unique identifier of the task
     * @return true if the task is locked, false otherwise
     */
    public boolean isTaskLocked(String taskId) {
        String lockKey = lockProperties.buildTaskLockKey(taskId);
        return distributedLock.isLocked(lockKey);
    }
    
    /**
     * Gets the current holder of a task lock.
     * 
     * @param taskId the unique identifier of the task
     * @return Optional containing the worker ID if the task is locked, empty otherwise
     */
    public Optional<String> getTaskLockHolder(String taskId) {
        String lockKey = lockProperties.buildTaskLockKey(taskId);
        return distributedLock.getLockHolder(lockKey)
                .map(this::extractWorkerIdFromLockValue);
    }
    
    /**
     * Attempts to acquire a lock with configurable retry logic.
     */
    private boolean tryAcquireWithRetry(String lockKey, String lockValue) {
        int maxAttempts = lockProperties.getMaxRetryAttempts();
        Duration retryDelay = lockProperties.getRetryDelay();
        Duration ttl = lockProperties.getTaskLockTtl();
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean acquired = distributedLock.tryAcquire(lockKey, lockValue, ttl);
            
            if (acquired) {
                return true;
            }
            
            // Don't sleep after last attempt
            if (attempt < maxAttempts) {
                logger.debug("Lock acquisition attempt {} failed, retrying after {}ms", 
                        attempt, retryDelay.toMillis());
                try {
                    Thread.sleep(retryDelay.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted during lock retry sleep", e);
                    return false;
                }
            }
        }
        
        return false;
    }
    
    /**
     * Builds a lock value that includes worker ID and a unique identifier.
     * Format: workerId:uuid
     */
    private String buildLockValue(String workerId) {
        return workerId + ":" + UUID.randomUUID().toString();
    }
    
    /**
     * Extracts worker ID from a lock value.
     */
    private String extractWorkerIdFromLockValue(String lockValue) {
        int separatorIndex = lockValue.indexOf(':');
        if (separatorIndex > 0) {
            return lockValue.substring(0, separatorIndex);
        }
        return lockValue;
    }
    
    /**
     * Result of a task claim attempt.
     */
    public static class TaskClaimResult {
        private final boolean success;
        private final String taskId;
        private final String workerId;
        private final String lockValue;
        private final String failureReason;
        
        private TaskClaimResult(boolean success, String taskId, String workerId, 
                               String lockValue, String failureReason) {
            this.success = success;
            this.taskId = taskId;
            this.workerId = workerId;
            this.lockValue = lockValue;
            this.failureReason = failureReason;
        }
        
        public static TaskClaimResult success(String taskId, String workerId, String lockValue) {
            return new TaskClaimResult(true, taskId, workerId, lockValue, null);
        }
        
        public static TaskClaimResult alreadyClaimed(String taskId, String holder) {
            return new TaskClaimResult(false, taskId, null, null, 
                    "Task already claimed by: " + holder);
        }
        
        public static TaskClaimResult failed(String taskId, String reason) {
            return new TaskClaimResult(false, taskId, null, null, reason);
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getTaskId() {
            return taskId;
        }
        
        public String getWorkerId() {
            return workerId;
        }
        
        public String getLockValue() {
            return lockValue;
        }
        
        public String getFailureReason() {
            return failureReason;
        }
        
        @Override
        public String toString() {
            if (success) {
                return String.format("TaskClaimResult{success=true, taskId='%s', workerId='%s'}", 
                        taskId, workerId);
            } else {
                return String.format("TaskClaimResult{success=false, taskId='%s', reason='%s'}", 
                        taskId, failureReason);
            }
        }
    }
}
