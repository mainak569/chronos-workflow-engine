package com.chronos.worker.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Service for acquiring and managing task execution locks.
 * Ensures only one worker can execute a task at a time using distributed locking.
 * 
 * Lock Key Format: chronos:lock:task:{taskId}
 * Lock Value Format: {workerId}:{uuid}
 * 
 * Guarantees:
 * - Atomic lock acquisition using Redis SET NX EX
 * - Safe lock release (only owner can release)
 * - Lock expiration prevents deadlock if worker crashes
 * - Unique ownership tokens prevent accidental release
 * 
 * Limitations:
 * - Does NOT provide exactly-once execution (at-least-once with idempotency)
 * - Network partitions may cause lock expiry while task is still executing
 * - Clock skew between Redis and workers may affect TTL accuracy
 */
@Service
public class TaskLockService {
    
    private static final Logger log = LoggerFactory.getLogger(TaskLockService.class);
    
    private static final String LOCK_KEY_PREFIX = "chronos:lock:task:";
    
    private final DistributedLock distributedLock;
    
    @Value("${chronos.lock.task-lock-ttl:5m}")
    private Duration defaultLockTtl;
    
    public TaskLockService(DistributedLock distributedLock) {
        this.distributedLock = distributedLock;
    }
    
    /**
     * Attempt to claim a task for execution.
     * Returns a lock token if successful, empty if the task is already claimed.
     * 
     * This operation is atomic and thread-safe across all workers.
     * 
     * @param taskId the unique task identifier
     * @param workerId the worker attempting to claim the task
     * @return Optional containing the lock token if claimed successfully, empty if already claimed
     */
    public Optional<LockToken> claimTask(String taskId, String workerId) {
        return claimTask(taskId, workerId, defaultLockTtl);
    }
    
    /**
     * Attempt to claim a task for execution with a custom TTL.
     * 
     * @param taskId the unique task identifier
     * @param workerId the worker attempting to claim the task
     * @param ttl time-to-live for the lock (should be longer than expected task execution time)
     * @return Optional containing the lock token if claimed successfully, empty if already claimed
     */
    public Optional<LockToken> claimTask(String taskId, String workerId, Duration ttl) {
        String lockKey = buildLockKey(taskId);
        LockToken token = LockToken.create(workerId);
        
        log.debug("Attempting to claim task: taskId={}, workerId={}, token={}, ttl={}",
                taskId, workerId, token.getToken(), ttl);
        
        boolean acquired = distributedLock.tryAcquire(lockKey, token.getToken(), ttl);
        
        if (acquired) {
            log.info("Task claimed successfully: taskId={}, workerId={}, token={}",
                    taskId, workerId, token.getToken());
            return Optional.of(token);
        } else {
            // Get the current holder for debugging
            Optional<String> currentHolder = distributedLock.getLockHolder(lockKey);
            log.info("Task claim failed (already claimed): taskId={}, workerId={}, currentHolder={}",
                    taskId, workerId, currentHolder.orElse("unknown"));
            return Optional.empty();
        }
    }
    
    /**
     * Release a task lock after execution completes.
     * Only succeeds if the provided token matches the current lock holder.
     * 
     * @param taskId the unique task identifier
     * @param token the lock token obtained from claimTask()
     * @return true if released successfully, false if not owned or already released
     */
    public boolean releaseTask(String taskId, LockToken token) {
        String lockKey = buildLockKey(taskId);
        
        log.debug("Attempting to release task: taskId={}, token={}", taskId, token.getToken());
        
        boolean released = distributedLock.release(lockKey, token.getToken());
        
        if (released) {
            log.info("Task released successfully: taskId={}, workerId={}", 
                    taskId, token.getWorkerId());
        } else {
            // This can happen if:
            // 1. Lock already expired (normal if task took longer than TTL)
            // 2. Lock was force-released
            // 3. Token doesn't match (should never happen with proper usage)
            log.warn("Task release failed: taskId={}, token={} (lock may have expired)",
                    taskId, token.getToken());
        }
        
        return released;
    }
    
    /**
     * Extend the lock TTL if the task is taking longer than expected.
     * Only succeeds if the provided token matches the current lock holder.
     * 
     * @param taskId the unique task identifier
     * @param token the lock token obtained from claimTask()
     * @param additionalTtl the additional time to add to the lock
     * @return true if extended successfully, false if not owned or expired
     */
    public boolean extendLock(String taskId, LockToken token, Duration additionalTtl) {
        String lockKey = buildLockKey(taskId);
        
        log.debug("Attempting to extend lock: taskId={}, token={}, additionalTtl={}",
                taskId, token.getToken(), additionalTtl);
        
        boolean extended = distributedLock.extend(lockKey, token.getToken(), additionalTtl);
        
        if (extended) {
            log.info("Lock extended successfully: taskId={}, workerId={}, additionalTtl={}",
                    taskId, token.getWorkerId(), additionalTtl);
        } else {
            log.warn("Lock extension failed: taskId={}, token={} (lock may have expired)",
                    taskId, token.getToken());
        }
        
        return extended;
    }
    
    /**
     * Check if a task is currently locked by any worker.
     * 
     * @param taskId the unique task identifier
     * @return true if the task is currently locked, false otherwise
     */
    public boolean isTaskLocked(String taskId) {
        String lockKey = buildLockKey(taskId);
        return distributedLock.isLocked(lockKey);
    }
    
    /**
     * Get the current lock holder for a task, if any.
     * 
     * @param taskId the unique task identifier
     * @return Optional containing the worker ID if locked, empty if not locked
     */
    public Optional<String> getTaskLockHolder(String taskId) {
        String lockKey = buildLockKey(taskId);
        return distributedLock.getLockHolder(lockKey)
                .map(tokenString -> {
                    LockToken token = LockToken.parse(tokenString);
                    return token != null ? token.getWorkerId() : tokenString;
                });
    }
    
    /**
     * Force-release a task lock (admin operation).
     * WARNING: Only use for administrative cleanup. May cause duplicate execution!
     * 
     * @param taskId the unique task identifier
     * @return true if the lock was removed, false if not locked
     */
    public boolean forceRelease(String taskId) {
        String lockKey = buildLockKey(taskId);
        
        Optional<String> currentHolder = distributedLock.getLockHolder(lockKey);
        if (currentHolder.isEmpty()) {
            return false;
        }
        
        log.warn("Force-releasing task lock: taskId={}, currentHolder={}",
                taskId, currentHolder.get());
        
        // Create a token matching the current holder to release it
        return distributedLock.release(lockKey, currentHolder.get());
    }
    
    private String buildLockKey(String taskId) {
        return LOCK_KEY_PREFIX + taskId;
    }
}
