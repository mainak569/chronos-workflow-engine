package com.chronos.scheduler.service;

import com.chronos.scheduler.config.LockConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Service for cleaning up orphaned locks.
 * Runs periodically to detect and remove locks from crashed or unresponsive workers.
 */
@Service
public class LockCleanupService {
    
    private static final Logger logger = LoggerFactory.getLogger(LockCleanupService.class);
    
    private final RedisTemplate<String, String> redisTemplate;
    private final LockConfiguration.LockProperties lockProperties;
    
    public LockCleanupService(
            RedisTemplate<String, String> redisTemplate,
            LockConfiguration.LockProperties lockProperties) {
        this.redisTemplate = redisTemplate;
        this.lockProperties = lockProperties;
    }
    
    /**
     * Scheduled task to clean up orphaned locks.
     * Runs at the configured cleanup interval (default: every 1 minute).
     */
    @Scheduled(fixedDelayString = "#{@lockProperties.getCleanupInterval().toMillis()}")
    public void cleanupOrphanedLocks() {
        logger.debug("Starting orphaned lock cleanup");
        
        try {
            int cleanedTaskLocks = cleanupLocksByPrefix(lockProperties.getTaskLockPrefix());
            int cleanedExecutionLocks = cleanupLocksByPrefix(lockProperties.getExecutionLockPrefix());
            
            int totalCleaned = cleanedTaskLocks + cleanedExecutionLocks;
            
            if (totalCleaned > 0) {
                logger.info("Orphaned lock cleanup completed: taskLocks={}, executionLocks={}, total={}", 
                        cleanedTaskLocks, cleanedExecutionLocks, totalCleaned);
            } else {
                logger.debug("Orphaned lock cleanup completed: no locks cleaned");
            }
            
        } catch (Exception e) {
            logger.error("Error during orphaned lock cleanup", e);
        }
    }
    
    /**
     * Clean up locks with a specific prefix that are older than the threshold.
     * 
     * @param prefix the lock key prefix to scan
     * @return the number of locks cleaned up
     */
    private int cleanupLocksByPrefix(String prefix) {
        int cleanedCount = 0;
        
        try {
            // Scan for all keys with the given prefix
            Set<String> lockKeys = redisTemplate.keys(prefix + "*");
            
            if (lockKeys == null || lockKeys.isEmpty()) {
                return 0;
            }
            
            logger.debug("Found {} locks with prefix: {}", lockKeys.size(), prefix);
            
            Duration threshold = lockProperties.getOrphanedLockThreshold();
            
            for (String lockKey : lockKeys) {
                if (isOrphanedLock(lockKey, threshold)) {
                    boolean deleted = deleteLockSafely(lockKey);
                    if (deleted) {
                        cleanedCount++;
                        logger.info("Cleaned up orphaned lock: key={}", lockKey);
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error cleaning up locks with prefix: {}", prefix, e);
        }
        
        return cleanedCount;
    }
    
    /**
     * Checks if a lock is orphaned based on its TTL.
     * A lock is considered orphaned if its remaining TTL is less than expected
     * or if it has no expiration set (which shouldn't happen but is defensive).
     * 
     * @param lockKey the lock key to check
     * @param threshold the threshold for considering a lock orphaned
     * @return true if the lock appears to be orphaned
     */
    private boolean isOrphanedLock(String lockKey, Duration threshold) {
        try {
            // Get the TTL of the lock
            Long ttlSeconds = redisTemplate.getExpire(lockKey, TimeUnit.SECONDS);
            
            if (ttlSeconds == null) {
                // Key doesn't exist
                return false;
            }
            
            if (ttlSeconds == -1) {
                // Key exists but has no expiration (should never happen with our locks)
                logger.warn("Lock exists without expiration (orphaned): key={}", lockKey);
                return true;
            }
            
            if (ttlSeconds == -2) {
                // Key doesn't exist (race condition - already expired)
                return false;
            }
            
            // Check if the lock is old (close to expiration)
            // If TTL is very low, it might be from a crashed worker that didn't release it
            Duration remainingTtl = Duration.ofSeconds(ttlSeconds);
            Duration originalTtl = lockProperties.getTaskLockTtl();
            Duration elapsed = originalTtl.minus(remainingTtl);
            
            // Consider orphaned if more than threshold has elapsed
            boolean isOrphaned = elapsed.compareTo(threshold) > 0;
            
            if (isOrphaned) {
                logger.debug("Lock identified as orphaned: key={}, remainingTtl={}s, elapsed={}s, threshold={}s",
                        lockKey, ttlSeconds, elapsed.toSeconds(), threshold.toSeconds());
            }
            
            return isOrphaned;
            
        } catch (Exception e) {
            logger.error("Error checking if lock is orphaned: key={}", lockKey, e);
            return false;
        }
    }
    
    /**
     * Safely deletes a lock key.
     * This is a simple delete - in production, you might want additional checks
     * like verifying the worker that owns the lock is actually down.
     * 
     * @param lockKey the lock key to delete
     * @return true if the lock was deleted, false otherwise
     */
    private boolean deleteLockSafely(String lockKey) {
        try {
            Boolean deleted = redisTemplate.delete(lockKey);
            return Boolean.TRUE.equals(deleted);
        } catch (Exception e) {
            logger.error("Error deleting orphaned lock: key={}", lockKey, e);
            return false;
        }
    }
    
    /**
     * Manually trigger cleanup for a specific lock key.
     * Useful for administrative operations.
     * 
     * @param lockKey the lock key to clean up
     * @return true if the lock was cleaned up, false otherwise
     */
    public boolean cleanupSpecificLock(String lockKey) {
        logger.info("Manual cleanup requested for lock: key={}", lockKey);
        
        try {
            // Check if it exists
            if (!Boolean.TRUE.equals(redisTemplate.hasKey(lockKey))) {
                logger.info("Lock does not exist: key={}", lockKey);
                return false;
            }
            
            // Delete it
            boolean deleted = deleteLockSafely(lockKey);
            
            if (deleted) {
                logger.info("Successfully cleaned up lock: key={}", lockKey);
            } else {
                logger.warn("Failed to clean up lock: key={}", lockKey);
            }
            
            return deleted;
            
        } catch (Exception e) {
            logger.error("Error during manual lock cleanup: key={}", lockKey, e);
            return false;
        }
    }
    
    /**
     * Get statistics about current locks.
     * 
     * @return LockStatistics containing counts of locks by type
     */
    public LockStatistics getLockStatistics() {
        try {
            Set<String> taskLocks = redisTemplate.keys(lockProperties.getTaskLockPrefix() + "*");
            Set<String> executionLocks = redisTemplate.keys(lockProperties.getExecutionLockPrefix() + "*");
            
            int taskLockCount = (taskLocks != null) ? taskLocks.size() : 0;
            int executionLockCount = (executionLocks != null) ? executionLocks.size() : 0;
            
            return new LockStatistics(taskLockCount, executionLockCount);
            
        } catch (Exception e) {
            logger.error("Error getting lock statistics", e);
            return new LockStatistics(0, 0);
        }
    }
    
    /**
     * Statistics about current locks in Redis.
     */
    public static class LockStatistics {
        private final int taskLockCount;
        private final int executionLockCount;
        
        public LockStatistics(int taskLockCount, int executionLockCount) {
            this.taskLockCount = taskLockCount;
            this.executionLockCount = executionLockCount;
        }
        
        public int getTaskLockCount() {
            return taskLockCount;
        }
        
        public int getExecutionLockCount() {
            return executionLockCount;
        }
        
        public int getTotalLockCount() {
            return taskLockCount + executionLockCount;
        }
        
        @Override
        public String toString() {
            return String.format("LockStatistics{taskLocks=%d, executionLocks=%d, total=%d}", 
                    taskLockCount, executionLockCount, getTotalLockCount());
        }
    }
}
