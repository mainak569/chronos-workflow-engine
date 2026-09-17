package com.chronos.worker.lock;

import java.time.Duration;
import java.util.Optional;

/**
 * Interface for distributed locking operations.
 * Provides lock acquisition, release, and extension capabilities with TTL support.
 */
public interface DistributedLock {
    
    /**
     * Attempts to acquire a lock with the given key.
     * 
     * @param lockKey the unique identifier for the lock
     * @param lockValue the value to set (typically a unique identifier for the lock holder)
     * @param ttl time-to-live for the lock
     * @return true if the lock was acquired, false otherwise
     */
    boolean tryAcquire(String lockKey, String lockValue, Duration ttl);
    
    /**
     * Releases a lock only if the current holder owns it.
     * Uses Lua script to ensure atomicity of check-and-delete.
     * 
     * @param lockKey the unique identifier for the lock
     * @param lockValue the expected value (must match to release)
     * @return true if the lock was released, false if not owned or already released
     */
    boolean release(String lockKey, String lockValue);
    
    /**
     * Extends the TTL of an existing lock if the current holder owns it.
     * 
     * @param lockKey the unique identifier for the lock
     * @param lockValue the expected value (must match to extend)
     * @param additionalTtl the additional time to add to the lock
     * @return true if the lock TTL was extended, false if not owned or expired
     */
    boolean extend(String lockKey, String lockValue, Duration additionalTtl);
    
    /**
     * Gets the current holder of a lock if it exists.
     * 
     * @param lockKey the unique identifier for the lock
     * @return Optional containing the lock value if the lock exists, empty otherwise
     */
    Optional<String> getLockHolder(String lockKey);
    
    /**
     * Checks if a lock exists and is held.
     * 
     * @param lockKey the unique identifier for the lock
     * @return true if the lock is currently held, false otherwise
     */
    boolean isLocked(String lockKey);
}
