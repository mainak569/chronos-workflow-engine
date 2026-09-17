package com.chronos.scheduler.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Redis-based implementation of distributed locking.
 * Uses SET NX EX for atomic lock acquisition and Lua scripts for atomic release/extend operations.
 */
@Component
public class RedisDistributedLock implements DistributedLock {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisDistributedLock.class);
    
    private final RedisTemplate<String, String> redisTemplate;
    
    // Lua script for atomic check-and-delete (release)
    private static final String RELEASE_SCRIPT = 
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";
    
    // Lua script for atomic check-and-expire (extend)
    private static final String EXTEND_SCRIPT = 
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('expire', KEYS[1], ARGV[2]) " +
            "else " +
            "    return 0 " +
            "end";
    
    public RedisDistributedLock(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    @Override
    public boolean tryAcquire(String lockKey, String lockValue, Duration ttl) {
        try {
            // SET NX EX is atomic - only sets if key doesn't exist
            Boolean acquired = redisTemplate.opsForValue()
                    .setIfAbsent(lockKey, lockValue, ttl.toMillis(), TimeUnit.MILLISECONDS);
            
            boolean result = Boolean.TRUE.equals(acquired);
            
            if (result) {
                logger.debug("Lock acquired: key={}, value={}, ttl={}ms", 
                        lockKey, lockValue, ttl.toMillis());
            } else {
                logger.debug("Lock acquisition failed: key={} (already held)", lockKey);
            }
            
            return result;
        } catch (Exception e) {
            logger.error("Error acquiring lock: key={}", lockKey, e);
            return false;
        }
    }
    
    @Override
    public boolean release(String lockKey, String lockValue) {
        try {
            // Use Lua script to ensure we only delete if we own the lock
            Long result = redisTemplate.execute(
                    RedisScript.of(RELEASE_SCRIPT, Long.class),
                    Collections.singletonList(lockKey),
                    lockValue
            );
            
            boolean released = result != null && result > 0;
            
            if (released) {
                logger.debug("Lock released: key={}, value={}", lockKey, lockValue);
            } else {
                logger.debug("Lock release failed: key={} (not owned or already released)", lockKey);
            }
            
            return released;
        } catch (Exception e) {
            logger.error("Error releasing lock: key={}", lockKey, e);
            return false;
        }
    }
    
    @Override
    public boolean extend(String lockKey, String lockValue, Duration additionalTtl) {
        try {
            // Use Lua script to ensure we only extend if we own the lock
            Long result = redisTemplate.execute(
                    RedisScript.of(EXTEND_SCRIPT, Long.class),
                    Collections.singletonList(lockKey),
                    lockValue,
                    String.valueOf(additionalTtl.toSeconds())
            );
            
            boolean extended = result != null && result > 0;
            
            if (extended) {
                logger.debug("Lock extended: key={}, value={}, additionalTtl={}s", 
                        lockKey, lockValue, additionalTtl.toSeconds());
            } else {
                logger.debug("Lock extension failed: key={} (not owned or expired)", lockKey);
            }
            
            return extended;
        } catch (Exception e) {
            logger.error("Error extending lock: key={}", lockKey, e);
            return false;
        }
    }
    
    @Override
    public Optional<String> getLockHolder(String lockKey) {
        try {
            String value = redisTemplate.opsForValue().get(lockKey);
            return Optional.ofNullable(value);
        } catch (Exception e) {
            logger.error("Error getting lock holder: key={}", lockKey, e);
            return Optional.empty();
        }
    }
    
    @Override
    public boolean isLocked(String lockKey) {
        try {
            return Boolean.TRUE.equals(redisTemplate.hasKey(lockKey));
        } catch (Exception e) {
            logger.error("Error checking lock status: key={}", lockKey, e);
            return false;
        }
    }
}
