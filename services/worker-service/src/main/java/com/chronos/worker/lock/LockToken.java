package com.chronos.worker.lock;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a unique lock ownership token.
 * Combines workerId with a unique UUID to ensure uniqueness even if the same worker
 * attempts to acquire the same lock multiple times.
 * 
 * Format: {workerId}:{uuid}
 */
public class LockToken {
    
    private final String workerId;
    private final String uuid;
    private final String token;
    
    private LockToken(String workerId, String uuid) {
        this.workerId = workerId;
        this.uuid = uuid;
        this.token = workerId + ":" + uuid;
    }
    
    /**
     * Create a new lock token for the given worker.
     */
    public static LockToken create(String workerId) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        return new LockToken(workerId, UUID.randomUUID().toString());
    }
    
    /**
     * Parse a token string back into a LockToken.
     * Returns null if the format is invalid.
     */
    public static LockToken parse(String tokenString) {
        if (tokenString == null || tokenString.isEmpty()) {
            return null;
        }
        
        int separatorIndex = tokenString.lastIndexOf(':');
        if (separatorIndex == -1) {
            return null;
        }
        
        String workerId = tokenString.substring(0, separatorIndex);
        String uuid = tokenString.substring(separatorIndex + 1);
        
        return new LockToken(workerId, uuid);
    }
    
    /**
     * Get the worker ID that owns this lock.
     */
    public String getWorkerId() {
        return workerId;
    }
    
    /**
     * Get the unique identifier portion of the token.
     */
    public String getUuid() {
        return uuid;
    }
    
    /**
     * Get the full token string.
     */
    public String getToken() {
        return token;
    }
    
    /**
     * Check if this token belongs to the given worker.
     */
    public boolean belongsTo(String workerId) {
        return this.workerId.equals(workerId);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockToken lockToken = (LockToken) o;
        return Objects.equals(token, lockToken.token);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(token);
    }
    
    @Override
    public String toString() {
        return token;
    }
}
