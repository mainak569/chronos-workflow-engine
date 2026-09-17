package com.chronos.worker.lock;

import java.time.Duration;
import java.time.Instant;

/**
 * Represents a task execution lease held by a worker.
 * Combines lock ownership with execution tracking for recovery purposes.
 * 
 * The lease tracks:
 * - Who owns the task (workerId + token)
 * - When the lease expires (for recovery)
 * - Execution metadata (executionId, attempt number)
 * 
 * Format in Redis:
 * Key: chronos:lease:task:{taskId}
 * Value: JSON of TaskLease
 * TTL: Lease duration (5 minutes default)
 */
public class TaskLease {
    
    /**
     * ID of the task being executed.
     */
    private String taskId;
    
    /**
     * ID of the workflow execution.
     */
    private String executionId;
    
    /**
     * ID of the worker holding the lease.
     */
    private String workerId;
    
    /**
     * Unique lock token (workerId:uuid).
     */
    private String lockToken;
    
    /**
     * Current attempt number.
     */
    private int attemptNumber;
    
    /**
     * When the lease was acquired.
     */
    private Instant acquiredAt;
    
    /**
     * When the lease expires (for recovery).
     */
    private Instant expiresAt;
    
    /**
     * Lease duration.
     */
    private Duration leaseDuration;
    
    // Constructors
    
    public TaskLease() {
    }
    
    public TaskLease(String taskId, String executionId, String workerId, String lockToken,
                     int attemptNumber, Duration leaseDuration) {
        this.taskId = taskId;
        this.executionId = executionId;
        this.workerId = workerId;
        this.lockToken = lockToken;
        this.attemptNumber = attemptNumber;
        this.leaseDuration = leaseDuration;
        this.acquiredAt = Instant.now();
        this.expiresAt = this.acquiredAt.plus(leaseDuration);
    }
    
    /**
     * Check if the lease has expired.
     */
    public boolean isExpired() {
        return Instant.now().isAfter(expiresAt);
    }
    
    /**
     * Check if the lease belongs to the given worker.
     */
    public boolean belongsTo(String workerId) {
        return this.workerId != null && this.workerId.equals(workerId);
    }
    
    /**
     * Check if the lock token matches.
     */
    public boolean hasToken(String token) {
        return this.lockToken != null && this.lockToken.equals(token);
    }
    
    /**
     * Get time until expiration.
     */
    public Duration getTimeUntilExpiration() {
        return Duration.between(Instant.now(), expiresAt);
    }
    
    /**
     * Get time since acquisition.
     */
    public Duration getTimeSinceAcquisition() {
        return Duration.between(acquiredAt, Instant.now());
    }
    
    // Getters and Setters
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getExecutionId() {
        return executionId;
    }
    
    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }
    
    public String getWorkerId() {
        return workerId;
    }
    
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
    
    public String getLockToken() {
        return lockToken;
    }
    
    public void setLockToken(String lockToken) {
        this.lockToken = lockToken;
    }
    
    public int getAttemptNumber() {
        return attemptNumber;
    }
    
    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }
    
    public Instant getAcquiredAt() {
        return acquiredAt;
    }
    
    public void setAcquiredAt(Instant acquiredAt) {
        this.acquiredAt = acquiredAt;
    }
    
    public Instant getExpiresAt() {
        return expiresAt;
    }
    
    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
    
    public Duration getLeaseDuration() {
        return leaseDuration;
    }
    
    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }
    
    @Override
    public String toString() {
        return "TaskLease{" +
                "taskId='" + taskId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", workerId='" + workerId + '\'' +
                ", attemptNumber=" + attemptNumber +
                ", acquiredAt=" + acquiredAt +
                ", expiresAt=" + expiresAt +
                ", expired=" + isExpired() +
                '}';
    }
}
