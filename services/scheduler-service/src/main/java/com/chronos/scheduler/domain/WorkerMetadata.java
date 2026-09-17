package com.chronos.scheduler.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.time.Duration;
import java.util.List;

/**
 * Represents metadata about a worker node in the distributed system.
 * Duplicated in scheduler-service for monitoring purposes.
 */
public class WorkerMetadata {
    
    /**
     * Unique identifier for the worker.
     */
    private String workerId;
    
    /**
     * Current status of the worker.
     */
    private WorkerStatus status;
    
    /**
     * List of task types this worker can execute.
     */
    private List<String> supportedTaskTypes;
    
    /**
     * Timestamp of the last heartbeat received from this worker.
     */
    private Instant lastHeartbeat;
    
    /**
     * Worker lifecycle states.
     */
    public enum WorkerStatus {
        REGISTERING,   // Initial state during registration
        AVAILABLE,     // Ready to accept tasks
        BUSY,          // Currently executing a task
        UNAVAILABLE,   // Not responding (heartbeat expired)
        STOPPED        // Gracefully shut down
    }
    
    // Constructors
    
    public WorkerMetadata() {
    }
    
    public WorkerMetadata(String workerId, WorkerStatus status, List<String> supportedTaskTypes, Instant lastHeartbeat) {
        this.workerId = workerId;
        this.status = status;
        this.supportedTaskTypes = supportedTaskTypes;
        this.lastHeartbeat = lastHeartbeat;
    }
    
    // Getters and Setters
    
    public String getWorkerId() {
        return workerId;
    }
    
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
    
    public WorkerStatus getStatus() {
        return status;
    }
    
    public void setStatus(WorkerStatus status) {
        this.status = status;
    }
    
    public List<String> getSupportedTaskTypes() {
        return supportedTaskTypes;
    }
    
    public void setSupportedTaskTypes(List<String> supportedTaskTypes) {
        this.supportedTaskTypes = supportedTaskTypes;
    }
    
    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }
    
    public void setLastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }
    
    // Business Methods
    
    /**
     * Mark worker as unavailable.
     */
    public void markUnavailable() {
        this.status = WorkerStatus.UNAVAILABLE;
    }
    
    /**
     * Check if worker heartbeat has expired.
     * Heartbeat expires after 30 seconds without update.
     */
    @JsonIgnore
    public boolean isHeartbeatExpired() {
        if (lastHeartbeat == null) {
            return true;
        }
        Duration elapsed = Duration.between(lastHeartbeat, Instant.now());
        return elapsed.getSeconds() > 30;
    }
}
