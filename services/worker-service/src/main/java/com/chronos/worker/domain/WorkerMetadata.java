package com.chronos.worker.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata about a worker instance.
 * Stored in Redis for distributed coordination.
 */
public class WorkerMetadata {
    
    /**
     * Unique identifier for this worker instance.
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
     * Timestamp of last heartbeat received from this worker.
     */
    private Instant lastHeartbeat;
    
    /**
     * Timestamp when worker was registered.
     */
    private Instant registeredAt;
    
    /**
     * Hostname or IP address of the worker.
     */
    private String host;
    
    /**
     * Version of the worker application.
     */
    private String version;
    
    /**
     * Current task being executed (if BUSY).
     */
    private String currentTaskId;
    
    /**
     * Current execution being processed (if BUSY).
     */
    private String currentExecutionId;
    
    // Constructors
    
    public WorkerMetadata() {
    }
    
    public WorkerMetadata(String workerId, WorkerStatus status, List<String> supportedTaskTypes,
                         Instant lastHeartbeat, Instant registeredAt, String host, String version,
                         String currentTaskId, String currentExecutionId) {
        this.workerId = workerId;
        this.status = status;
        this.supportedTaskTypes = supportedTaskTypes;
        this.lastHeartbeat = lastHeartbeat;
        this.registeredAt = registeredAt;
        this.host = host;
        this.version = version;
        this.currentTaskId = currentTaskId;
        this.currentExecutionId = currentExecutionId;
    }
    
    // State transition methods
    
    /**
     * Transition to AVAILABLE state.
     */
    public void markAvailable() {
        if (this.status != null) {
            this.status.validateTransition(WorkerStatus.AVAILABLE);
        }
        this.status = WorkerStatus.AVAILABLE;
        this.currentTaskId = null;
        this.currentExecutionId = null;
    }
    
    /**
     * Transition to BUSY state.
     */
    public void markBusy(String executionId, String taskId) {
        if (this.status != null) {
            this.status.validateTransition(WorkerStatus.BUSY);
        }
        this.status = WorkerStatus.BUSY;
        this.currentExecutionId = executionId;
        this.currentTaskId = taskId;
    }
    
    /**
     * Transition to UNAVAILABLE state.
     */
    public void markUnavailable() {
        if (this.status != null) {
            this.status.validateTransition(WorkerStatus.UNAVAILABLE);
        }
        this.status = WorkerStatus.UNAVAILABLE;
    }
    
    /**
     * Transition to STOPPED state.
     */
    public void markStopped() {
        if (this.status != null) {
            this.status.validateTransition(WorkerStatus.STOPPED);
        }
        this.status = WorkerStatus.STOPPED;
        this.currentTaskId = null;
        this.currentExecutionId = null;
    }
    
    /**
     * Update heartbeat timestamp.
     */
    public void updateHeartbeat() {
        this.lastHeartbeat = Instant.now();
    }
    
    /**
     * Check if heartbeat has expired based on TTL.
     */
    public boolean isHeartbeatExpired(long heartbeatTtlSeconds) {
        if (lastHeartbeat == null) {
            return true;
        }
        Instant expiryTime = lastHeartbeat.plusSeconds(heartbeatTtlSeconds);
        return Instant.now().isAfter(expiryTime);
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
    
    public Instant getRegisteredAt() {
        return registeredAt;
    }
    
    public void setRegisteredAt(Instant registeredAt) {
        this.registeredAt = registeredAt;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public String getCurrentTaskId() {
        return currentTaskId;
    }
    
    public void setCurrentTaskId(String currentTaskId) {
        this.currentTaskId = currentTaskId;
    }
    
    public String getCurrentExecutionId() {
        return currentExecutionId;
    }
    
    public void setCurrentExecutionId(String currentExecutionId) {
        this.currentExecutionId = currentExecutionId;
    }
    
    // Builder
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String workerId;
        private WorkerStatus status = WorkerStatus.REGISTERING;
        private List<String> supportedTaskTypes = new ArrayList<>();
        private Instant lastHeartbeat;
        private Instant registeredAt;
        private String host;
        private String version;
        private String currentTaskId;
        private String currentExecutionId;
        
        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }
        
        public Builder status(WorkerStatus status) {
            this.status = status;
            return this;
        }
        
        public Builder supportedTaskTypes(List<String> supportedTaskTypes) {
            this.supportedTaskTypes = supportedTaskTypes;
            return this;
        }
        
        public Builder lastHeartbeat(Instant lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
            return this;
        }
        
        public Builder registeredAt(Instant registeredAt) {
            this.registeredAt = registeredAt;
            return this;
        }
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder version(String version) {
            this.version = version;
            return this;
        }
        
        public Builder currentTaskId(String currentTaskId) {
            this.currentTaskId = currentTaskId;
            return this;
        }
        
        public Builder currentExecutionId(String currentExecutionId) {
            this.currentExecutionId = currentExecutionId;
            return this;
        }
        
        public WorkerMetadata build() {
            return new WorkerMetadata(workerId, status, supportedTaskTypes, lastHeartbeat,
                    registeredAt, host, version, currentTaskId, currentExecutionId);
        }
    }
    
    @Override
    public String toString() {
        return "WorkerMetadata{" +
                "workerId='" + workerId + '\'' +
                ", status=" + status +
                ", supportedTaskTypes=" + supportedTaskTypes +
                ", lastHeartbeat=" + lastHeartbeat +
                ", host='" + host + '\'' +
                ", currentTaskId='" + currentTaskId + '\'' +
                '}';
    }
}
