package com.chronos.workflow.domain;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single execution instance of a workflow.
 * Tracks overall workflow execution state and metadata.
 */
@Document(collection = "workflow_executions")
@CompoundIndex(name = "workflow_status_idx", def = "{'workflowId': 1, 'status': 1}")
public class WorkflowExecution {
    
    /**
     * Unique identifier for this execution.
     */
    @Id
    private String id;
    
    /**
     * ID of the workflow definition being executed.
     */
    @Indexed
    private String workflowId;
    
    /**
     * Name of the workflow (denormalized for convenience).
     */
    private String workflowName;
    
    /**
     * ID of the user who owns the workflow (used for access control).
     */
    @Indexed
    private String ownerId;
    
    /**
     * User ID who triggered this execution.
     */
    @Indexed
    private String triggeredBy;
    
    /**
     * Current status of the workflow execution.
     */
    @Indexed
    private ExecutionStatus status;
    
    /**
     * Input parameters provided when starting the execution.
     */
    private Map<String, Object> input;
    
    /**
     * Output result when workflow completes successfully.
     */
    private Map<String, Object> output;
    
    /**
     * Error message if workflow fails.
     */
    private String errorMessage;
    
    /**
     * Error type/category if workflow fails.
     */
    private String errorType;
    
    /**
     * Timestamp when execution was created.
     */
    @CreatedDate
    @Indexed
    private Instant createdAt;
    
    /**
     * Timestamp when execution was last updated.
     */
    @LastModifiedDate
    private Instant updatedAt;
    
    /**
     * Timestamp when execution started (transitioned to RUNNING).
     */
    private Instant startedAt;
    
    /**
     * Timestamp when execution completed (reached terminal state).
     */
    private Instant completedAt;
    
    /**
     * Duration in milliseconds (only set after completion).
     */
    private Long durationMs;
    
    /**
     * Version for optimistic locking.
     */
    @Version
    private Long version;
    
    // Constructors
    public WorkflowExecution() {
    }
    
    public WorkflowExecution(String id, String workflowId, String workflowName, String triggeredBy,
                           ExecutionStatus status, Map<String, Object> input, Map<String, Object> output,
                           String errorMessage, String errorType, Instant createdAt, Instant updatedAt,
                           Instant startedAt, Instant completedAt, Long durationMs, Long version) {
        this.id = id;
        this.workflowId = workflowId;
        this.workflowName = workflowName;
        this.triggeredBy = triggeredBy;
        this.status = status;
        this.input = input;
        this.output = output;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.durationMs = durationMs;
        this.version = version;
    }
    
    // State transition methods with validation
    
    /**
     * Transition to RUNNING state.
     * Sets startedAt timestamp.
     */
    public void start() {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.RUNNING);
        }
        this.status = ExecutionStatus.RUNNING;
        this.startedAt = Instant.now();
    }
    
    /**
     * Transition to COMPLETED state.
     * Sets completedAt and calculates duration.
     */
    public void complete(Map<String, Object> output) {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.COMPLETED);
        }
        this.status = ExecutionStatus.COMPLETED;
        this.output = output;
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    /**
     * Transition to FAILED state.
     * Sets error information, completedAt and calculates duration.
     */
    public void fail(String errorMessage, String errorType) {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.FAILED);
        }
        this.status = ExecutionStatus.FAILED;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    /**
     * Transition to CANCELLED state.
     * Sets completedAt and calculates duration.
     */
    public void cancel() {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.CANCELLED);
        }
        this.status = ExecutionStatus.CANCELLED;
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getTriggeredBy() {
        return triggeredBy;
    }

    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }
    
    // Builder
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private String workflowId;
        private String workflowName;
        private String ownerId;
        private String triggeredBy;
        private ExecutionStatus status = ExecutionStatus.PENDING;
        private Map<String, Object> input = new HashMap<>();
        private Map<String, Object> output;
        private String errorMessage;
        private String errorType;
        private Instant createdAt;
        private Instant updatedAt;
        private Instant startedAt;
        private Instant completedAt;
        private Long durationMs;
        private Long version;
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }
        
        public Builder workflowName(String workflowName) {
            this.workflowName = workflowName;
            return this;
        }
        
        public Builder ownerId(String ownerId) {
            this.ownerId = ownerId;
            return this;
        }
        
        public Builder triggeredBy(String triggeredBy) {
            this.triggeredBy = triggeredBy;
            return this;
        }
        
        public Builder status(ExecutionStatus status) {
            this.status = status;
            return this;
        }
        
        public Builder input(Map<String, Object> input) {
            this.input = input;
            return this;
        }
        
        public Builder output(Map<String, Object> output) {
            this.output = output;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder errorType(String errorType) {
            this.errorType = errorType;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }
        
        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }
        
        public Builder completedAt(Instant completedAt) {
            this.completedAt = completedAt;
            return this;
        }
        
        public Builder durationMs(Long durationMs) {
            this.durationMs = durationMs;
            return this;
        }
        
        public Builder version(Long version) {
            this.version = version;
            return this;
        }
        
        public WorkflowExecution build() {
            WorkflowExecution execution = new WorkflowExecution(id, workflowId, workflowName, triggeredBy, status, 
                    input, output, errorMessage, errorType, createdAt, updatedAt, 
                    startedAt, completedAt, durationMs, version);
            execution.setOwnerId(ownerId);
            return execution;
        }
    }
}
