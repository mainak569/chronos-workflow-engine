package com.chronos.worker.event;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Event published when a task has exhausted all retry attempts and failed permanently.
 * This event is sent to the Dead Letter Queue (DLQ) for manual investigation.
 */
public class TaskFailedPermanentlyEvent {
    
    /**
     * Unique identifier for this event.
     */
    private String eventId;
    
    /**
     * Correlation ID for tracing (from original request).
     */
    private String correlationId;
    
    /**
     * Workflow identifier.
     */
    private String workflowId;
    
    /**
     * Workflow execution identifier.
     */
    private String executionId;
    
    /**
     * Task identifier within the workflow.
     */
    private String taskId;
    
    /**
     * Task name.
     */
    private String taskName;
    
    /**
     * Task type.
     */
    private String taskType;
    
    /**
     * Total number of attempts made.
     */
    private int totalAttempts;
    
    /**
     * Maximum attempts allowed.
     */
    private int maxAttempts;
    
    /**
     * List of all error messages from all attempts.
     */
    private List<String> attemptErrors;
    
    /**
     * Error message from the last attempt.
     */
    private String lastErrorMessage;
    
    /**
     * Error type/category from the last attempt.
     */
    private String lastErrorType;
    
    /**
     * Worker ID that made the last attempt.
     */
    private String lastWorkerId;
    
    /**
     * Task configuration.
     */
    private Map<String, Object> configuration;
    
    /**
     * Task input data.
     */
    private Map<String, Object> input;
    
    /**
     * Timestamp when the task was originally created.
     */
    private Instant taskCreatedAt;
    
    /**
     * Timestamp when the first attempt started.
     */
    private Instant firstAttemptAt;
    
    /**
     * Timestamp when the last attempt failed.
     */
    private Instant lastAttemptAt;
    
    /**
     * Timestamp when this event was created.
     */
    private Instant timestamp;
    
    // Constructors
    
    public TaskFailedPermanentlyEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
    }
    
    // Builder pattern
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private TaskFailedPermanentlyEvent event = new TaskFailedPermanentlyEvent();
        
        public Builder correlationId(String correlationId) {
            event.correlationId = correlationId;
            return this;
        }
        
        public Builder workflowId(String workflowId) {
            event.workflowId = workflowId;
            return this;
        }
        
        public Builder executionId(String executionId) {
            event.executionId = executionId;
            return this;
        }
        
        public Builder taskId(String taskId) {
            event.taskId = taskId;
            return this;
        }
        
        public Builder taskName(String taskName) {
            event.taskName = taskName;
            return this;
        }
        
        public Builder taskType(String taskType) {
            event.taskType = taskType;
            return this;
        }
        
        public Builder totalAttempts(int totalAttempts) {
            event.totalAttempts = totalAttempts;
            return this;
        }
        
        public Builder maxAttempts(int maxAttempts) {
            event.maxAttempts = maxAttempts;
            return this;
        }
        
        public Builder attemptErrors(List<String> attemptErrors) {
            event.attemptErrors = attemptErrors;
            return this;
        }
        
        public Builder lastErrorMessage(String lastErrorMessage) {
            event.lastErrorMessage = lastErrorMessage;
            return this;
        }
        
        public Builder lastErrorType(String lastErrorType) {
            event.lastErrorType = lastErrorType;
            return this;
        }
        
        public Builder lastWorkerId(String lastWorkerId) {
            event.lastWorkerId = lastWorkerId;
            return this;
        }
        
        public Builder configuration(Map<String, Object> configuration) {
            event.configuration = configuration;
            return this;
        }
        
        public Builder input(Map<String, Object> input) {
            event.input = input;
            return this;
        }
        
        public Builder taskCreatedAt(Instant taskCreatedAt) {
            event.taskCreatedAt = taskCreatedAt;
            return this;
        }
        
        public Builder firstAttemptAt(Instant firstAttemptAt) {
            event.firstAttemptAt = firstAttemptAt;
            return this;
        }
        
        public Builder lastAttemptAt(Instant lastAttemptAt) {
            event.lastAttemptAt = lastAttemptAt;
            return this;
        }
        
        public TaskFailedPermanentlyEvent build() {
            return event;
        }
    }
    
    // Getters and Setters
    
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getWorkflowId() {
        return workflowId;
    }
    
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }
    
    public String getExecutionId() {
        return executionId;
    }
    
    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getTaskName() {
        return taskName;
    }
    
    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
    
    public String getTaskType() {
        return taskType;
    }
    
    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }
    
    public int getTotalAttempts() {
        return totalAttempts;
    }
    
    public void setTotalAttempts(int totalAttempts) {
        this.totalAttempts = totalAttempts;
    }
    
    public int getMaxAttempts() {
        return maxAttempts;
    }
    
    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }
    
    public List<String> getAttemptErrors() {
        return attemptErrors;
    }
    
    public void setAttemptErrors(List<String> attemptErrors) {
        this.attemptErrors = attemptErrors;
    }
    
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }
    
    public void setLastErrorMessage(String lastErrorMessage) {
        this.lastErrorMessage = lastErrorMessage;
    }
    
    public String getLastErrorType() {
        return lastErrorType;
    }
    
    public void setLastErrorType(String lastErrorType) {
        this.lastErrorType = lastErrorType;
    }
    
    public String getLastWorkerId() {
        return lastWorkerId;
    }
    
    public void setLastWorkerId(String lastWorkerId) {
        this.lastWorkerId = lastWorkerId;
    }
    
    public Map<String, Object> getConfiguration() {
        return configuration;
    }
    
    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }
    
    public Map<String, Object> getInput() {
        return input;
    }
    
    public void setInput(Map<String, Object> input) {
        this.input = input;
    }
    
    public Instant getTaskCreatedAt() {
        return taskCreatedAt;
    }
    
    public void setTaskCreatedAt(Instant taskCreatedAt) {
        this.taskCreatedAt = taskCreatedAt;
    }
    
    public Instant getFirstAttemptAt() {
        return firstAttemptAt;
    }
    
    public void setFirstAttemptAt(Instant firstAttemptAt) {
        this.firstAttemptAt = firstAttemptAt;
    }
    
    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }
    
    public void setLastAttemptAt(Instant lastAttemptAt) {
        this.lastAttemptAt = lastAttemptAt;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "TaskFailedPermanentlyEvent{" +
                "eventId='" + eventId + '\'' +
                ", workflowId='" + workflowId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", taskName='" + taskName + '\'' +
                ", totalAttempts=" + totalAttempts +
                ", maxAttempts=" + maxAttempts +
                ", lastErrorMessage='" + lastErrorMessage + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
