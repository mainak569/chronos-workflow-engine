package com.chronos.worker.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Event published when a task execution fails.
 */
public class TaskFailedEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workflowId;
    private String executionId;
    private String taskId;
    private String workerId;
    private Integer attemptNumber;
    private String errorMessage;
    private String errorType;
    private boolean retriable;
    private Long durationMs;

    // Constructors
    public TaskFailedEvent() {
    }

    public TaskFailedEvent(String eventId, String correlationId, Instant timestamp,
                          String workflowId, String executionId, String taskId,
                          String workerId, Integer attemptNumber,
                          String errorMessage, String errorType, boolean retriable,
                          Long durationMs) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workflowId = workflowId;
        this.executionId = executionId;
        this.taskId = taskId;
        this.workerId = workerId;
        this.attemptNumber = attemptNumber;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.retriable = retriable;
        this.durationMs = durationMs;
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

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
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

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Integer getAttemptNumber() {
        return attemptNumber;
    }

    public void setAttemptNumber(Integer attemptNumber) {
        this.attemptNumber = attemptNumber;
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

    public boolean isRetriable() {
        return retriable;
    }

    public void setRetriable(boolean retriable) {
        this.retriable = retriable;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String eventId = UUID.randomUUID().toString();
        private String correlationId;
        private Instant timestamp = Instant.now();
        private String workflowId;
        private String executionId;
        private String taskId;
        private String workerId;
        private Integer attemptNumber;
        private String errorMessage;
        private String errorType;
        private boolean retriable = true;
        private Long durationMs;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder executionId(String executionId) {
            this.executionId = executionId;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder attemptNumber(Integer attemptNumber) {
            this.attemptNumber = attemptNumber;
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

        public Builder retriable(boolean retriable) {
            this.retriable = retriable;
            return this;
        }

        public Builder durationMs(Long durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        public TaskFailedEvent build() {
            return new TaskFailedEvent(eventId, correlationId, timestamp,
                    workflowId, executionId, taskId, workerId, attemptNumber,
                    errorMessage, errorType, retriable, durationMs);
        }
    }

    @Override
    public String toString() {
        return "TaskFailedEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workflowId='" + workflowId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", workerId='" + workerId + '\'' +
                ", attemptNumber=" + attemptNumber +
                ", errorType='" + errorType + '\'' +
                ", retriable=" + retriable +
                ", durationMs=" + durationMs +
                '}';
    }
}
