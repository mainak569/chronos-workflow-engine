package com.chronos.worker.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Event published when a worker starts executing a task.
 */
public class TaskStartedEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workflowId;
    private String executionId;
    private String taskId;
    private String workerId;
    private Integer attemptNumber;

    // Constructors
    public TaskStartedEvent() {
    }

    public TaskStartedEvent(String eventId, String correlationId, Instant timestamp,
                           String workflowId, String executionId, String taskId,
                           String workerId, Integer attemptNumber) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workflowId = workflowId;
        this.executionId = executionId;
        this.taskId = taskId;
        this.workerId = workerId;
        this.attemptNumber = attemptNumber;
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

        public TaskStartedEvent build() {
            return new TaskStartedEvent(eventId, correlationId, timestamp,
                    workflowId, executionId, taskId, workerId, attemptNumber);
        }
    }

    @Override
    public String toString() {
        return "TaskStartedEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workflowId='" + workflowId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", workerId='" + workerId + '\'' +
                ", attemptNumber=" + attemptNumber +
                '}';
    }
}
