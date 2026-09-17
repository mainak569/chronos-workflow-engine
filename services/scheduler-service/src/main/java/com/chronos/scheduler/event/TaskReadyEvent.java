package com.chronos.scheduler.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Event published when a task is ready to be executed.
 * Consumed by workers to pick up and execute tasks.
 */
public class TaskReadyEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workflowId;
    private String executionId;
    private String taskId;
    private String taskType;
    private Map<String, Object> configuration;
    private Long timeoutMs;
    private Integer maxRetries;

    // Constructors
    public TaskReadyEvent() {
    }

    public TaskReadyEvent(String eventId, String correlationId, Instant timestamp,
                         String workflowId, String executionId, String taskId,
                         String taskType, Map<String, Object> configuration,
                         Long timeoutMs, Integer maxRetries) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workflowId = workflowId;
        this.executionId = executionId;
        this.taskId = taskId;
        this.taskType = taskType;
        this.configuration = configuration;
        this.timeoutMs = timeoutMs;
        this.maxRetries = maxRetries;
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

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
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
        private String taskType;
        private Map<String, Object> configuration;
        private Long timeoutMs;
        private Integer maxRetries;

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

        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder configuration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder timeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder maxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public TaskReadyEvent build() {
            return new TaskReadyEvent(eventId, correlationId, timestamp,
                    workflowId, executionId, taskId, taskType, configuration,
                    timeoutMs, maxRetries);
        }
    }

    @Override
    public String toString() {
        return "TaskReadyEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workflowId='" + workflowId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", taskType='" + taskType + '\'' +
                ", timeoutMs=" + timeoutMs +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
