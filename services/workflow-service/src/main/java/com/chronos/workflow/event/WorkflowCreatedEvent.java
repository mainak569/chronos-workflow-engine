package com.chronos.workflow.event;

import com.chronos.workflow.domain.TaskDefinition;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event published when a workflow is created.
 * Triggers the scheduler to begin execution planning.
 */
public class WorkflowCreatedEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workflowId;
    private String executionId;  // ID of the workflow execution
    private String ownerId;
    private String workflowName;
    private List<TaskDefinition> tasks;

    // Constructors
    public WorkflowCreatedEvent() {
    }

    public WorkflowCreatedEvent(String eventId, String correlationId, Instant timestamp,
                                String workflowId, String executionId, String ownerId, String workflowName,
                                List<TaskDefinition> tasks) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workflowId = workflowId;
        this.executionId = executionId;
        this.ownerId = ownerId;
        this.workflowName = workflowName;
        this.tasks = tasks;
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

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public List<TaskDefinition> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDefinition> tasks) {
        this.tasks = tasks;
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
        private String ownerId;
        private String workflowName;
        private List<TaskDefinition> tasks;

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

        public Builder ownerId(String ownerId) {
            this.ownerId = ownerId;
            return this;
        }

        public Builder workflowName(String workflowName) {
            this.workflowName = workflowName;
            return this;
        }

        public Builder tasks(List<TaskDefinition> tasks) {
            this.tasks = tasks;
            return this;
        }

        public WorkflowCreatedEvent build() {
            return new WorkflowCreatedEvent(eventId, correlationId, timestamp,
                    workflowId, executionId, ownerId, workflowName, tasks);
        }
    }

    @Override
    public String toString() {
        return "WorkflowCreatedEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workflowId='" + workflowId + '\'' +
                ", ownerId='" + ownerId + '\'' +
                ", workflowName='" + workflowName + '\'' +
                ", tasksCount=" + (tasks != null ? tasks.size() : 0) +
                '}';
    }
}
