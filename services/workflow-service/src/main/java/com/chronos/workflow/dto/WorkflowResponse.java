package com.chronos.workflow.dto;

import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.Workflow;

import java.time.Instant;
import java.util.List;

/**
 * Response DTO for workflow data.
 */
public class WorkflowResponse {

    private String workflowId;
    private String ownerId;
    private String name;
    private String description;
    private List<TaskDefinition> tasks;
    private Instant createdAt;
    private Instant updatedAt;
    
    // Constructors
    public WorkflowResponse() {
    }
    
    public WorkflowResponse(String workflowId, String ownerId, String name, String description,
                           List<TaskDefinition> tasks, Instant createdAt, Instant updatedAt) {
        this.workflowId = workflowId;
        this.ownerId = ownerId;
        this.name = name;
        this.description = description;
        this.tasks = tasks;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
    
    // Getters and Setters
    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<TaskDefinition> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDefinition> tasks) {
        this.tasks = tasks;
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
    
    // Builder
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String workflowId;
        private String ownerId;
        private String name;
        private String description;
        private List<TaskDefinition> tasks;
        private Instant createdAt;
        private Instant updatedAt;
        
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }
        
        public Builder ownerId(String ownerId) {
            this.ownerId = ownerId;
            return this;
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Builder tasks(List<TaskDefinition> tasks) {
            this.tasks = tasks;
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
        
        public WorkflowResponse build() {
            return new WorkflowResponse(workflowId, ownerId, name, description, tasks, createdAt, updatedAt);
        }
    }

    /**
     * Convert domain model to response DTO.
     *
     * @param workflow Domain model
     * @return Response DTO
     */
    public static WorkflowResponse from(Workflow workflow) {
        return WorkflowResponse.builder()
                .workflowId(workflow.getId())
                .ownerId(workflow.getOwnerId())
                .name(workflow.getName())
                .description(workflow.getDescription())
                .tasks(workflow.getTasks())
                .createdAt(workflow.getCreatedAt())
                .updatedAt(workflow.getUpdatedAt())
                .build();
    }
}
