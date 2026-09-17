package com.chronos.workflow.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Workflow domain model.
 * Represents a directed acyclic graph (DAG) of tasks.
 */
@Document(collection = "workflows")
public class Workflow {

    /**
     * Unique identifier for this workflow.
     */
    @Id
    private String id;

    /**
     * User ID of the workflow owner.
     * Indexed for efficient lookup of user's workflows.
     */
    @NotBlank(message = "Owner ID is required")
    @Indexed
    private String ownerId;

    /**
     * Human-readable name for the workflow.
     */
    @NotBlank(message = "Workflow name is required")
    private String name;

    /**
     * Optional description of what this workflow does.
     */
    private String description;

    /**
     * List of task definitions that compose this workflow.
     * Must contain at least one task.
     */
    @NotEmpty(message = "Workflow must contain at least one task")
    @Valid
    private List<TaskDefinition> tasks = new ArrayList<>();

    /**
     * Timestamp when this workflow was created.
     */
    @CreatedDate
    @Indexed
    private Instant createdAt;

    /**
     * Timestamp when this workflow was last modified.
     */
    @LastModifiedDate
    private Instant updatedAt;

    /**
     * Version number for optimistic locking.
     */
    private Long version;
    
    // Constructors
    public Workflow() {
    }
    
    public Workflow(String id, String ownerId, String name, String description, 
                   List<TaskDefinition> tasks, Instant createdAt, Instant updatedAt, Long version) {
        this.id = id;
        this.ownerId = ownerId;
        this.name = name;
        this.description = description;
        this.tasks = tasks != null ? tasks : new ArrayList<>();
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.version = version;
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
        private String ownerId;
        private String name;
        private String description;
        private List<TaskDefinition> tasks = new ArrayList<>();
        private Instant createdAt;
        private Instant updatedAt;
        private Long version;
        
        public Builder id(String id) {
            this.id = id;
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
        
        public Builder version(Long version) {
            this.version = version;
            return this;
        }
        
        public Workflow build() {
            return new Workflow(id, ownerId, name, description, tasks, createdAt, updatedAt, version);
        }
    }

    /**
     * Find a task definition by ID.
     *
     * @param taskId Task identifier
     * @return Task definition or null if not found
     */
    public TaskDefinition getTaskById(String taskId) {
        return tasks.stream()
                .filter(task -> task.getTaskId().equals(taskId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Check if a task with the given ID exists in this workflow.
     *
     * @param taskId Task identifier
     * @return true if task exists, false otherwise
     */
    public boolean hasTask(String taskId) {
        return tasks.stream()
                .anyMatch(task -> task.getTaskId().equals(taskId));
    }

    /**
     * Get all task IDs in this workflow.
     *
     * @return List of task IDs
     */
    public List<String> getTaskIds() {
        return tasks.stream()
                .map(TaskDefinition::getTaskId)
                .toList();
    }
}
