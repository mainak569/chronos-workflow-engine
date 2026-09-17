package com.chronos.workflow.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Task definition within a workflow.
 * Represents the blueprint for a task, not an execution instance.
 */
public class TaskDefinition {

    /**
     * Unique identifier for this task within the workflow.
     * Must be unique within the workflow.
     */
    @NotBlank(message = "Task ID is required")
    private String taskId;

    /**
     * Human-readable name for the task.
     */
    @NotBlank(message = "Task name is required")
    private String name;

    /**
     * Type of task (e.g., IMAGE_RESIZE, DATA_PROCESSING, etc.).
     * Used by workers to determine if they can execute this task.
     */
    @NotBlank(message = "Task type is required")
    private String taskType;

    /**
     * List of task IDs that must complete before this task can run.
     * Empty set means no dependencies (can run immediately).
     */
    private Set<String> dependencies = new HashSet<>();

    /**
     * Configuration specific to this task type.
     * Contents depend on the task type.
     */
    private Map<String, Object> configuration = new HashMap<>();

    /**
     * Retry configuration for this task.
     */
    @NotNull(message = "Retry configuration is required")
    private RetryConfiguration retryConfig = RetryConfiguration.builder().build();

    /**
     * Maximum time (in milliseconds) this task is allowed to run.
     * Worker will timeout the task after this duration.
     */
    private Long timeoutMs = 300000L; // 5 minutes default

    /**
     * Optional description of what this task does.
     */
    private String description;
    
    // Constructors
    public TaskDefinition() {
    }
    
    public TaskDefinition(String taskId, String name, String taskType, Set<String> dependencies, 
                         Map<String, Object> configuration, RetryConfiguration retryConfig, 
                         Long timeoutMs, String description) {
        this.taskId = taskId;
        this.name = name;
        this.taskType = taskType;
        this.dependencies = dependencies != null ? dependencies : new HashSet<>();
        this.configuration = configuration != null ? configuration : new HashMap<>();
        this.retryConfig = retryConfig != null ? retryConfig : RetryConfiguration.builder().build();
        this.timeoutMs = timeoutMs != null ? timeoutMs : 300000L;
        this.description = description;
    }
    
    // Getters and Setters
    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<String> dependencies) {
        this.dependencies = dependencies;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public RetryConfiguration getRetryConfig() {
        return retryConfig;
    }

    public void setRetryConfig(RetryConfiguration retryConfig) {
        this.retryConfig = retryConfig;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
    
    // Builder
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String taskId;
        private String name;
        private String taskType;
        private Set<String> dependencies = new HashSet<>();
        private Map<String, Object> configuration = new HashMap<>();
        private RetryConfiguration retryConfig = RetryConfiguration.builder().build();
        private Long timeoutMs = 300000L;
        private String description;
        
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }
        
        public Builder dependencies(Set<String> dependencies) {
            this.dependencies = dependencies;
            return this;
        }
        
        public Builder configuration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }
        
        public Builder retryConfig(RetryConfiguration retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }
        
        public Builder timeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public TaskDefinition build() {
            return new TaskDefinition(taskId, name, taskType, dependencies, configuration, 
                                    retryConfig, timeoutMs, description);
        }
    }
}

