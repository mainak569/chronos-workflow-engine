package com.chronos.workflow.dto;

import com.chronos.workflow.domain.TaskDefinition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;

/**
 * Request DTO for creating a new workflow.
 */
public class CreateWorkflowRequest {

    @NotBlank(message = "Workflow name is required")
    private String name;

    private String description;

    @NotEmpty(message = "Workflow must contain at least one task")
    @Valid
    private List<TaskDefinition> tasks = new ArrayList<>();
    
    // Constructors
    public CreateWorkflowRequest() {
    }
    
    public CreateWorkflowRequest(String name, String description, List<TaskDefinition> tasks) {
        this.name = name;
        this.description = description;
        this.tasks = tasks != null ? tasks : new ArrayList<>();
    }
    
    // Getters and Setters
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
    
    // Builder
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String name;
        private String description;
        private List<TaskDefinition> tasks = new ArrayList<>();
        
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
        
        public CreateWorkflowRequest build() {
            return new CreateWorkflowRequest(name, description, tasks);
        }
    }
}
