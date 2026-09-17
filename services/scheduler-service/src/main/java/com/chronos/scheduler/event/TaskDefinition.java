package com.chronos.scheduler.event;

import java.util.List;
import java.util.Map;

/**
 * Task definition DTO for events.
 * Lightweight representation of a task for event payloads.
 */
public class TaskDefinition {
    
    private String taskId;
    private String taskName;
    private String taskType;
    private List<String> dependsOn;
    private Map<String, Object> configuration;
    private Integer maxRetries;
    private Long timeoutMs;
    
    // Constructors
    public TaskDefinition() {
    }
    
    public TaskDefinition(String taskId, String taskName, String taskType) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.taskType = taskType;
    }
    
    // Getters and Setters
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
    
    public List<String> getDependsOn() {
        return dependsOn;
    }
    
    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn;
    }
    
    public Map<String, Object> getConfiguration() {
        return configuration;
    }
    
    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }
    
    public Integer getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public Long getTimeoutMs() {
        return timeoutMs;
    }
    
    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}
