package com.chronos.workflow.dto;

import com.chronos.workflow.domain.ExecutionStatus;
import com.chronos.workflow.domain.TaskExecution;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Response containing task execution details.
 */
public class TaskExecutionResponse {
    
    private String id;
    private String executionId;
    private String taskId;
    private String taskName;
    private String taskType;
    private ExecutionStatus status;
    private List<String> dependsOn;
    private Map<String, Object> configuration;
    private Map<String, Object> input;
    private Map<String, Object> output;
    private String errorMessage;
    private String errorType;
    private String workerId;
    private int attemptNumber;
    private int maxRetries;
    private boolean retriable;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant startedAt;
    private Instant completedAt;
    private Long durationMs;
    
    public TaskExecutionResponse() {
    }
    
    public TaskExecutionResponse(TaskExecution taskExecution) {
        this.id = taskExecution.getId();
        this.executionId = taskExecution.getExecutionId();
        this.taskId = taskExecution.getTaskId();
        this.taskName = taskExecution.getTaskName();
        this.taskType = taskExecution.getTaskType();
        this.status = taskExecution.getStatus();
        this.dependsOn = taskExecution.getDependsOn();
        this.configuration = taskExecution.getConfiguration();
        this.input = taskExecution.getInput();
        this.output = taskExecution.getOutput();
        this.errorMessage = taskExecution.getErrorMessage();
        this.errorType = taskExecution.getErrorType();
        this.workerId = taskExecution.getWorkerId();
        this.attemptNumber = taskExecution.getAttemptNumber();
        this.maxRetries = taskExecution.getMaxRetries();
        this.retriable = taskExecution.isRetriable();
        this.createdAt = taskExecution.getCreatedAt();
        this.updatedAt = taskExecution.getUpdatedAt();
        this.startedAt = taskExecution.getStartedAt();
        this.completedAt = taskExecution.getCompletedAt();
        this.durationMs = taskExecution.getDurationMs();
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
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
    
    public ExecutionStatus getStatus() {
        return status;
    }
    
    public void setStatus(ExecutionStatus status) {
        this.status = status;
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
    
    public Map<String, Object> getInput() {
        return input;
    }
    
    public void setInput(Map<String, Object> input) {
        this.input = input;
    }
    
    public Map<String, Object> getOutput() {
        return output;
    }
    
    public void setOutput(Map<String, Object> output) {
        this.output = output;
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
    
    public String getWorkerId() {
        return workerId;
    }
    
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
    
    public int getAttemptNumber() {
        return attemptNumber;
    }
    
    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public boolean isRetriable() {
        return retriable;
    }
    
    public void setRetriable(boolean retriable) {
        this.retriable = retriable;
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
    
    public Instant getStartedAt() {
        return startedAt;
    }
    
    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }
    
    public Instant getCompletedAt() {
        return completedAt;
    }
    
    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }
    
    public Long getDurationMs() {
        return durationMs;
    }
    
    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }
}
