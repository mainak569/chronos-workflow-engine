package com.chronos.workflow.dto;

import com.chronos.workflow.domain.ExecutionStatus;
import com.chronos.workflow.domain.WorkflowExecution;

import java.time.Instant;
import java.util.Map;

/**
 * Response containing workflow execution details.
 */
public class ExecutionResponse {
    
    private String executionId;
    private String workflowId;
    private String workflowName;
    private String triggeredBy;
    private ExecutionStatus status;
    private Map<String, Object> input;
    private Map<String, Object> output;
    private String errorMessage;
    private String errorType;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant startedAt;
    private Instant completedAt;
    private Long durationMs;
    
    public ExecutionResponse() {
    }
    
    public ExecutionResponse(WorkflowExecution execution) {
        this.executionId = execution.getId();
        this.workflowId = execution.getWorkflowId();
        this.workflowName = execution.getWorkflowName();
        this.triggeredBy = execution.getTriggeredBy();
        this.status = execution.getStatus();
        this.input = execution.getInput();
        this.output = execution.getOutput();
        this.errorMessage = execution.getErrorMessage();
        this.errorType = execution.getErrorType();
        this.createdAt = execution.getCreatedAt();
        this.updatedAt = execution.getUpdatedAt();
        this.startedAt = execution.getStartedAt();
        this.completedAt = execution.getCompletedAt();
        this.durationMs = execution.getDurationMs();
    }
    
    // Getters and Setters
    
    public String getExecutionId() {
        return executionId;
    }
    
    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }
    
    public String getWorkflowId() {
        return workflowId;
    }
    
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }
    
    public String getWorkflowName() {
        return workflowName;
    }
    
    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }
    
    public String getTriggeredBy() {
        return triggeredBy;
    }
    
    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }
    
    public ExecutionStatus getStatus() {
        return status;
    }
    
    public void setStatus(ExecutionStatus status) {
        this.status = status;
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
