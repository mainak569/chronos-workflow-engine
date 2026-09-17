package com.chronos.workflow.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.HashMap;
import java.util.Map;

/**
 * Request to start a workflow execution.
 */
public class ExecutionRequest {
    
    /**
     * User ID who is triggering the execution.
     */
    @NotBlank(message = "triggeredBy is required")
    private String triggeredBy;
    
    /**
     * Input parameters for the workflow execution.
     */
    private Map<String, Object> input;
    
    public ExecutionRequest() {
        this.input = new HashMap<>();
    }
    
    public ExecutionRequest(String triggeredBy, Map<String, Object> input) {
        this.triggeredBy = triggeredBy;
        this.input = input != null ? input : new HashMap<>();
    }
    
    public String getTriggeredBy() {
        return triggeredBy;
    }
    
    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }
    
    public Map<String, Object> getInput() {
        return input;
    }
    
    public void setInput(Map<String, Object> input) {
        this.input = input;
    }
}
