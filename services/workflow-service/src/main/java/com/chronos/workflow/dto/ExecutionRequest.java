package com.chronos.workflow.dto;

import java.util.HashMap;
import java.util.Map;

/**
 * Request to start a workflow execution.
 * The user starting the execution is taken from the authenticated JWT, never from the body.
 */
public class ExecutionRequest {
    
    /**
     * Input parameters for the workflow execution.
     */
    private Map<String, Object> input;
    
    public ExecutionRequest() {
        this.input = new HashMap<>();
    }
    
    public ExecutionRequest(Map<String, Object> input) {
        this.input = input != null ? input : new HashMap<>();
    }
    
    public Map<String, Object> getInput() {
        return input;
    }
    
    public void setInput(Map<String, Object> input) {
        this.input = input;
    }
}
