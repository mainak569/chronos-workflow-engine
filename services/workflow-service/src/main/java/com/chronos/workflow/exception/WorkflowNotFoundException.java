package com.chronos.workflow.exception;

/**
 * Exception thrown when a workflow is not found.
 */
public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("Workflow not found: " + workflowId);
    }
}
