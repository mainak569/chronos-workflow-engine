package com.chronos.workflow.exception;

/**
 * Exception thrown when workflow validation fails.
 */
public class WorkflowValidationException extends RuntimeException {

    public WorkflowValidationException(String message) {
        super(message);
    }

    public WorkflowValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
