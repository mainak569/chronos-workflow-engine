package com.chronos.workflow.exception;

/**
 * Exception thrown when a workflow execution (or one of its tasks) does not exist
 * or is not visible to the requesting user.
 */
public class ExecutionNotFoundException extends RuntimeException {

    public ExecutionNotFoundException(String message) {
        super(message);
    }
}
