package com.chronos.workflow.exception;

/**
 * Exception thrown when an operation is not allowed in the execution's current state
 * (for example cancelling an execution that already finished).
 */
public class InvalidExecutionStateException extends RuntimeException {

    public InvalidExecutionStateException(String message) {
        super(message);
    }
}
