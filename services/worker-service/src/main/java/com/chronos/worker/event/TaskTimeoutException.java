package com.chronos.worker.event;

/**
 * Thrown when a task attempt runs longer than its configured timeout.
 * Timeouts are treated as retriable failures.
 */
public class TaskTimeoutException extends RuntimeException {

    public TaskTimeoutException(String message) {
        super(message);
    }
}
