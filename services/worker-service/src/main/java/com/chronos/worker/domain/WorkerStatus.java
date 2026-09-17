package com.chronos.worker.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * Worker status enum with state machine transition rules.
 * Defines valid state transitions for worker lifecycle.
 */
public enum WorkerStatus {
    /**
     * Worker is in the process of registering with the system.
     * Initial state when worker starts up.
     */
    REGISTERING,
    
    /**
     * Worker is registered and available to accept tasks.
     */
    AVAILABLE,
    
    /**
     * Worker is currently executing a task.
     */
    BUSY,
    
    /**
     * Worker is unavailable (heartbeat expired or manually marked unavailable).
     * Can transition back to AVAILABLE if heartbeat resumes.
     */
    UNAVAILABLE,
    
    /**
     * Worker has been stopped/deregistered.
     * Terminal state.
     */
    STOPPED;
    
    /**
     * Check if this status is a terminal state.
     */
    public boolean isTerminal() {
        return this == STOPPED;
    }
    
    /**
     * Check if worker can accept new tasks in this state.
     */
    public boolean canAcceptTasks() {
        return this == AVAILABLE;
    }
    
    /**
     * Check if transition from this status to target status is valid.
     *
     * @param target The target status to transition to
     * @return true if transition is valid, false otherwise
     */
    public boolean canTransitionTo(WorkerStatus target) {
        // Terminal states cannot transition to anything
        if (this.isTerminal()) {
            return false;
        }
        
        return getValidTransitions().contains(target);
    }
    
    /**
     * Get all valid transitions from this status.
     *
     * @return Set of valid target statuses
     */
    public Set<WorkerStatus> getValidTransitions() {
        return switch (this) {
            case REGISTERING -> EnumSet.of(AVAILABLE, STOPPED);
            case AVAILABLE -> EnumSet.of(BUSY, UNAVAILABLE, STOPPED);
            case BUSY -> EnumSet.of(AVAILABLE, UNAVAILABLE, STOPPED);
            case UNAVAILABLE -> EnumSet.of(AVAILABLE, STOPPED);
            case STOPPED -> EnumSet.noneOf(WorkerStatus.class);
        };
    }
    
    /**
     * Validate that a transition is allowed and throw exception if not.
     *
     * @param target The target status
     * @throws IllegalStateException if transition is not valid
     */
    public void validateTransition(WorkerStatus target) {
        if (!canTransitionTo(target)) {
            throw new IllegalStateException(
                String.format("Invalid worker state transition from %s to %s. Valid transitions: %s",
                    this, target, getValidTransitions())
            );
        }
    }
}
