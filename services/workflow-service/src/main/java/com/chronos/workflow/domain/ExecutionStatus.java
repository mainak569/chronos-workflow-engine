package com.chronos.workflow.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * Execution status enum with state machine transition rules.
 * Defines valid state transitions for both workflow and task executions.
 */
public enum ExecutionStatus {
    /**
     * Execution has been created but not yet started.
     * Initial state for new executions.
     */
    PENDING,
    
    /**
     * Execution is currently in progress.
     */
    RUNNING,
    
    /**
     * Execution completed successfully.
     * Terminal state.
     */
    COMPLETED,
    
    /**
     * Execution failed and cannot be automatically retried.
     * Terminal state.
     */
    FAILED,
    
    /**
     * Execution was explicitly cancelled by user or system.
     * Terminal state.
     */
    CANCELLED;
    
    /**
     * Check if this status is a terminal state.
     * Terminal states cannot transition to any other state.
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
    
    /**
     * Check if transition from this status to target status is valid.
     *
     * @param target The target status to transition to
     * @return true if transition is valid, false otherwise
     */
    public boolean canTransitionTo(ExecutionStatus target) {
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
    public Set<ExecutionStatus> getValidTransitions() {
        return switch (this) {
            case PENDING -> EnumSet.of(RUNNING, CANCELLED);
            case RUNNING -> EnumSet.of(COMPLETED, FAILED, CANCELLED);
            case COMPLETED, FAILED, CANCELLED -> EnumSet.noneOf(ExecutionStatus.class);
        };
    }
    
    /**
     * Validate that a transition is allowed and throw exception if not.
     *
     * @param target The target status
     * @throws IllegalStateException if transition is not valid
     */
    public void validateTransition(ExecutionStatus target) {
        if (!canTransitionTo(target)) {
            throw new IllegalStateException(
                String.format("Invalid state transition from %s to %s. Valid transitions: %s",
                    this, target, getValidTransitions())
            );
        }
    }
}
