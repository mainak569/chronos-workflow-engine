package com.chronos.workflow.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ExecutionStatus state machine.
 */
class ExecutionStatusTest {
    
    @Test
    void testTerminalStates() {
        assertTrue(ExecutionStatus.COMPLETED.isTerminal());
        assertTrue(ExecutionStatus.FAILED.isTerminal());
        assertTrue(ExecutionStatus.CANCELLED.isTerminal());
        assertFalse(ExecutionStatus.PENDING.isTerminal());
        assertFalse(ExecutionStatus.RUNNING.isTerminal());
    }
    
    @Test
    void testValidTransitionsFromPending() {
        assertTrue(ExecutionStatus.PENDING.canTransitionTo(ExecutionStatus.RUNNING));
        assertTrue(ExecutionStatus.PENDING.canTransitionTo(ExecutionStatus.CANCELLED));
        assertFalse(ExecutionStatus.PENDING.canTransitionTo(ExecutionStatus.COMPLETED));
        assertFalse(ExecutionStatus.PENDING.canTransitionTo(ExecutionStatus.FAILED));
        assertFalse(ExecutionStatus.PENDING.canTransitionTo(ExecutionStatus.PENDING));
    }
    
    @Test
    void testValidTransitionsFromRunning() {
        assertTrue(ExecutionStatus.RUNNING.canTransitionTo(ExecutionStatus.COMPLETED));
        assertTrue(ExecutionStatus.RUNNING.canTransitionTo(ExecutionStatus.FAILED));
        assertTrue(ExecutionStatus.RUNNING.canTransitionTo(ExecutionStatus.CANCELLED));
        assertFalse(ExecutionStatus.RUNNING.canTransitionTo(ExecutionStatus.PENDING));
        assertFalse(ExecutionStatus.RUNNING.canTransitionTo(ExecutionStatus.RUNNING));
    }
    
    @Test
    void testNoTransitionsFromTerminalStates() {
        // COMPLETED cannot transition to anything
        assertFalse(ExecutionStatus.COMPLETED.canTransitionTo(ExecutionStatus.PENDING));
        assertFalse(ExecutionStatus.COMPLETED.canTransitionTo(ExecutionStatus.RUNNING));
        assertFalse(ExecutionStatus.COMPLETED.canTransitionTo(ExecutionStatus.FAILED));
        assertFalse(ExecutionStatus.COMPLETED.canTransitionTo(ExecutionStatus.CANCELLED));
        assertFalse(ExecutionStatus.COMPLETED.canTransitionTo(ExecutionStatus.COMPLETED));
        
        // FAILED cannot transition to anything
        assertFalse(ExecutionStatus.FAILED.canTransitionTo(ExecutionStatus.PENDING));
        assertFalse(ExecutionStatus.FAILED.canTransitionTo(ExecutionStatus.RUNNING));
        assertFalse(ExecutionStatus.FAILED.canTransitionTo(ExecutionStatus.COMPLETED));
        
        // CANCELLED cannot transition to anything
        assertFalse(ExecutionStatus.CANCELLED.canTransitionTo(ExecutionStatus.PENDING));
        assertFalse(ExecutionStatus.CANCELLED.canTransitionTo(ExecutionStatus.RUNNING));
    }
    
    @Test
    void testValidateTransitionSuccess() {
        // Should not throw for valid transitions
        assertDoesNotThrow(() -> ExecutionStatus.PENDING.validateTransition(ExecutionStatus.RUNNING));
        assertDoesNotThrow(() -> ExecutionStatus.PENDING.validateTransition(ExecutionStatus.CANCELLED));
        assertDoesNotThrow(() -> ExecutionStatus.RUNNING.validateTransition(ExecutionStatus.COMPLETED));
        assertDoesNotThrow(() -> ExecutionStatus.RUNNING.validateTransition(ExecutionStatus.FAILED));
        assertDoesNotThrow(() -> ExecutionStatus.RUNNING.validateTransition(ExecutionStatus.CANCELLED));
    }
    
    @Test
    void testValidateTransitionThrowsForInvalidTransitions() {
        // Should throw IllegalStateException for invalid transitions
        IllegalStateException e1 = assertThrows(IllegalStateException.class,
                () -> ExecutionStatus.PENDING.validateTransition(ExecutionStatus.COMPLETED));
        assertTrue(e1.getMessage().contains("Invalid state transition"));
        
        IllegalStateException e2 = assertThrows(IllegalStateException.class,
                () -> ExecutionStatus.RUNNING.validateTransition(ExecutionStatus.PENDING));
        assertTrue(e2.getMessage().contains("Invalid state transition"));
        
        IllegalStateException e3 = assertThrows(IllegalStateException.class,
                () -> ExecutionStatus.COMPLETED.validateTransition(ExecutionStatus.RUNNING));
        assertTrue(e3.getMessage().contains("Invalid state transition"));
    }
    
    @Test
    void testGetValidTransitions() {
        assertEquals(2, ExecutionStatus.PENDING.getValidTransitions().size());
        assertTrue(ExecutionStatus.PENDING.getValidTransitions().contains(ExecutionStatus.RUNNING));
        assertTrue(ExecutionStatus.PENDING.getValidTransitions().contains(ExecutionStatus.CANCELLED));
        
        assertEquals(3, ExecutionStatus.RUNNING.getValidTransitions().size());
        assertTrue(ExecutionStatus.RUNNING.getValidTransitions().contains(ExecutionStatus.COMPLETED));
        assertTrue(ExecutionStatus.RUNNING.getValidTransitions().contains(ExecutionStatus.FAILED));
        assertTrue(ExecutionStatus.RUNNING.getValidTransitions().contains(ExecutionStatus.CANCELLED));
        
        assertEquals(0, ExecutionStatus.COMPLETED.getValidTransitions().size());
        assertEquals(0, ExecutionStatus.FAILED.getValidTransitions().size());
        assertEquals(0, ExecutionStatus.CANCELLED.getValidTransitions().size());
    }
}
