package com.chronos.workflow.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WorkflowExecution state transitions.
 */
class WorkflowExecutionTest {
    
    private WorkflowExecution execution;
    
    @BeforeEach
    void setUp() {
        execution = WorkflowExecution.builder()
                .id("exec-123")
                .workflowId("wf-456")
                .workflowName("Test Workflow")
                .triggeredBy("user-789")
                .status(ExecutionStatus.PENDING)
                .input(new HashMap<>())
                .build();
    }
    
    @Test
    void testStartTransitionFromPending() {
        assertEquals(ExecutionStatus.PENDING, execution.getStatus());
        assertNull(execution.getStartedAt());
        
        execution.start();
        
        assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
        assertNotNull(execution.getStartedAt());
        assertNull(execution.getCompletedAt());
    }
    
    @Test
    void testCompleteTransitionFromRunning() {
        execution.start();
        assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
        
        Map<String, Object> output = Map.of("result", "success");
        execution.complete(output);
        
        assertEquals(ExecutionStatus.COMPLETED, execution.getStatus());
        assertEquals(output, execution.getOutput());
        assertNotNull(execution.getCompletedAt());
        assertNotNull(execution.getDurationMs());
        assertTrue(execution.getDurationMs() >= 0);
    }
    
    @Test
    void testFailTransitionFromRunning() {
        execution.start();
        assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
        
        String errorMessage = "Task failed";
        String errorType = "TASK_FAILURE";
        execution.fail(errorMessage, errorType);
        
        assertEquals(ExecutionStatus.FAILED, execution.getStatus());
        assertEquals(errorMessage, execution.getErrorMessage());
        assertEquals(errorType, execution.getErrorType());
        assertNotNull(execution.getCompletedAt());
        assertNotNull(execution.getDurationMs());
    }
    
    @Test
    void testCancelTransitionFromPending() {
        assertEquals(ExecutionStatus.PENDING, execution.getStatus());
        
        execution.cancel();
        
        assertEquals(ExecutionStatus.CANCELLED, execution.getStatus());
        assertNotNull(execution.getCompletedAt());
        assertNull(execution.getDurationMs()); // No duration since never started
    }
    
    @Test
    void testCancelTransitionFromRunning() {
        execution.start();
        assertEquals(ExecutionStatus.RUNNING, execution.getStatus());
        
        execution.cancel();
        
        assertEquals(ExecutionStatus.CANCELLED, execution.getStatus());
        assertNotNull(execution.getCompletedAt());
        assertNotNull(execution.getDurationMs());
    }
    
    @Test
    void testInvalidTransitionFromPendingToCompleted() {
        assertEquals(ExecutionStatus.PENDING, execution.getStatus());
        
        Map<String, Object> output = Map.of("result", "success");
        
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> execution.complete(output));
        
        assertTrue(exception.getMessage().contains("Invalid state transition"));
        assertEquals(ExecutionStatus.PENDING, execution.getStatus()); // Status unchanged
    }
    
    @Test
    void testInvalidTransitionFromCompletedToRunning() {
        execution.start();
        Map<String, Object> output = Map.of("result", "success");
        execution.complete(output);
        
        assertEquals(ExecutionStatus.COMPLETED, execution.getStatus());
        
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> execution.start());
        
        assertTrue(exception.getMessage().contains("Invalid state transition"));
        assertEquals(ExecutionStatus.COMPLETED, execution.getStatus()); // Status unchanged
    }
    
    @Test
    void testInvalidTransitionFromFailedToRunning() {
        execution.start();
        execution.fail("Error", "ERROR_TYPE");
        
        assertEquals(ExecutionStatus.FAILED, execution.getStatus());
        
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> execution.start());
        
        assertTrue(exception.getMessage().contains("Invalid state transition"));
        assertEquals(ExecutionStatus.FAILED, execution.getStatus()); // Status unchanged
    }
    
    @Test
    void testBuilder() {
        Map<String, Object> input = Map.of("key", "value");
        
        WorkflowExecution built = WorkflowExecution.builder()
                .id("exec-999")
                .workflowId("wf-888")
                .workflowName("Built Workflow")
                .triggeredBy("user-777")
                .status(ExecutionStatus.PENDING)
                .input(input)
                .build();
        
        assertEquals("exec-999", built.getId());
        assertEquals("wf-888", built.getWorkflowId());
        assertEquals("Built Workflow", built.getWorkflowName());
        assertEquals("user-777", built.getTriggeredBy());
        assertEquals(ExecutionStatus.PENDING, built.getStatus());
        assertEquals(input, built.getInput());
    }
}
