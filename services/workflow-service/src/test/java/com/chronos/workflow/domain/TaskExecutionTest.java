package com.chronos.workflow.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TaskExecution state transitions and dependency resolution.
 */
class TaskExecutionTest {
    
    private TaskExecution taskExecution;
    
    @BeforeEach
    void setUp() {
        taskExecution = TaskExecution.builder()
                .id("task-exec-123")
                .executionId("exec-456")
                .workflowId("wf-789")
                .taskId("task-1")
                .taskName("Test Task")
                .taskType("IMAGE_RESIZE")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of())
                .configuration(new HashMap<>())
                .attemptNumber(1)
                .maxRetries(3)
                .retriable(true)
                .build();
    }
    
    @Test
    void testStartTransition() {
        assertEquals(ExecutionStatus.PENDING, taskExecution.getStatus());
        assertNull(taskExecution.getStartedAt());
        assertNull(taskExecution.getWorkerId());
        
        String workerId = "worker-123";
        taskExecution.start(workerId);
        
        assertEquals(ExecutionStatus.RUNNING, taskExecution.getStatus());
        assertEquals(workerId, taskExecution.getWorkerId());
        assertNotNull(taskExecution.getStartedAt());
    }
    
    @Test
    void testCompleteTransition() {
        taskExecution.start("worker-123");
        
        Map<String, Object> output = Map.of("result", "completed");
        taskExecution.complete(output);
        
        assertEquals(ExecutionStatus.COMPLETED, taskExecution.getStatus());
        assertEquals(output, taskExecution.getOutput());
        assertNotNull(taskExecution.getCompletedAt());
        assertNotNull(taskExecution.getDurationMs());
    }
    
    @Test
    void testFailTransition() {
        taskExecution.start("worker-123");
        
        String errorMessage = "Task failed";
        String errorType = "PROCESSING_ERROR";
        boolean retriable = true;
        
        taskExecution.fail(errorMessage, errorType, retriable);
        
        assertEquals(ExecutionStatus.FAILED, taskExecution.getStatus());
        assertEquals(errorMessage, taskExecution.getErrorMessage());
        assertEquals(errorType, taskExecution.getErrorType());
        assertTrue(taskExecution.isRetriable());
        assertNotNull(taskExecution.getCompletedAt());
        assertNotNull(taskExecution.getDurationMs());
    }
    
    @Test
    void testCancelTransition() {
        taskExecution.start("worker-123");
        
        taskExecution.cancel();
        
        assertEquals(ExecutionStatus.CANCELLED, taskExecution.getStatus());
        assertNotNull(taskExecution.getCompletedAt());
    }
    
    @Test
    void testDependenciesSatisfied_NoDependencies() {
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of())
                .build();
        
        Map<String, TaskExecution> taskMap = new HashMap<>();
        assertTrue(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testDependenciesSatisfied_AllCompleted() {
        TaskExecution dep1 = TaskExecution.builder()
                .taskId("dep-1")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution dep2 = TaskExecution.builder()
                .taskId("dep-2")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of("dep-1", "dep-2"))
                .build();
        
        Map<String, TaskExecution> taskMap = Map.of(
                "dep-1", dep1,
                "dep-2", dep2
        );
        
        assertTrue(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testDependenciesNotSatisfied_OnePending() {
        TaskExecution dep1 = TaskExecution.builder()
                .taskId("dep-1")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution dep2 = TaskExecution.builder()
                .taskId("dep-2")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .build();
        
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of("dep-1", "dep-2"))
                .build();
        
        Map<String, TaskExecution> taskMap = Map.of(
                "dep-1", dep1,
                "dep-2", dep2
        );
        
        assertFalse(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testDependenciesNotSatisfied_OneRunning() {
        TaskExecution dep1 = TaskExecution.builder()
                .taskId("dep-1")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution dep2 = TaskExecution.builder()
                .taskId("dep-2")
                .executionId("exec-1")
                .status(ExecutionStatus.RUNNING)
                .build();
        
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of("dep-1", "dep-2"))
                .build();
        
        Map<String, TaskExecution> taskMap = Map.of(
                "dep-1", dep1,
                "dep-2", dep2
        );
        
        assertFalse(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testDependenciesNotSatisfied_OneFailed() {
        TaskExecution dep1 = TaskExecution.builder()
                .taskId("dep-1")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution dep2 = TaskExecution.builder()
                .taskId("dep-2")
                .executionId("exec-1")
                .status(ExecutionStatus.FAILED)
                .build();
        
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of("dep-1", "dep-2"))
                .build();
        
        Map<String, TaskExecution> taskMap = Map.of(
                "dep-1", dep1,
                "dep-2", dep2
        );
        
        assertFalse(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testDependenciesNotSatisfied_MissingDependency() {
        TaskExecution dep1 = TaskExecution.builder()
                .taskId("dep-1")
                .executionId("exec-1")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .status(ExecutionStatus.PENDING)
                .dependsOn(List.of("dep-1", "dep-2")) // dep-2 is missing
                .build();
        
        Map<String, TaskExecution> taskMap = Map.of(
                "dep-1", dep1
                // dep-2 is not in the map
        );
        
        assertFalse(task.areDependenciesSatisfied(taskMap));
    }
    
    @Test
    void testCanRetry_Retriable() {
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .attemptNumber(1)
                .maxRetries(3)
                .retriable(true)
                .build();
        
        assertTrue(task.canRetry());
    }
    
    @Test
    void testCanRetry_MaxRetriesReached() {
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .attemptNumber(3)
                .maxRetries(3)
                .retriable(true)
                .build();
        
        assertFalse(task.canRetry());
    }
    
    @Test
    void testCanRetry_NotRetriable() {
        TaskExecution task = TaskExecution.builder()
                .taskId("task-1")
                .executionId("exec-1")
                .attemptNumber(1)
                .maxRetries(3)
                .retriable(false)
                .build();
        
        assertFalse(task.canRetry());
    }
}
