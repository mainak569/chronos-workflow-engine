package com.chronos.workflow.service;

import com.chronos.workflow.domain.*;
import com.chronos.workflow.event.WorkflowEventPublisher;
import com.chronos.workflow.exception.WorkflowNotFoundException;
import com.chronos.workflow.repository.TaskExecutionRepository;
import com.chronos.workflow.repository.WorkflowExecutionRepository;
import com.chronos.workflow.repository.WorkflowRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration tests for WorkflowExecutionService.
 * Tests workflow execution scenarios including dependencies, parallel tasks, and completion.
 */
@ExtendWith(MockitoExtension.class)
class WorkflowExecutionServiceTest {
    
    @Mock
    private WorkflowRepository workflowRepository;
    
    @Mock
    private WorkflowExecutionRepository executionRepository;
    
    @Mock
    private TaskExecutionRepository taskExecutionRepository;
    
    @Mock
    private WorkflowEventPublisher eventPublisher;
    
    private WorkflowExecutionService service;
    
    @BeforeEach
    void setUp() {
        service = new WorkflowExecutionService(
                workflowRepository,
                executionRepository,
                taskExecutionRepository,
                eventPublisher
        );
    }
    
    @Test
    void testStartExecution_SimpleWorkflow() {
        // Arrange
        String workflowId = "wf-123";
        String triggeredBy = "user-456";
        Map<String, Object> input = Map.of("key", "value");
        
        Workflow workflow = createSimpleWorkflow(workflowId);
        when(workflowRepository.findById(workflowId)).thenReturn(Optional.of(workflow));
        
        WorkflowExecution savedExecution = WorkflowExecution.builder()
                .id("exec-789")
                .workflowId(workflowId)
                .workflowName(workflow.getName())
                .triggeredBy(triggeredBy)
                .status(ExecutionStatus.PENDING)
                .input(input)
                .build();
        when(executionRepository.save(any(WorkflowExecution.class))).thenReturn(savedExecution);
        
        // Act
        WorkflowExecution result = service.startExecution(workflowId, triggeredBy, input);
        
        // Assert
        assertNotNull(result);
        assertEquals(workflowId, result.getWorkflowId());
        assertEquals(triggeredBy, result.getTriggeredBy());
        assertEquals(ExecutionStatus.PENDING, result.getStatus());
        assertEquals(input, result.getInput());
        
        verify(workflowRepository).findById(workflowId);
        verify(executionRepository).save(any(WorkflowExecution.class));
        verify(taskExecutionRepository).saveAll(anyList());
        verify(eventPublisher).publishWorkflowCreated(any(WorkflowExecution.class));
    }
    
    @Test
    void testStartExecution_WorkflowNotFound() {
        // Arrange
        String workflowId = "non-existent";
        when(workflowRepository.findById(workflowId)).thenReturn(Optional.empty());
        
        // Act & Assert
        assertThrows(WorkflowNotFoundException.class,
                () -> service.startExecution(workflowId, "user", new HashMap<>()));
        
        verify(workflowRepository).findById(workflowId);
        verify(executionRepository, never()).save(any());
        verify(eventPublisher, never()).publishWorkflowCreated(any());
    }
    
    @Test
    void testGetReadyTasks_NoDependencies() {
        // Arrange
        String executionId = "exec-123";
        
        TaskExecution task1 = createTaskExecution("task-1", executionId, ExecutionStatus.PENDING, List.of());
        TaskExecution task2 = createTaskExecution("task-2", executionId, ExecutionStatus.PENDING, List.of());
        
        when(taskExecutionRepository.findByExecutionId(executionId))
                .thenReturn(List.of(task1, task2));
        
        // Act
        List<String> readyTasks = service.getReadyTasks(executionId);
        
        // Assert
        assertEquals(2, readyTasks.size());
        assertTrue(readyTasks.contains("task-1"));
        assertTrue(readyTasks.contains("task-2"));
    }
    
    @Test
    void testGetReadyTasks_WithDependencies() {
        // Arrange
        String executionId = "exec-123";
        
        // task-1: no dependencies, COMPLETED
        TaskExecution task1 = createTaskExecution("task-1", executionId, ExecutionStatus.COMPLETED, List.of());
        
        // task-2: depends on task-1, COMPLETED
        TaskExecution task2 = createTaskExecution("task-2", executionId, ExecutionStatus.COMPLETED, List.of("task-1"));
        
        // task-3: depends on task-1, PENDING -> should be ready
        TaskExecution task3 = createTaskExecution("task-3", executionId, ExecutionStatus.PENDING, List.of("task-1"));
        
        // task-4: depends on task-2 and task-3, PENDING -> not ready (task-3 not completed)
        TaskExecution task4 = createTaskExecution("task-4", executionId, ExecutionStatus.PENDING, List.of("task-2", "task-3"));
        
        when(taskExecutionRepository.findByExecutionId(executionId))
                .thenReturn(List.of(task1, task2, task3, task4));
        
        // Act
        List<String> readyTasks = service.getReadyTasks(executionId);
        
        // Assert
        assertEquals(1, readyTasks.size());
        assertTrue(readyTasks.contains("task-3"));
        assertFalse(readyTasks.contains("task-4")); // Not ready yet
    }
    
    @Test
    void testGetReadyTasks_ParallelTasks() {
        // Arrange
        String executionId = "exec-123";
        
        // task-1: no dependencies, COMPLETED
        TaskExecution task1 = createTaskExecution("task-1", executionId, ExecutionStatus.COMPLETED, List.of());
        
        // task-2, task-3, task-4: all depend on task-1, all PENDING -> all ready
        TaskExecution task2 = createTaskExecution("task-2", executionId, ExecutionStatus.PENDING, List.of("task-1"));
        TaskExecution task3 = createTaskExecution("task-3", executionId, ExecutionStatus.PENDING, List.of("task-1"));
        TaskExecution task4 = createTaskExecution("task-4", executionId, ExecutionStatus.PENDING, List.of("task-1"));
        
        when(taskExecutionRepository.findByExecutionId(executionId))
                .thenReturn(List.of(task1, task2, task3, task4));
        
        // Act
        List<String> readyTasks = service.getReadyTasks(executionId);
        
        // Assert
        assertEquals(3, readyTasks.size());
        assertTrue(readyTasks.contains("task-2"));
        assertTrue(readyTasks.contains("task-3"));
        assertTrue(readyTasks.contains("task-4"));
    }
    
    @Test
    void testUpdateExecutionStatus_TransitionToRunning() {
        // Arrange
        String executionId = "exec-123";
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId("wf-123")
                .status(ExecutionStatus.PENDING)
                .build();
        
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(execution));
        when(taskExecutionRepository.countByExecutionId(executionId)).thenReturn(3L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED)).thenReturn(0L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED)).thenReturn(0L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING)).thenReturn(1L);
        
        // Act
        service.updateExecutionStatus(executionId);
        
        // Assert
        ArgumentCaptor<WorkflowExecution> captor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(captor.capture());
        
        WorkflowExecution saved = captor.getValue();
        assertEquals(ExecutionStatus.RUNNING, saved.getStatus());
        assertNotNull(saved.getStartedAt());
    }
    
    @Test
    void testUpdateExecutionStatus_CompleteWorkflow() {
        // Arrange
        String executionId = "exec-123";
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId("wf-123")
                .status(ExecutionStatus.RUNNING)
                .build();
        execution.start(); // Set startedAt
        
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(execution));
        when(taskExecutionRepository.countByExecutionId(executionId)).thenReturn(3L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED)).thenReturn(3L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED)).thenReturn(0L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING)).thenReturn(0L);
        when(taskExecutionRepository.findCompletedTasksByExecution(executionId)).thenReturn(List.of());
        
        // Act
        service.updateExecutionStatus(executionId);
        
        // Assert
        ArgumentCaptor<WorkflowExecution> captor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(captor.capture());
        
        WorkflowExecution saved = captor.getValue();
        assertEquals(ExecutionStatus.COMPLETED, saved.getStatus());
        assertNotNull(saved.getCompletedAt());
        assertNotNull(saved.getDurationMs());
    }
    
    @Test
    void testUpdateExecutionStatus_FailWorkflow() {
        // Arrange
        String executionId = "exec-123";
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId("wf-123")
                .status(ExecutionStatus.RUNNING)
                .build();
        execution.start();
        
        TaskExecution failedTask = createTaskExecution("task-1", executionId, ExecutionStatus.FAILED, List.of());
        failedTask.setRetriable(false); // Non-retriable failure
        
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(execution));
        when(taskExecutionRepository.countByExecutionId(executionId)).thenReturn(3L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED)).thenReturn(1L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED)).thenReturn(1L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING)).thenReturn(0L);
        when(taskExecutionRepository.findFailedTasksByExecution(executionId)).thenReturn(List.of(failedTask));
        
        // Act
        service.updateExecutionStatus(executionId);
        
        // Assert
        ArgumentCaptor<WorkflowExecution> captor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(captor.capture());
        
        WorkflowExecution saved = captor.getValue();
        assertEquals(ExecutionStatus.FAILED, saved.getStatus());
        assertNotNull(saved.getErrorMessage());
        assertNotNull(saved.getCompletedAt());
    }
    
    @Test
    void testUpdateExecutionStatus_AlreadyTerminal() {
        // Arrange
        String executionId = "exec-123";
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId("wf-123")
                .status(ExecutionStatus.COMPLETED)
                .build();
        
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(execution));
        
        // Act
        service.updateExecutionStatus(executionId);
        
        // Assert - should not update
        verify(executionRepository, never()).save(any());
        verify(taskExecutionRepository, never()).countByExecutionId(anyString());
    }
    
    @Test
    void testCancelExecution() {
        // Arrange
        String executionId = "exec-123";
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId("wf-123")
                .status(ExecutionStatus.RUNNING)
                .build();
        
        TaskExecution task1 = createTaskExecution("task-1", executionId, ExecutionStatus.COMPLETED, List.of());
        TaskExecution task2 = createTaskExecution("task-2", executionId, ExecutionStatus.RUNNING, List.of());
        TaskExecution task3 = createTaskExecution("task-3", executionId, ExecutionStatus.PENDING, List.of());
        
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(execution));
        when(taskExecutionRepository.findByExecutionId(executionId))
                .thenReturn(List.of(task1, task2, task3));
        
        // Act
        service.cancelExecution(executionId);
        
        // Assert
        verify(executionRepository).save(argThat(exec -> exec.getStatus() == ExecutionStatus.CANCELLED));
        verify(taskExecutionRepository).saveAll(argThat(tasks -> {
            List<TaskExecution> taskList = (List<TaskExecution>) tasks;
            // task1 should remain COMPLETED, task2 and task3 should be CANCELLED
            return taskList.stream().anyMatch(t -> t.getTaskId().equals("task-2") && t.getStatus() == ExecutionStatus.CANCELLED) &&
                   taskList.stream().anyMatch(t -> t.getTaskId().equals("task-3") && t.getStatus() == ExecutionStatus.CANCELLED);
        }));
    }
    
    @Test
    void testGetExecutionStatistics() {
        // Arrange
        String executionId = "exec-123";
        
        when(taskExecutionRepository.countByExecutionId(executionId)).thenReturn(10L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.PENDING)).thenReturn(2L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING)).thenReturn(3L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED)).thenReturn(4L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED)).thenReturn(1L);
        when(taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.CANCELLED)).thenReturn(0L);
        
        // Act
        WorkflowExecutionService.ExecutionStatistics stats = service.getExecutionStatistics(executionId);
        
        // Assert
        assertEquals(10L, stats.getTotalTasks());
        assertEquals(2L, stats.getPendingTasks());
        assertEquals(3L, stats.getRunningTasks());
        assertEquals(4L, stats.getCompletedTasks());
        assertEquals(1L, stats.getFailedTasks());
        assertEquals(0L, stats.getCancelledTasks());
        assertEquals(40.0, stats.getCompletionPercentage(), 0.01);
    }
    
    // Helper methods
    
    private Workflow createSimpleWorkflow(String workflowId) {
        TaskDefinition task1 = new TaskDefinition();
        task1.setTaskId("task-1");
        task1.setName("Task 1");
        task1.setTaskType("IMAGE_RESIZE");
        task1.setDependencies(new HashSet<>());
        
        TaskDefinition task2 = new TaskDefinition();
        task2.setTaskId("task-2");
        task2.setName("Task 2");
        task2.setTaskType("DATA_PROCESSING");
        task2.setDependencies(new HashSet<>(List.of("task-1")));
        
        Workflow workflow = new Workflow();
        workflow.setId(workflowId);
        workflow.setName("Test Workflow");
        workflow.setOwnerId("owner-123");
        workflow.setTasks(List.of(task1, task2));
        
        return workflow;
    }
    
    private TaskExecution createTaskExecution(String taskId, String executionId, 
                                             ExecutionStatus status, List<String> dependsOn) {
        TaskExecution task = TaskExecution.builder()
                .id("exec-" + taskId)
                .executionId(executionId)
                .workflowId("wf-123")
                .taskId(taskId)
                .taskName("Task " + taskId)
                .taskType("TEST_TYPE")
                .status(status)
                .dependsOn(dependsOn)
                .configuration(new HashMap<>())
                .attemptNumber(1)
                .maxRetries(3)
                .retriable(true)
                .build();
        
        if (status == ExecutionStatus.COMPLETED) {
            task.setOutput(Map.of("result", "success"));
        }
        
        return task;
    }
}
