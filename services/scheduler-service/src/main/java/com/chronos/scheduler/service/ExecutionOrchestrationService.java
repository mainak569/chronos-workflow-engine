package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for orchestrating workflow execution in scheduler-service.
 * Handles dependency resolution and state transitions.
 */
@Service
public class ExecutionOrchestrationService {
    
    private static final Logger logger = LoggerFactory.getLogger(ExecutionOrchestrationService.class);
    
    private final WorkflowExecutionRepository executionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    
    public ExecutionOrchestrationService(
            WorkflowExecutionRepository executionRepository,
            TaskExecutionRepository taskExecutionRepository) {
        this.executionRepository = executionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
    }
    
    /**
     * Get ready tasks for a workflow execution.
     * A task is ready if it's PENDING and all its dependencies are COMPLETED.
     */
    public List<TaskExecution> getReadyTasks(String executionId) {
        logger.debug("Finding ready tasks for executionId={}", executionId);
        
        // Get all task executions
        List<TaskExecution> allTasks = taskExecutionRepository.findByExecutionId(executionId);
        
        // Build a map for quick lookup
        Map<String, TaskExecution> taskMap = allTasks.stream()
                .collect(Collectors.toMap(TaskExecution::getTaskId, t -> t));
        
        // Find tasks that are ready
        List<TaskExecution> readyTasks = allTasks.stream()
                .filter(task -> task.getStatus() == ExecutionStatus.PENDING)
                .filter(task -> task.areDependenciesSatisfied(taskMap))
                .collect(Collectors.toList());
        
        logger.info("Found {} ready tasks for executionId={}", readyTasks.size(), executionId);
        
        return readyTasks;
    }
    
    /**
     * Update workflow execution status based on task states.
     */
    @Transactional
    public void updateExecutionStatus(String executionId) {
        logger.debug("Updating execution status for executionId={}", executionId);
        
        WorkflowExecution execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));
        
        // Don't update if already in terminal state
        if (execution.getStatus().isTerminal()) {
            logger.debug("Execution already in terminal state: executionId={}, status={}", 
                    executionId, execution.getStatus());
            return;
        }
        
        // Get task execution statistics
        long totalTasks = taskExecutionRepository.countByExecutionId(executionId);
        long completedTasks = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED);
        long failedTasks = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED);
        long runningTasks = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING);
        
        logger.debug("Execution stats: executionId={}, total={}, completed={}, failed={}, running={}", 
                executionId, totalTasks, completedTasks, failedTasks, runningTasks);
        
        // Transition to RUNNING if we have running tasks and still in PENDING
        if (execution.getStatus() == ExecutionStatus.PENDING && runningTasks > 0) {
            execution.start();
            executionRepository.save(execution);
            logger.info("Transitioned execution to RUNNING: executionId={}", executionId);
        }
        
        // Check if workflow has failed
        if (failedTasks > 0) {
            List<TaskExecution> failedTaskExecutions = taskExecutionRepository
                    .findByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED);
            boolean hasNonRetriableFailure = failedTaskExecutions.stream()
                    .anyMatch(task -> !task.canRetry());
            
            if (hasNonRetriableFailure) {
                String errorMessage = String.format("Workflow failed due to task failures: %d task(s) failed", failedTasks);
                execution.fail(errorMessage, "TASK_FAILURE");
                executionRepository.save(execution);
                logger.info("Execution failed: executionId={}, failedTasks={}", executionId, failedTasks);
                return;
            }
        }
        
        // Check if workflow has completed
        if (completedTasks == totalTasks) {
            // Aggregate outputs from all tasks
            List<TaskExecution> completedTaskExecutions = taskExecutionRepository
                    .findByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED);
            
            Map<String, Object> workflowOutput = completedTaskExecutions.stream()
                    .filter(task -> task.getOutput() != null)
                    .collect(Collectors.toMap(TaskExecution::getTaskId, TaskExecution::getOutput));
            
            execution.complete(workflowOutput);
            executionRepository.save(execution);
            logger.info("Execution completed successfully: executionId={}, totalTasks={}", 
                    executionId, totalTasks);
        }
    }
    
    /**
     * Update task execution state when it starts.
     */
    @Transactional
    public void markTaskAsRunning(String executionId, String taskId, String workerId) {
        logger.debug("Marking task as running: executionId={}, taskId={}, workerId={}", 
                executionId, taskId, workerId);
        
        TaskExecution taskExecution = taskExecutionRepository.findByExecutionIdAndTaskId(executionId, taskId)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Task execution not found: executionId=%s, taskId=%s", executionId, taskId)));
        
        taskExecution.start(workerId);
        taskExecutionRepository.save(taskExecution);
        
        logger.info("Task marked as running: executionId={}, taskId={}, workerId={}", 
                executionId, taskId, workerId);
        
        // Update workflow execution status
        updateExecutionStatus(executionId);
    }
    
    /**
     * Update task execution state when it completes.
     */
    @Transactional
    public void markTaskAsCompleted(String executionId, String taskId, Map<String, Object> output) {
        logger.debug("Marking task as completed: executionId={}, taskId={}", executionId, taskId);
        
        TaskExecution taskExecution = taskExecutionRepository.findByExecutionIdAndTaskId(executionId, taskId)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Task execution not found: executionId=%s, taskId=%s", executionId, taskId)));
        
        taskExecution.complete(output);
        taskExecutionRepository.save(taskExecution);
        
        logger.info("Task marked as completed: executionId={}, taskId={}", executionId, taskId);
        
        // Update workflow execution status
        updateExecutionStatus(executionId);
    }
    
    /**
     * Update task execution state when it fails.
     */
    @Transactional
    public void markTaskAsFailed(String executionId, String taskId, String errorMessage, 
                                String errorType, boolean retriable) {
        logger.debug("Marking task as failed: executionId={}, taskId={}, retriable={}", 
                executionId, taskId, retriable);
        
        TaskExecution taskExecution = taskExecutionRepository.findByExecutionIdAndTaskId(executionId, taskId)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Task execution not found: executionId=%s, taskId=%s", executionId, taskId)));
        
        taskExecution.fail(errorMessage, errorType, retriable);
        taskExecutionRepository.save(taskExecution);
        
        logger.info("Task marked as failed: executionId={}, taskId={}, retriable={}", 
                executionId, taskId, retriable);
        
        // Update workflow execution status
        updateExecutionStatus(executionId);
    }
}
