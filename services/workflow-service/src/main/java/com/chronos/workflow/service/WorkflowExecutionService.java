package com.chronos.workflow.service;

import com.chronos.workflow.domain.*;
import com.chronos.workflow.event.WorkflowEventPublisher;
import com.chronos.workflow.exception.WorkflowNotFoundException;
import com.chronos.workflow.repository.TaskExecutionRepository;
import com.chronos.workflow.repository.WorkflowExecutionRepository;
import com.chronos.workflow.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for managing workflow executions.
 * Handles workflow execution lifecycle, state transitions, and dependency resolution.
 */
@Service
public class WorkflowExecutionService {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionService.class);
    
    private final WorkflowRepository workflowRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final WorkflowEventPublisher eventPublisher;
    
    public WorkflowExecutionService(
            WorkflowRepository workflowRepository,
            WorkflowExecutionRepository executionRepository,
            TaskExecutionRepository taskExecutionRepository,
            WorkflowEventPublisher eventPublisher) {
        this.workflowRepository = workflowRepository;
        this.executionRepository = executionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.eventPublisher = eventPublisher;
    }
    
    /**
     * Start a new workflow execution.
     * Creates execution record, initializes task executions, and publishes WorkflowCreated event.
     *
     * @param workflowId Workflow definition ID
     * @param triggeredBy User who triggered the execution
     * @param input Input parameters for the workflow
     * @return Created workflow execution
     */
    @Transactional
    public WorkflowExecution startExecution(String workflowId, String triggeredBy, Map<String, Object> input) {
        logger.info("Starting workflow execution: workflowId={}, triggeredBy={}", workflowId, triggeredBy);
        
        // Load workflow definition
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new WorkflowNotFoundException("Workflow not found: " + workflowId));
        
        // Create workflow execution
        WorkflowExecution execution = WorkflowExecution.builder()
                .workflowId(workflowId)
                .workflowName(workflow.getName())
                .triggeredBy(triggeredBy)
                .status(ExecutionStatus.PENDING)
                .input(input != null ? input : new HashMap<>())
                .build();
        
        execution = executionRepository.save(execution);
        logger.info("Created workflow execution: executionId={}", execution.getId());
        
        // Initialize task executions
        initializeTaskExecutions(execution, workflow);
        
        // Publish WorkflowCreated event
        eventPublisher.publishWorkflowCreated(execution);
        
        logger.info("Workflow execution started successfully: executionId={}", execution.getId());
        return execution;
    }
    
    /**
     * Initialize task executions for all tasks in the workflow.
     * All tasks start in PENDING status.
     */
    private void initializeTaskExecutions(WorkflowExecution execution, Workflow workflow) {
        logger.debug("Initializing task executions for execution: executionId={}", execution.getId());
        
        List<TaskExecution> taskExecutions = workflow.getTasks().stream()
                .map(taskDef -> createTaskExecution(execution, taskDef))
                .collect(Collectors.toList());
        
        taskExecutionRepository.saveAll(taskExecutions);
        logger.info("Initialized {} task executions for executionId={}", 
                taskExecutions.size(), execution.getId());
    }
    
    /**
     * Create a TaskExecution from a TaskDefinition.
     */
    private TaskExecution createTaskExecution(WorkflowExecution execution, TaskDefinition taskDef) {
        return TaskExecution.builder()
                .executionId(execution.getId())
                .workflowId(execution.getWorkflowId())
                .taskId(taskDef.getTaskId())
                .taskName(taskDef.getName())
                .taskType(taskDef.getTaskType())
                .status(ExecutionStatus.PENDING)
                .dependsOn(taskDef.getDependencies() != null ? 
                        new ArrayList<>(taskDef.getDependencies()) : new ArrayList<>())
                .configuration(taskDef.getConfiguration() != null ? taskDef.getConfiguration() : new HashMap<>())
                .input(new HashMap<>())
                .attemptNumber(1)
                .maxRetries(taskDef.getRetryConfig() != null ? 
                        taskDef.getRetryConfig().getMaxAttempts() : 3)
                .retriable(true)
                .build();
    }
    
    /**
     * Get a workflow execution by ID.
     */
    public Optional<WorkflowExecution> getExecution(String executionId) {
        return executionRepository.findById(executionId);
    }
    
    /**
     * Get all task executions for a workflow execution.
     */
    public List<TaskExecution> getTaskExecutions(String executionId) {
        return taskExecutionRepository.findByExecutionId(executionId);
    }
    
    /**
     * Get a specific task execution.
     */
    public Optional<TaskExecution> getTaskExecution(String executionId, String taskId) {
        return taskExecutionRepository.findByExecutionIdAndTaskId(executionId, taskId);
    }
    
    /**
     * Determine which tasks are ready to execute based on dependency resolution.
     * A task is ready if:
     * 1. It is in PENDING status
     * 2. All its dependencies are COMPLETED
     *
     * @param executionId Workflow execution ID
     * @return List of task IDs that are ready to execute
     */
    public List<String> getReadyTasks(String executionId) {
        logger.debug("Determining ready tasks for executionId={}", executionId);
        
        // Get all task executions
        List<TaskExecution> allTasks = taskExecutionRepository.findByExecutionId(executionId);
        
        // Build a map for quick lookup
        Map<String, TaskExecution> taskMap = allTasks.stream()
                .collect(Collectors.toMap(TaskExecution::getTaskId, t -> t));
        
        // Find tasks that are ready
        List<String> readyTaskIds = allTasks.stream()
                .filter(task -> task.getStatus() == ExecutionStatus.PENDING)
                .filter(task -> task.areDependenciesSatisfied(taskMap))
                .map(TaskExecution::getTaskId)
                .collect(Collectors.toList());
        
        logger.info("Found {} ready tasks for executionId={}: {}", 
                readyTaskIds.size(), executionId, readyTaskIds);
        
        return readyTaskIds;
    }
    
    /**
     * Update workflow execution status and check for completion.
     * 
     * @param executionId Workflow execution ID
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
        
        // Check if workflow has failed (any non-retriable task failed)
        if (failedTasks > 0) {
            List<TaskExecution> failedTaskExecutions = taskExecutionRepository.findFailedTasksByExecution(executionId);
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
        
        // Check if workflow has completed (all tasks completed)
        if (completedTasks == totalTasks) {
            // Aggregate outputs from all tasks
            Map<String, Object> workflowOutput = aggregateTaskOutputs(executionId);
            execution.complete(workflowOutput);
            executionRepository.save(execution);
            logger.info("Execution completed successfully: executionId={}, totalTasks={}", 
                    executionId, totalTasks);
        }
    }
    
    /**
     * Aggregate outputs from all completed tasks into workflow output.
     */
    private Map<String, Object> aggregateTaskOutputs(String executionId) {
        List<TaskExecution> completedTasks = taskExecutionRepository.findCompletedTasksByExecution(executionId);
        
        Map<String, Object> aggregated = new HashMap<>();
        for (TaskExecution task : completedTasks) {
            if (task.getOutput() != null) {
                aggregated.put(task.getTaskId(), task.getOutput());
            }
        }
        
        return aggregated;
    }
    
    /**
     * Cancel a workflow execution.
     * Cancels all non-terminal tasks and transitions execution to CANCELLED.
     *
     * @param executionId Workflow execution ID
     */
    @Transactional
    public void cancelExecution(String executionId) {
        logger.info("Cancelling workflow execution: executionId={}", executionId);
        
        WorkflowExecution execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));
        
        // Cancel the workflow execution
        execution.cancel();
        executionRepository.save(execution);
        
        // Cancel all non-terminal tasks
        List<TaskExecution> tasks = taskExecutionRepository.findByExecutionId(executionId);
        for (TaskExecution task : tasks) {
            if (!task.getStatus().isTerminal()) {
                task.cancel();
            }
        }
        taskExecutionRepository.saveAll(tasks);
        
        logger.info("Workflow execution cancelled: executionId={}", executionId);
    }
    
    /**
     * Get execution statistics.
     */
    public ExecutionStatistics getExecutionStatistics(String executionId) {
        long total = taskExecutionRepository.countByExecutionId(executionId);
        long pending = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.PENDING);
        long running = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.RUNNING);
        long completed = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.COMPLETED);
        long failed = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.FAILED);
        long cancelled = taskExecutionRepository.countByExecutionIdAndStatus(executionId, ExecutionStatus.CANCELLED);
        
        return new ExecutionStatistics(total, pending, running, completed, failed, cancelled);
    }
    
    /**
     * Statistics for a workflow execution.
     */
    public static class ExecutionStatistics {
        private final long totalTasks;
        private final long pendingTasks;
        private final long runningTasks;
        private final long completedTasks;
        private final long failedTasks;
        private final long cancelledTasks;
        
        public ExecutionStatistics(long totalTasks, long pendingTasks, long runningTasks, 
                                 long completedTasks, long failedTasks, long cancelledTasks) {
            this.totalTasks = totalTasks;
            this.pendingTasks = pendingTasks;
            this.runningTasks = runningTasks;
            this.completedTasks = completedTasks;
            this.failedTasks = failedTasks;
            this.cancelledTasks = cancelledTasks;
        }
        
        public long getTotalTasks() {
            return totalTasks;
        }
        
        public long getPendingTasks() {
            return pendingTasks;
        }
        
        public long getRunningTasks() {
            return runningTasks;
        }
        
        public long getCompletedTasks() {
            return completedTasks;
        }
        
        public long getFailedTasks() {
            return failedTasks;
        }
        
        public long getCancelledTasks() {
            return cancelledTasks;
        }
        
        public double getCompletionPercentage() {
            if (totalTasks == 0) {
                return 0.0;
            }
            return (completedTasks * 100.0) / totalTasks;
        }
    }
}
