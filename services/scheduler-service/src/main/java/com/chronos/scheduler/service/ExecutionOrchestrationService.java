package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.metrics.SchedulerMetrics;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Service for orchestrating workflow execution state in scheduler-service.
 *
 * Applies task status events reported by workers to the task and workflow execution documents.
 * Kafka delivers events at least once and started/completed/failed arrive on different topics,
 * so every update is idempotent and tolerates out-of-order delivery:
 * - duplicate events for a task that already reached the reported state are ignored
 * - events for an older attempt (the task was retried or requeued since) are ignored
 * - a completion or failure that arrives before its "started" event implicitly starts the task
 *
 * Concurrent updates of the same document are detected with optimistic locking and retried.
 */
@Service
public class ExecutionOrchestrationService {
    
    private static final Logger logger = LoggerFactory.getLogger(ExecutionOrchestrationService.class);
    
    private static final int MAX_CONFLICT_RETRIES = 5;
    
    private final WorkflowExecutionRepository executionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final SchedulerMetrics metrics;
    
    @Value("${chronos.scheduler.retry.initial-delay:5s}")
    private Duration defaultRetryInitialDelay = Duration.ofSeconds(5);
    
    @Value("${chronos.scheduler.retry.backoff-multiplier:2.0}")
    private double defaultRetryBackoffMultiplier = 2.0;
    
    @Value("${chronos.scheduler.retry.max-delay:5m}")
    private Duration defaultRetryMaxDelay = Duration.ofMinutes(5);
    
    public ExecutionOrchestrationService(
            WorkflowExecutionRepository executionRepository,
            TaskExecutionRepository taskExecutionRepository,
            SchedulerMetrics metrics) {
        this.executionRepository = executionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.metrics = metrics;
    }
    
    /**
     * Find tasks that can be dispatched now: PENDING, not yet dispatched for their current
     * attempt, past any retry backoff, and with all dependencies COMPLETED.
     */
    public List<TaskExecution> getReadyTasks(String executionId) {
        logger.debug("Finding ready tasks for executionId={}", executionId);
        
        List<TaskExecution> allTasks = taskExecutionRepository.findByExecutionId(executionId);
        Map<String, TaskExecution> taskMap = allTasks.stream()
                .collect(Collectors.toMap(TaskExecution::getTaskId, t -> t));
        
        Instant now = Instant.now();
        List<TaskExecution> readyTasks = allTasks.stream()
                .filter(task -> task.isDispatchable(taskMap, now))
                .collect(Collectors.toList());
        
        logger.debug("Found {} ready tasks for executionId={}", readyTasks.size(), executionId);
        
        return readyTasks;
    }
    
    /**
     * Recalculate the workflow execution status from its tasks.
     * - PENDING becomes RUNNING once any task has left PENDING
     * - any permanently FAILED task fails the execution and cancels tasks that have not started
     * - all tasks COMPLETED completes the execution with the aggregated task outputs
     */
    public void updateExecutionStatus(String executionId) {
        withConflictRetry(() -> {
            doUpdateExecutionStatus(executionId);
            return null;
        });
    }
    
    private void doUpdateExecutionStatus(String executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            logger.warn("Execution not found while updating status: executionId={}", executionId);
            return;
        }
        if (execution.getStatus().isTerminal()) {
            logger.debug("Execution already in terminal state: executionId={}, status={}",
                    executionId, execution.getStatus());
            return;
        }
        
        List<TaskExecution> tasks = taskExecutionRepository.findByExecutionId(executionId);
        if (tasks.isEmpty()) {
            return;
        }
        
        Map<ExecutionStatus, Long> counts = tasks.stream()
                .collect(Collectors.groupingBy(TaskExecution::getStatus, Collectors.counting()));
        long total = tasks.size();
        long pending = counts.getOrDefault(ExecutionStatus.PENDING, 0L);
        long completed = counts.getOrDefault(ExecutionStatus.COMPLETED, 0L);
        long failed = counts.getOrDefault(ExecutionStatus.FAILED, 0L);
        
        logger.debug("Execution stats: executionId={}, total={}, pending={}, completed={}, failed={}",
                executionId, total, pending, completed, failed);
        
        boolean changed = false;
        
        if (execution.getStatus() == ExecutionStatus.PENDING && pending < total) {
            execution.start();
            changed = true;
            logger.info("Transitioned execution to RUNNING: executionId={}", executionId);
        }
        
        if (failed > 0) {
            if (execution.getStatus() == ExecutionStatus.PENDING) {
                execution.start();
            }
            execution.fail(String.format("Workflow failed due to task failures: %d task(s) failed", failed),
                    "TASK_FAILURE");
            executionRepository.save(execution);
            metrics.recordExecutionFinished(ExecutionStatus.FAILED.name(), execution.getDurationMs());
            cancelUnstartedTasks(tasks);
            logger.info("Execution failed: executionId={}, failedTasks={}", executionId, failed);
            return;
        }
        
        if (completed == total) {
            Map<String, Object> workflowOutput = new HashMap<>();
            for (TaskExecution task : tasks) {
                if (task.getOutput() != null) {
                    workflowOutput.put(task.getTaskId(), task.getOutput());
                }
            }
            execution.complete(workflowOutput);
            changed = true;
            logger.info("Execution completed successfully: executionId={}, totalTasks={}", executionId, total);
        }
        
        if (changed) {
            executionRepository.save(execution);
            if (execution.getStatus() == ExecutionStatus.COMPLETED) {
                metrics.recordExecutionFinished(ExecutionStatus.COMPLETED.name(), execution.getDurationMs());
            }
        }
    }
    
    private void cancelUnstartedTasks(List<TaskExecution> tasks) {
        for (TaskExecution task : tasks) {
            if (task.getStatus() == ExecutionStatus.PENDING) {
                try {
                    task.cancel();
                    taskExecutionRepository.save(task);
                } catch (OptimisticLockingFailureException e) {
                    // A worker picked the task up concurrently; its status event will be ignored/logged
                    logger.debug("Task changed while cancelling: executionId={}, taskId={}",
                            task.getExecutionId(), task.getTaskId());
                }
            }
        }
    }
    
    /**
     * Mark a task as RUNNING on a worker.
     */
    public void markTaskAsRunning(String executionId, String taskId, String workerId, Integer attemptNumber) {
        boolean updated = withConflictRetry(() -> {
            TaskExecution task = findTask(executionId, taskId);
            if (task == null || isStaleAttempt(task, attemptNumber, "started")) {
                return false;
            }
            if (task.getStatus() != ExecutionStatus.PENDING) {
                logger.debug("Ignoring started event, task already {}: executionId={}, taskId={}",
                        task.getStatus(), executionId, taskId);
                return false;
            }
            task.start(workerId);
            taskExecutionRepository.save(task);
            logger.info("Task marked as running: executionId={}, taskId={}, workerId={}, attempt={}",
                    executionId, taskId, workerId, task.getAttemptNumber());
            return true;
        });
        
        if (updated) {
            updateExecutionStatus(executionId);
        }
    }
    
    /**
     * Mark a task as COMPLETED with its output.
     */
    public void markTaskAsCompleted(String executionId, String taskId, String workerId,
                                    Integer attemptNumber, Map<String, Object> output) {
        boolean updated = withConflictRetry(() -> {
            TaskExecution task = findTask(executionId, taskId);
            if (task == null || isStaleAttempt(task, attemptNumber, "completed")) {
                return false;
            }
            if (task.getStatus().isTerminal()) {
                logger.debug("Ignoring completed event, task already {}: executionId={}, taskId={}",
                        task.getStatus(), executionId, taskId);
                return false;
            }
            if (task.getStatus() == ExecutionStatus.PENDING) {
                // The started event has not been processed yet
                task.start(workerId);
            }
            task.complete(output);
            taskExecutionRepository.save(task);
            metrics.recordTaskOutcome("completed");
            logger.info("Task marked as completed: executionId={}, taskId={}, attempt={}",
                    executionId, taskId, task.getAttemptNumber());
            return true;
        });
        
        if (updated) {
            updateExecutionStatus(executionId);
        }
    }
    
    /**
     * Record a failed task attempt.
     * Retriable failures with attempts left are put back to PENDING with exponential backoff;
     * otherwise the task is marked FAILED, which fails the workflow execution.
     */
    public void markTaskAsFailed(String executionId, String taskId, String workerId, Integer attemptNumber,
                                 String errorMessage, String errorType, boolean retriable) {
        boolean updated = withConflictRetry(() -> {
            TaskExecution task = findTask(executionId, taskId);
            if (task == null || isStaleAttempt(task, attemptNumber, "failed")) {
                return false;
            }
            if (task.getStatus().isTerminal()) {
                logger.debug("Ignoring failed event, task already {}: executionId={}, taskId={}",
                        task.getStatus(), executionId, taskId);
                return false;
            }
            if (task.getStatus() == ExecutionStatus.PENDING) {
                task.start(workerId);
            }
            
            task.setRetriable(retriable);
            boolean retrying = task.canRetry();
            if (retrying) {
                Duration delay = calculateRetryDelay(task);
                task.scheduleRetry(errorMessage, errorType, Instant.now().plus(delay));
                logger.info("Task attempt failed, retry scheduled: executionId={}, taskId={}, nextAttempt={}/{}, delay={}",
                        executionId, taskId, task.getAttemptNumber(), task.getMaxRetries(), delay);
            } else {
                task.fail(errorMessage, errorType, retriable);
                logger.warn("Task failed permanently: executionId={}, taskId={}, attempts={}, retriable={}",
                        executionId, taskId, task.getAttemptNumber(), retriable);
            }
            taskExecutionRepository.save(task);
            metrics.recordTaskOutcome(retrying ? "retried" : "failed");
            return true;
        });
        
        if (updated) {
            updateExecutionStatus(executionId);
        }
    }
    
    /**
     * Put tasks that were RUNNING on a lost worker back to PENDING so they are dispatched again.
     *
     * @return the requeued tasks
     */
    public List<TaskExecution> requeueTasksOfWorker(String workerId) {
        List<TaskExecution> requeued = new ArrayList<>();
        for (TaskExecution task : taskExecutionRepository.findByStatusAndWorkerId(ExecutionStatus.RUNNING, workerId)) {
            try {
                task.requeue();
                taskExecutionRepository.save(task);
                metrics.recordTaskOutcome("requeued");
                requeued.add(task);
                logger.warn("Requeued task of unavailable worker: workerId={}, executionId={}, taskId={}",
                        workerId, task.getExecutionId(), task.getTaskId());
            } catch (OptimisticLockingFailureException e) {
                logger.debug("Task changed while requeueing, skipping: executionId={}, taskId={}",
                        task.getExecutionId(), task.getTaskId());
            }
        }
        return requeued;
    }
    
    Duration calculateRetryDelay(TaskExecution task) {
        long initialMs = task.getRetryInitialDelayMs() != null
                ? task.getRetryInitialDelayMs() : defaultRetryInitialDelay.toMillis();
        double multiplier = task.getRetryBackoffMultiplier() != null
                ? task.getRetryBackoffMultiplier() : defaultRetryBackoffMultiplier;
        long maxMs = task.getRetryMaxDelayMs() != null
                ? task.getRetryMaxDelayMs() : defaultRetryMaxDelay.toMillis();
        
        // attemptNumber is the attempt that just failed (1-based)
        double delay = initialMs * Math.pow(multiplier, Math.max(0, task.getAttemptNumber() - 1));
        return Duration.ofMillis((long) Math.min(delay, maxMs));
    }
    
    private TaskExecution findTask(String executionId, String taskId) {
        TaskExecution task = taskExecutionRepository.findByExecutionIdAndTaskId(executionId, taskId).orElse(null);
        if (task == null) {
            logger.warn("Task execution not found: executionId={}, taskId={}", executionId, taskId);
        }
        return task;
    }
    
    private boolean isStaleAttempt(TaskExecution task, Integer reportedAttempt, String eventType) {
        if (reportedAttempt != null && reportedAttempt != task.getAttemptNumber()) {
            logger.info("Ignoring {} event for old attempt: executionId={}, taskId={}, reportedAttempt={}, currentAttempt={}",
                    eventType, task.getExecutionId(), task.getTaskId(), reportedAttempt, task.getAttemptNumber());
            return true;
        }
        return false;
    }
    
    private <T> T withConflictRetry(Supplier<T> action) {
        for (int attempt = 1; ; attempt++) {
            try {
                return action.get();
            } catch (OptimisticLockingFailureException e) {
                if (attempt >= MAX_CONFLICT_RETRIES) {
                    throw e;
                }
                logger.debug("Concurrent modification, retrying (attempt {}): {}", attempt, e.getMessage());
            }
        }
    }
}
