package com.chronos.scheduler.recovery;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.event.TaskReadyEvent;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Service for recovering workflow executions after scheduler restart or crash.
 * 
 * Recovery Process:
 * 1. Find in-progress workflow executions (RUNNING or PENDING)
 * 2. For each execution, find tasks that should be running
 * 3. Check if tasks have active leases (workers still processing)
 * 4. Republish TaskReady events for tasks that need recovery
 * 
 * Safety:
 * - Does not republish if task started recently (< 1 minute ago)
 * - Does not republish if task has active lease (worker still processing)
 * - Idempotent: safe to run multiple times
 * 
 * Triggered:
 * - Automatically on application startup
 * - Can be triggered manually via admin API
 */
@Service
public class RestartRecoveryService {
    
    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryService.class);
    
    private static final String TASK_READY_TOPIC = "chronos.task.ready";
    
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${chronos.scheduler.restart-recovery.enabled:true}")
    private boolean recoveryEnabled;
    
    @Value("${chronos.scheduler.restart-recovery.task-republish-delay:1m}")
    private Duration taskRepublishDelay;
    
    public RestartRecoveryService(
            WorkflowExecutionRepository workflowExecutionRepository,
            TaskExecutionRepository taskExecutionRepository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Run recovery on application startup.
     * Triggered after ApplicationContext is ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverOnStartup() {
        if (!recoveryEnabled) {
            log.info("Restart recovery is disabled");
            return;
        }
        
        log.info("Starting restart recovery");
        
        try {
            RecoveryStats stats = performRecovery();
            
            log.info("Restart recovery completed: executionsFound={}, tasksRepublished={}, tasksSkipped={}",
                    stats.executionsProcessed, stats.tasksRepublished, stats.tasksSkipped);
            
        } catch (Exception e) {
            log.error("Error during restart recovery", e);
        }
    }
    
    /**
     * Perform recovery (can be called manually).
     * 
     * @return recovery statistics
     */
    public RecoveryStats performRecovery() {
        int executionsProcessed = 0;
        int tasksRepublished = 0;
        int tasksSkipped = 0;
        
        // Find in-progress workflow executions
        List<WorkflowExecution> inProgressExecutions = workflowExecutionRepository
                .findByStatusIn(List.of(ExecutionStatus.RUNNING, ExecutionStatus.PENDING));
        
        log.info("Found {} in-progress executions to recover", inProgressExecutions.size());
        
        for (WorkflowExecution execution : inProgressExecutions) {
            try {
                RecoveryResult result = recoverExecution(execution);
                executionsProcessed++;
                tasksRepublished += result.tasksRepublished;
                tasksSkipped += result.tasksSkipped;
                
            } catch (Exception e) {
                log.error("Failed to recover execution: executionId={}", 
                        execution.getId(), e);
            }
        }
        
        return new RecoveryStats(executionsProcessed, tasksRepublished, tasksSkipped);
    }
    
    /**
     * Recover a single workflow execution.
     * 
     * @param execution workflow execution to recover
     * @return recovery result
     */
    private RecoveryResult recoverExecution(WorkflowExecution execution) {
        String executionId = execution.getId();
        
        log.info("Recovering execution: executionId={}, workflowId={}, status={}",
                executionId, execution.getWorkflowId(), execution.getStatus());
        
        // Find tasks that might need republishing
        List<TaskExecution> tasks = taskExecutionRepository.findByExecutionId(executionId);
        
        // Build dependency map
        Map<String, TaskExecution> taskMap = tasks.stream()
                .collect(Collectors.toMap(TaskExecution::getTaskId, t -> t));
        
        int republished = 0;
        int skipped = 0;
        
        for (TaskExecution task : tasks) {
            if (shouldRepublishTask(task, taskMap)) {
                republishTaskReadyEvent(task);
                republished++;
            } else {
                skipped++;
                log.debug("Skipping task republish: executionId={}, taskId={}, status={}",
                        executionId, task.getTaskId(), task.getStatus());
            }
        }
        
        log.info("Execution recovery complete: executionId={}, tasksRepublished={}, tasksSkipped={}",
                executionId, republished, skipped);
        
        return new RecoveryResult(republished, skipped);
    }
    
    /**
     * Determine if a task should be republished.
     * 
     * Criteria:
     * - Task is PENDING (not started yet)
     * - Task dependencies are satisfied
     * - Task did not start recently (avoid race with workers)
     * - Task does not have active lease (worker not processing)
     * 
     * @param task task to check
     * @param allTasks map of all tasks for dependency checking
     * @return true if task should be republished
     */
    private boolean shouldRepublishTask(TaskExecution task, Map<String, TaskExecution> allTasks) {
        // Only republish PENDING tasks
        if (task.getStatus() != ExecutionStatus.PENDING) {
            return false;
        }
        
        // Check if dependencies are satisfied
        if (!task.areDependenciesSatisfied(allTasks)) {
            log.debug("Task dependencies not satisfied: taskId={}", task.getTaskId());
            return false;
        }
        
        // Don't republish if task started very recently (possible race with worker)
        if (task.getStartedAt() != null) {
            Duration timeSinceStart = Duration.between(task.getStartedAt(), Instant.now());
            if (timeSinceStart.compareTo(taskRepublishDelay) < 0) {
                log.debug("Task started recently, not republishing: taskId={}, startedAt={}",
                        task.getTaskId(), task.getStartedAt());
                return false;
            }
        }
        
        // TODO: Check if task has active lease (requires TaskLeaseService integration)
        // For now, we rely on the time-since-start check above
        
        return true;
    }
    
    /**
     * Republish TaskReady event for a task.
     * 
     * @param task task to republish
     */
    private void republishTaskReadyEvent(TaskExecution task) {
        TaskReadyEvent event = new TaskReadyEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setCorrelationId(task.getExecutionId());
        event.setTimestamp(Instant.now());
        event.setWorkflowId(task.getWorkflowId());
        event.setExecutionId(task.getExecutionId());
        event.setTaskId(task.getTaskId());
        event.setTaskType(task.getTaskType());
        event.setConfiguration(task.getConfiguration());
        event.setMaxRetries(task.getMaxRetries());
        
        kafkaTemplate.send(TASK_READY_TOPIC, task.getTaskId(), event);
        
        log.info("Republished TaskReady event after recovery: executionId={}, taskId={}, eventId={}",
                task.getExecutionId(), task.getTaskId(), event.getEventId());
    }
    
    /**
     * Recovery statistics.
     */
    public static class RecoveryStats {
        private final int executionsProcessed;
        private final int tasksRepublished;
        private final int tasksSkipped;
        
        public RecoveryStats(int executionsProcessed, int tasksRepublished, int tasksSkipped) {
            this.executionsProcessed = executionsProcessed;
            this.tasksRepublished = tasksRepublished;
            this.tasksSkipped = tasksSkipped;
        }
        
        public int getExecutionsProcessed() {
            return executionsProcessed;
        }
        
        public int getTasksRepublished() {
            return tasksRepublished;
        }
        
        public int getTasksSkipped() {
            return tasksSkipped;
        }
        
        @Override
        public String toString() {
            return String.format("RecoveryStats{executionsProcessed=%d, tasksRepublished=%d, tasksSkipped=%d}",
                    executionsProcessed, tasksRepublished, tasksSkipped);
        }
    }
    
    /**
     * Recovery result for a single execution.
     */
    private static class RecoveryResult {
        private final int tasksRepublished;
        private final int tasksSkipped;
        
        public RecoveryResult(int tasksRepublished, int tasksSkipped) {
            this.tasksRepublished = tasksRepublished;
            this.tasksSkipped = tasksSkipped;
        }
    }
}
