package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.event.TaskReadyEvent;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.outbox.OutboxService;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Main workflow scheduling service.
 * 
 * Responsibilities:
 * 1. Identify workflows ready to execute (based on schedule)
 * 2. Create workflow executions (with idempotency)
 * 3. Determine runnable tasks (no pending dependencies)
 * 4. Write TaskReady events to outbox (transactional)
 * 5. Update scheduler state (next execution time)
 * 
 * CRITICAL: Only executes when this instance is the elected leader.
 * Never uses local state - always checks Redis for leadership.
 * 
 * TRANSACTIONAL OUTBOX PATTERN:
 * Instead of publishing directly to Kafka (which is not atomic with MongoDB),
 * this service writes events to an outbox table in the same transaction.
 * OutboxPublisherService polls the outbox and publishes to Kafka separately.
 * 
 * This ensures atomicity: either both MongoDB write + outbox write succeed,
 * or neither does. No partial failures.
 * 
 * Concurrency Safety:
 * - Leader election ensures only one scheduler runs
 * - Optimistic locking prevents duplicate scheduling
 * - Idempotency prevents duplicate executions
 */
@Service
public class WorkflowScheduler {
    
    private static final Logger log = LoggerFactory.getLogger(WorkflowScheduler.class);
    
    private static final String TASK_READY_TOPIC = "chronos.task.ready";
    
    private final LeaderElectionService leaderElectionService;
    private final SchedulerStateRepository schedulerStateRepository;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final ExecutionOrchestrationService orchestrationService;
    private final ExecutionIdempotencyService idempotencyService;
    private final OutboxService outboxService;
    
    @Value("${scheduler.id}")
    private String schedulerId;
    
    @Value("${chronos.scheduler.scheduling.batch-size:100}")
    private int batchSize;
    
    public WorkflowScheduler(
            LeaderElectionService leaderElectionService,
            SchedulerStateRepository schedulerStateRepository,
            WorkflowExecutionRepository workflowExecutionRepository,
            TaskExecutionRepository taskExecutionRepository,
            ExecutionOrchestrationService orchestrationService,
            ExecutionIdempotencyService idempotencyService,
            OutboxService outboxService) {
        this.leaderElectionService = leaderElectionService;
        this.schedulerStateRepository = schedulerStateRepository;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.orchestrationService = orchestrationService;
        this.idempotencyService = idempotencyService;
        this.outboxService = outboxService;
    }
    
    /**
     * Main scheduling loop.
     * Runs every 5 seconds to identify and trigger ready workflows.
     * 
     * Only executes if this instance is the leader.
     */
    @Scheduled(fixedDelayString = "${chronos.scheduler.scheduling.interval:5000}")
    public void scheduleWorkflows() {
        // CRITICAL: Check leadership in Redis (not local state)
        if (!leaderElectionService.isLeader()) {
            log.trace("Not the leader, skipping scheduling");
            return;
        }
        
        log.debug("Starting scheduling scan (schedulerId={})", schedulerId);
        
        try {
            // Find workflows ready to execute
            Instant now = Instant.now();
            List<SchedulerState> readyWorkflows = schedulerStateRepository.findReadyToExecute(now);
            
            if (readyWorkflows.isEmpty()) {
                log.trace("No workflows ready to execute");
                return;
            }
            
            log.info("Found {} workflows ready to execute", readyWorkflows.size());
            
            // Process each workflow
            int scheduled = 0;
            int skipped = 0;
            int failed = 0;
            
            for (SchedulerState state : readyWorkflows) {
                try {
                    boolean success = scheduleWorkflow(state, now);
                    if (success) {
                        scheduled++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    failed++;
                    log.error("Failed to schedule workflow: workflowId={}", 
                            state.getWorkflowId(), e);
                }
            }
            
            log.info("Scheduling scan complete: scheduled={}, skipped={}, failed={}", 
                    scheduled, skipped, failed);
            
        } catch (Exception e) {
            log.error("Error in scheduling loop", e);
        }
    }
    
    /**
     * Schedule a single workflow.
     * 
     * Steps:
     * 1. Check idempotency (already executed this time slot?)
     * 2. Create WorkflowExecution (or get existing)
     * 3. Find runnable tasks
     * 4. Write TaskReady events to outbox (transactional)
     * 5. Update SchedulerState with next execution time
     * 
     * IMPORTANT: All database writes happen in one transaction.
     * Events are written to outbox, not published directly to Kafka.
     * 
     * @param state scheduler state for workflow
     * @param now current time
     * @return true if scheduled, false if skipped (duplicate)
     */
    @Transactional
    protected boolean scheduleWorkflow(SchedulerState state, Instant now) {
        String workflowId = state.getWorkflowId();
        
        log.info("Scheduling workflow: workflowId={}, lastExecution={}, nextScheduled={}",
                workflowId, state.getLastExecutionTime(), state.getNextScheduledTime());
        
        try {
            // Step 1: Get or create execution with idempotency
            String executionId = idempotencyService.getOrCreateExecution(
                    workflowId,
                    state.getNextScheduledTime(),
                    () -> createWorkflowExecution(workflowId)
            );
            
            // Step 2: Find runnable tasks
            List<TaskExecution> runnableTasks = orchestrationService.getReadyTasks(executionId);
            
            if (runnableTasks.isEmpty()) {
                log.warn("No runnable tasks for execution: workflowId={}, executionId={}",
                        workflowId, executionId);
            }
            
            // Step 3: Publish TaskReady events to outbox (transactional)
            for (TaskExecution task : runnableTasks) {
                writeTaskReadyToOutbox(task);
            }
            
            // Step 4: Calculate next schedule time
            Instant nextScheduleTime = calculateNextScheduleTime(
                    state.getSchedule(),
                    state.getTimezone(),
                    now
            );
            
            // Step 5: Update scheduler state with optimistic locking
            state.markExecuted(executionId, ExecutionStatus.RUNNING, nextScheduleTime, schedulerId);
            schedulerStateRepository.save(state);
            
            log.info("Workflow scheduled successfully: workflowId={}, executionId={}, " +
                            "runnableTasks={}, nextSchedule={}",
                    workflowId, executionId, runnableTasks.size(), nextScheduleTime);
            
            return true;
            
        } catch (OptimisticLockingFailureException e) {
            // Another scheduler already processed this workflow
            log.info("Workflow already scheduled by another instance: workflowId={}", workflowId);
            return false;
        }
    }
    
    /**
     * Create a new workflow execution.
     * 
     * @param workflowId workflow identifier
     * @return execution ID
     */
    private String createWorkflowExecution(String workflowId) {
        String executionId = "exec-" + UUID.randomUUID().toString();
        
        // Create WorkflowExecution
        WorkflowExecution execution = new WorkflowExecution();
        execution.setId(executionId);
        execution.setWorkflowId(workflowId);
        execution.setStatus(ExecutionStatus.PENDING);
        execution.setCreatedAt(Instant.now());
        
        workflowExecutionRepository.save(execution);
        
        log.info("Created workflow execution: workflowId={}, executionId={}", 
                workflowId, executionId);
        
        // TODO: Create TaskExecution records from workflow definition
        // For now, this assumes tasks are created elsewhere or exist
        // In production, fetch workflow definition and create task executions
        
        return executionId;
    }
    
    /**
     * Write TaskReady event to outbox for later publishing.
     * Called within a transaction to ensure atomicity.
     * 
     * @param task task execution to publish
     */
    private void writeTaskReadyToOutbox(TaskExecution task) {
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
        
        // Write to outbox instead of publishing directly
        outboxService.createMessage(
                TASK_READY_TOPIC,
                task.getTaskId(),
                event,
                TaskReadyEvent.class.getName()
        );
        
        log.info("Written TaskReady to outbox: executionId={}, taskId={}, eventId={}",
                task.getExecutionId(), task.getTaskId(), event.getEventId());
    }
    
    /**
     * Calculate next schedule time based on cron expression.
     * 
     * @param cronExpression cron expression (e.g., "0 * * * *")
     * @param timezone timezone for evaluation
     * @param fromTime time to calculate from
     * @return next execution time
     */
    private Instant calculateNextScheduleTime(String cronExpression, String timezone, Instant fromTime) {
        try {
            CronExpression cron = CronExpression.parse(cronExpression);
            ZoneId zoneId = ZoneId.of(timezone != null ? timezone : "UTC");
            ZonedDateTime fromZoned = ZonedDateTime.ofInstant(fromTime, zoneId);
            
            ZonedDateTime next = cron.next(fromZoned);
            
            if (next == null) {
                log.warn("No next execution time for cron: {}", cronExpression);
                // Fallback: schedule 1 hour from now
                return fromTime.plus(java.time.Duration.ofHours(1));
            }
            
            return next.toInstant();
            
        } catch (Exception e) {
            log.error("Error calculating next schedule time: cron={}, timezone={}", 
                    cronExpression, timezone, e);
            // Fallback: schedule 1 hour from now
            return fromTime.plus(java.time.Duration.ofHours(1));
        }
    }
}
