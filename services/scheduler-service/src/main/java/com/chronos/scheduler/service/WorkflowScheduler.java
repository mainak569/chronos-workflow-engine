package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowDefinition;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowDefinitionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cron-based workflow scheduling.
 * 
 * Responsibilities:
 * 1. Keep scheduler_state in sync with workflows that have a cron schedule
 * 2. Identify scheduled workflows that are due
 * 3. Create workflow executions (with idempotency per workflow + scheduled time)
 * 4. Dispatch runnable tasks through TaskDispatchService (outbox)
 * 5. Advance the scheduler state to the next execution time
 * 
 * CRITICAL: Only executes when this instance is the elected leader.
 * Never uses local state - always checks Redis for leadership.
 * 
 * Crash safety (MongoDB runs without multi-document transactions):
 * - The execution ID for a time slot is reserved in Redis before anything is written,
 *   so a retried slot reuses the same execution ID.
 * - Task executions are inserted before the workflow execution; recovery only looks at
 *   workflow executions, so a partially created execution is never dispatched and is
 *   completed idempotently on the next attempt (duplicate inserts are ignored).
 * - The scheduler state is advanced last, so a crash re-runs the same slot.
 * 
 * Concurrency Safety:
 * - Leader election ensures only one scheduler runs
 * - Optimistic locking on scheduler state prevents duplicate scheduling
 * - Idempotency prevents duplicate executions
 */
@Service
public class WorkflowScheduler {
    
    private static final Logger log = LoggerFactory.getLogger(WorkflowScheduler.class);
    
    static final String SCHEDULER_TRIGGER = "scheduler";
    
    private final LeaderElectionService leaderElectionService;
    private final SchedulerStateRepository schedulerStateRepository;
    private final WorkflowDefinitionRepository workflowDefinitionRepository;
    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final ExecutionIdempotencyService idempotencyService;
    private final TaskDispatchService taskDispatchService;
    
    @Value("${scheduler.id}")
    private String schedulerId;
    
    @Value("${chronos.scheduler.scheduling.enabled:true}")
    private boolean schedulingEnabled = true;
    
    public WorkflowScheduler(
            LeaderElectionService leaderElectionService,
            SchedulerStateRepository schedulerStateRepository,
            WorkflowDefinitionRepository workflowDefinitionRepository,
            WorkflowExecutionRepository workflowExecutionRepository,
            TaskExecutionRepository taskExecutionRepository,
            ExecutionIdempotencyService idempotencyService,
            TaskDispatchService taskDispatchService) {
        this.leaderElectionService = leaderElectionService;
        this.schedulerStateRepository = schedulerStateRepository;
        this.workflowDefinitionRepository = workflowDefinitionRepository;
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.idempotencyService = idempotencyService;
        this.taskDispatchService = taskDispatchService;
    }
    
    /**
     * Synchronise scheduler_state with workflow definitions:
     * create state for newly scheduled workflows, update changed schedules and
     * remove state of workflows that were deleted or are no longer scheduled.
     */
    @Scheduled(fixedDelayString = "${chronos.scheduler.scheduling.sync-interval:15000}")
    public void syncSchedules() {
        if (!schedulingEnabled || !leaderElectionService.isLeader()) {
            return;
        }
        
        try {
            Instant now = Instant.now();
            List<WorkflowDefinition> scheduled = workflowDefinitionRepository.findScheduled();
            Set<String> scheduledIds = scheduled.stream().map(WorkflowDefinition::getId).collect(Collectors.toSet());
            
            for (WorkflowDefinition workflow : scheduled) {
                String timezone = workflow.getTimezone() != null ? workflow.getTimezone() : "UTC";
                SchedulerState state = schedulerStateRepository.findByWorkflowId(workflow.getId()).orElse(null);
                
                if (state == null) {
                    state = SchedulerState.builder()
                            .workflowId(workflow.getId())
                            .schedule(workflow.getSchedule())
                            .timezone(timezone)
                            .enabled(true)
                            .nextScheduledTime(calculateNextScheduleTime(workflow.getSchedule(), timezone, now))
                            .build();
                    saveState(state);
                    log.info("Registered schedule: workflowId={}, cron='{}', timezone={}, next={}",
                            workflow.getId(), workflow.getSchedule(), timezone, state.getNextScheduledTime());
                } else if (!Objects.equals(state.getSchedule(), workflow.getSchedule())
                        || !Objects.equals(state.getTimezone(), timezone)
                        || !state.isEnabled()) {
                    state.setSchedule(workflow.getSchedule());
                    state.setTimezone(timezone);
                    state.setEnabled(true);
                    state.setNextScheduledTime(calculateNextScheduleTime(workflow.getSchedule(), timezone, now));
                    saveState(state);
                    log.info("Updated schedule: workflowId={}, cron='{}', timezone={}, next={}",
                            workflow.getId(), workflow.getSchedule(), timezone, state.getNextScheduledTime());
                }
            }
            
            for (SchedulerState state : schedulerStateRepository.findAll()) {
                if (!scheduledIds.contains(state.getWorkflowId())) {
                    schedulerStateRepository.delete(state);
                    log.info("Removed schedule of deleted/unscheduled workflow: workflowId={}", state.getWorkflowId());
                }
            }
        } catch (Exception e) {
            log.error("Error synchronising workflow schedules", e);
        }
    }
    
    /**
     * Main scheduling loop.
     * Runs every 5 seconds (configurable).
     * Only executes if this instance is the leader.
     */
    @Scheduled(fixedDelayString = "${chronos.scheduler.scheduling.interval:5000}")
    public void scheduleWorkflows() {
        if (!schedulingEnabled || !leaderElectionService.isLeader()) {
            log.trace("Not the leader or scheduling disabled, skipping scheduling");
            return;
        }
        
        log.debug("Starting scheduling scan (schedulerId={})", schedulerId);
        
        try {
            Instant now = Instant.now();
            List<SchedulerState> readyWorkflows = schedulerStateRepository.findReadyToExecute(now);
            
            if (readyWorkflows.isEmpty()) {
                log.trace("No workflows ready to execute");
                return;
            }
            
            log.info("Found {} workflows ready to execute", readyWorkflows.size());
            
            int scheduled = 0;
            int skipped = 0;
            int failed = 0;
            
            for (SchedulerState state : readyWorkflows) {
                try {
                    if (scheduleWorkflow(state, now)) {
                        scheduled++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    failed++;
                    log.error("Failed to schedule workflow: workflowId={}", state.getWorkflowId(), e);
                }
            }
            
            log.info("Scheduling scan complete: scheduled={}, skipped={}, failed={}",
                    scheduled, skipped, failed);
            
        } catch (Exception e) {
            log.error("Error in scheduling loop", e);
        }
    }
    
    /**
     * Schedule a single workflow run for the state's next scheduled time.
     *
     * @return true if an execution was created/dispatched and the state advanced
     */
    boolean scheduleWorkflow(SchedulerState state, Instant now) {
        String workflowId = state.getWorkflowId();
        Instant slot = state.getNextScheduledTime();
        
        WorkflowDefinition workflow = workflowDefinitionRepository.findById(workflowId).orElse(null);
        if (workflow == null || workflow.getTasks().isEmpty()) {
            log.warn("Scheduled workflow no longer exists or has no tasks, disabling: workflowId={}", workflowId);
            state.disable();
            saveState(state);
            return false;
        }
        
        log.info("Scheduling workflow: workflowId={}, slot={}, lastExecution={}",
                workflowId, slot, state.getLastExecutionTime());
        
        // Reserve the execution ID for this slot (no side effects inside the supplier)
        String executionId = idempotencyService.getOrCreateExecution(
                workflowId, slot, () -> new ObjectId().toHexString());
        
        createExecutionIfAbsent(executionId, workflow, slot);
        int dispatched = taskDispatchService.dispatchReadyTasks(executionId);
        
        // Skip missed slots: the next run is computed from now
        Instant nextScheduleTime = calculateNextScheduleTime(state.getSchedule(), state.getTimezone(), now);
        state.markExecuted(executionId, ExecutionStatus.PENDING, nextScheduleTime, schedulerId);
        
        try {
            schedulerStateRepository.save(state);
        } catch (OptimisticLockingFailureException e) {
            log.info("Workflow schedule updated concurrently: workflowId={}", workflowId);
            return false;
        }
        
        log.info("Workflow scheduled: workflowId={}, executionId={}, dispatchedTasks={}, nextSchedule={}",
                workflowId, executionId, dispatched, nextScheduleTime);
        return true;
    }
    
    /**
     * Create the execution and its task executions unless they already exist.
     */
    private void createExecutionIfAbsent(String executionId, WorkflowDefinition workflow, Instant slot) {
        if (workflowExecutionRepository.existsById(executionId)) {
            return;
        }
        
        for (WorkflowDefinition.TaskSpec spec : workflow.getTasks()) {
            TaskExecution task = TaskExecution.builder()
                    .executionId(executionId)
                    .workflowId(workflow.getId())
                    .taskId(spec.getTaskId())
                    .taskName(spec.getName())
                    .taskType(spec.getTaskType())
                    .status(ExecutionStatus.PENDING)
                    .dependsOn(new ArrayList<>(spec.getDependencies()))
                    .configuration(spec.getConfiguration())
                    .input(new HashMap<>())
                    .attemptNumber(1)
                    .maxRetries(spec.getRetryConfig() != null && spec.getRetryConfig().getMaxAttempts() != null
                            ? spec.getRetryConfig().getMaxAttempts() : 3)
                    .retriable(true)
                    .build();
            task.setTimeoutMs(spec.getTimeoutMs());
            if (spec.getRetryConfig() != null) {
                task.setRetryInitialDelayMs(spec.getRetryConfig().getInitialDelayMs());
                task.setRetryBackoffMultiplier(spec.getRetryConfig().getBackoffMultiplier());
                task.setRetryMaxDelayMs(spec.getRetryConfig().getMaxDelayMs());
            }
            try {
                taskExecutionRepository.insert(task);
            } catch (DuplicateKeyException e) {
                log.debug("Task execution already exists: executionId={}, taskId={}", executionId, spec.getTaskId());
            }
        }
        
        Map<String, Object> input = new HashMap<>();
        input.put("scheduledTime", slot.toString());
        
        WorkflowExecution execution = WorkflowExecution.builder()
                .id(executionId)
                .workflowId(workflow.getId())
                .workflowName(workflow.getName())
                .ownerId(workflow.getOwnerId())
                .triggeredBy(SCHEDULER_TRIGGER)
                .status(ExecutionStatus.PENDING)
                .input(input)
                .build();
        try {
            workflowExecutionRepository.insert(execution);
            log.info("Created scheduled execution: workflowId={}, executionId={}, tasks={}",
                    workflow.getId(), executionId, workflow.getTasks().size());
        } catch (DuplicateKeyException e) {
            log.debug("Execution already exists: executionId={}", executionId);
        }
    }
    
    private void saveState(SchedulerState state) {
        try {
            schedulerStateRepository.save(state);
        } catch (OptimisticLockingFailureException | DuplicateKeyException e) {
            log.debug("Scheduler state changed concurrently: workflowId={}", state.getWorkflowId());
        }
    }
    
    /**
     * Calculate next schedule time based on cron expression.
     * 
     * @param cronExpression cron expression (Spring format with seconds)
     * @param timezone timezone for cron evaluation
     * @param fromTime calculate next time after this
     * @return next scheduled time
     */
    Instant calculateNextScheduleTime(String cronExpression, String timezone, Instant fromTime) {
        try {
            CronExpression cron = CronExpression.parse(cronExpression);
            ZoneId zoneId = ZoneId.of(timezone != null ? timezone : "UTC");
            ZonedDateTime next = cron.next(ZonedDateTime.ofInstant(fromTime, zoneId));
            
            if (next == null) {
                log.warn("No next execution time for cron: {}", cronExpression);
                return fromTime.plus(java.time.Duration.ofHours(1));
            }
            
            return next.toInstant();
            
        } catch (Exception e) {
            log.error("Error calculating next schedule time: cron={}, timezone={}",
                    cronExpression, timezone, e);
            return fromTime.plus(java.time.Duration.ofHours(1));
        }
    }
}
