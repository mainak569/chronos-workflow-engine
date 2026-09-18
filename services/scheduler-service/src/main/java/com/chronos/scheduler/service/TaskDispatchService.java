package com.chronos.scheduler.service;

import com.chronos.scheduler.config.KafkaTopics;
import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.event.TaskReadyEvent;
import com.chronos.scheduler.metrics.SchedulerMetrics;
import com.chronos.scheduler.outbox.OutboxService;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Dispatches ready tasks to workers.
 *
 * A task is claimed for dispatch with an atomic conditional update (PENDING, not yet dispatched,
 * same attempt) before its TaskReady event is written to the outbox. Concurrent callers - the
 * event consumers and the recovery sweeps - therefore dispatch each attempt only once.
 * If the scheduler dies between the claim and the outbox write, the stale-dispatch sweep in
 * RestartRecoveryService releases the claim so the task is dispatched again.
 */
@Service
public class TaskDispatchService {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatchService.class);

    private final ExecutionOrchestrationService orchestrationService;
    private final WorkflowExecutionRepository executionRepository;
    private final OutboxService outboxService;
    private final MongoTemplate mongoTemplate;
    private final SchedulerMetrics metrics;

    public TaskDispatchService(ExecutionOrchestrationService orchestrationService,
                               WorkflowExecutionRepository executionRepository,
                               OutboxService outboxService,
                               MongoTemplate mongoTemplate,
                               SchedulerMetrics metrics) {
        this.orchestrationService = orchestrationService;
        this.executionRepository = executionRepository;
        this.outboxService = outboxService;
        this.mongoTemplate = mongoTemplate;
        this.metrics = metrics;
    }

    /**
     * Dispatch every task of the execution that is ready to run.
     *
     * @return number of tasks dispatched by this call
     */
    public int dispatchReadyTasks(String executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            log.warn("Cannot dispatch tasks, execution not found: executionId={}", executionId);
            return 0;
        }
        if (execution.getStatus().isTerminal()) {
            log.debug("Not dispatching tasks of finished execution: executionId={}, status={}",
                    executionId, execution.getStatus());
            return 0;
        }

        List<TaskExecution> readyTasks = orchestrationService.getReadyTasks(executionId);
        int dispatched = 0;
        for (TaskExecution task : readyTasks) {
            if (claimForDispatch(task)) {
                writeTaskReady(task, execution);
                metrics.recordTaskDispatched();
                dispatched++;
            }
        }

        if (dispatched > 0) {
            log.info("Dispatched {} task(s) for executionId={}", dispatched, executionId);
        }
        return dispatched;
    }

    /**
     * Release the dispatch claim of an attempt that was never picked up, so it is dispatched again.
     *
     * @return true if the claim was released
     */
    public boolean releaseStaleDispatch(TaskExecution task) {
        Query query = new Query(Criteria.where("_id").is(task.getId())
                .and("status").is(ExecutionStatus.PENDING)
                .and("attemptNumber").is(task.getAttemptNumber())
                .and("dispatchedAt").is(task.getDispatchedAt()));
        Update update = new Update().unset("dispatchedAt").inc("version", 1);
        return mongoTemplate.updateFirst(query, update, TaskExecution.class).getModifiedCount() == 1;
    }

    private boolean claimForDispatch(TaskExecution task) {
        Query query = new Query(Criteria.where("_id").is(task.getId())
                .and("status").is(ExecutionStatus.PENDING)
                .and("attemptNumber").is(task.getAttemptNumber())
                .and("dispatchedAt").is(null));
        Update update = new Update().set("dispatchedAt", Instant.now()).inc("version", 1);
        boolean claimed = mongoTemplate.updateFirst(query, update, TaskExecution.class).getModifiedCount() == 1;
        if (!claimed) {
            log.debug("Task already dispatched by another caller: executionId={}, taskId={}",
                    task.getExecutionId(), task.getTaskId());
        }
        return claimed;
    }

    private void writeTaskReady(TaskExecution task, WorkflowExecution execution) {
        TaskReadyEvent event = TaskReadyEvent.builder()
                .correlationId(task.getExecutionId())
                .workflowId(task.getWorkflowId())
                .executionId(task.getExecutionId())
                .taskId(task.getTaskId())
                .taskType(task.getTaskType())
                .configuration(task.getConfiguration())
                .maxRetries(task.getMaxRetries())
                .timeoutMs(task.getTimeoutMs())
                .attemptNumber(task.getAttemptNumber())
                .build();

        // Key by executionId so all tasks of one execution land on the same partition
        outboxService.createMessage(KafkaTopics.TASK_READY, task.getExecutionId(), event,
                TaskReadyEvent.class.getName());

        log.info("TaskReady queued: executionId={}, workflow={}, taskId={}, attempt={}, eventId={}",
                task.getExecutionId(), execution.getWorkflowName(), task.getTaskId(),
                task.getAttemptNumber(), event.getEventId());
    }
}
