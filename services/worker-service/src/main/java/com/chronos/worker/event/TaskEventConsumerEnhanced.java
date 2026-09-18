package com.chronos.worker.event;

import com.chronos.worker.config.KafkaTopics;
import com.chronos.worker.dlq.DeadLetterQueuePublisher;
import com.chronos.worker.idempotency.EventIdempotencyService;
import com.chronos.worker.lock.LockToken;
import com.chronos.worker.lock.TaskLeaseRenewalService;
import com.chronos.worker.lock.TaskLeaseService;
import com.chronos.worker.lock.TaskLockService;
import com.chronos.worker.metrics.WorkerMetrics;
import com.chronos.worker.service.WorkerRegistry;
import com.chronos.worker.shutdown.GracefulShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consumer for task ready events with fault tolerance.
 *
 * Features:
 * - Distributed locking for atomic task claiming (per execution + task)
 * - Task leases for execution tracking, renewed for long-running tasks
 * - Event idempotency to prevent duplicate processing
 * - Dead letter queue for permanently failed tasks
 *
 * Retries are owned by the scheduler: the worker reports each failed attempt
 * (with its attempt number and whether the error is retriable) and the scheduler
 * dispatches the next attempt after the configured backoff.
 *
 * Guarantees:
 * - At-least-once execution (may execute multiple times on failure)
 * - No unsafe concurrent execution of the same attempt (locks prevent races)
 * - Results are published before the TaskReady record is acknowledged
 * - No deadlocks from crashed workers (locks and leases expire)
 */
@Component
public class TaskEventConsumerEnhanced {

    private static final Logger log = LoggerFactory.getLogger(TaskEventConsumerEnhanced.class);

    private static final Duration REDELIVERY_DELAY = Duration.ofSeconds(1);

    /**
     * Runs task attempts so they can be abandoned when they exceed their timeout.
     */
    private final ExecutorService taskExecutor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "task-executor");
        thread.setDaemon(true);
        return thread;
    });

    @Value("${worker.id}")
    private String workerId;

    @Value("#{'${worker.supported-task-types}'.split(',')}")
    private List<String> supportedTaskTypes;

    private final WorkerEventPublisher eventPublisher;
    private final TaskLockService taskLockService;
    private final TaskLeaseService taskLeaseService;
    private final TaskLeaseRenewalService leaseRenewalService;
    private final WorkerRegistry workerRegistry;
    private final EventIdempotencyService idempotencyService;
    private final DeadLetterQueuePublisher dlqPublisher;
    private final GracefulShutdownManager shutdownManager;
    private final WorkerMetrics metrics;

    public TaskEventConsumerEnhanced(
            WorkerEventPublisher eventPublisher,
            TaskLockService taskLockService,
            TaskLeaseService taskLeaseService,
            TaskLeaseRenewalService leaseRenewalService,
            WorkerRegistry workerRegistry,
            EventIdempotencyService idempotencyService,
            DeadLetterQueuePublisher dlqPublisher,
            GracefulShutdownManager shutdownManager,
            WorkerMetrics metrics) {
        this.eventPublisher = eventPublisher;
        this.taskLockService = taskLockService;
        this.taskLeaseService = taskLeaseService;
        this.leaseRenewalService = leaseRenewalService;
        this.workerRegistry = workerRegistry;
        this.idempotencyService = idempotencyService;
        this.dlqPublisher = dlqPublisher;
        this.shutdownManager = shutdownManager;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = KafkaTopics.TASK_READY,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "taskReadyKafkaListenerContainerFactory"
    )
    public void handleTaskReady(
            @Payload TaskReadyEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        int attemptNumber = event.getAttemptNumber() != null ? event.getAttemptNumber() : 1;
        int maxRetries = event.getMaxRetries() != null ? event.getMaxRetries() : 3;
        // Task IDs are only unique within an execution, so every per-task key includes the execution
        String taskKey = event.getExecutionId() + ":" + event.getTaskId();

        log.info("Received TaskReadyEvent: executionId={}, taskId={}, taskType={}, attempt={}, eventId={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getTaskType(), attemptNumber,
                event.getEventId(), partition, offset);

        // Step 1: Shutdown in progress - hand the record back so it is redelivered
        if (shutdownManager.isShutdownRequested()) {
            log.warn("Shutdown in progress, returning task for redelivery: taskKey={}", taskKey);
            acknowledgment.nack(REDELIVERY_DELAY);
            return;
        }

        // Step 2: Idempotency check (events are marked processed only after the result is published)
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("Event already processed (idempotent skip): eventId={}", event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // Step 3: Check task type support
        if (!supportedTaskTypes.contains(event.getTaskType())) {
            // All workers share one consumer group, so no other worker will see this record:
            // report a permanent failure instead of silently dropping the task
            log.error("Unsupported task type {}: taskKey={}, supported={}",
                    event.getTaskType(), taskKey, supportedTaskTypes);
            publishFailure(event, attemptNumber, maxRetries,
                    new UnsupportedOperationException("Task type not supported by workers: " + event.getTaskType()),
                    false, 0);
            idempotencyService.markAsProcessed(event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // Step 4: Atomic task claiming
        Optional<LockToken> lockToken = taskLockService.claimTask(taskKey, workerId);
        if (lockToken.isEmpty()) {
            log.info("Task already being executed by another worker: taskKey={}", taskKey);
            acknowledgment.acknowledge();
            return;
        }

        // Step 5: Register for graceful shutdown tracking
        if (!shutdownManager.registerTask(taskKey)) {
            log.warn("Shutdown in progress, releasing task: taskKey={}", taskKey);
            taskLockService.releaseTask(taskKey, lockToken.get());
            acknowledgment.nack(REDELIVERY_DELAY);
            return;
        }

        try {
            // Step 6: Acquire execution lease and register it for automatic renewal
            taskLeaseService.acquireLease(taskKey, event.getExecutionId(), workerId,
                    lockToken.get().getToken(), attemptNumber);
            leaseRenewalService.registerTask(taskKey, lockToken.get());

            // Step 7: Execute task and publish the result
            processTask(event, attemptNumber, maxRetries);

            idempotencyService.markAsProcessed(event.getEventId());
        } finally {
            // Step 8: Cleanup
            shutdownManager.completeTask(taskKey);
            leaseRenewalService.unregisterTask(taskKey);
            taskLeaseService.releaseLease(taskKey, lockToken.get().getToken());
            taskLockService.releaseTask(taskKey, lockToken.get());
        }

        acknowledgment.acknowledge();
    }

    private void processTask(TaskReadyEvent event, int attemptNumber, int maxRetries) {
        markBusy(event);

        try {
            eventPublisher.publishTaskStarted(TaskStartedEvent.builder()
                    .correlationId(event.getCorrelationId())
                    .workflowId(event.getWorkflowId())
                    .executionId(event.getExecutionId())
                    .taskId(event.getTaskId())
                    .workerId(workerId)
                    .attemptNumber(attemptNumber)
                    .build());

            long startTime = System.currentTimeMillis();
            Map<String, Object> result;
            try {
                result = executeWithTimeout(event, attemptNumber);
            } catch (Exception taskError) {
                long duration = System.currentTimeMillis() - startTime;
                metrics.recordExecution(event.getTaskType(), "failure", duration);
                log.error("Task failed: executionId={}, taskId={}, attempt={}/{}, error={}",
                        event.getExecutionId(), event.getTaskId(), attemptNumber, maxRetries,
                        taskError.getMessage(), taskError);
                publishFailure(event, attemptNumber, maxRetries, taskError, isRetriable(taskError), duration);
                return;
            }

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordExecution(event.getTaskType(), "success", duration);
            eventPublisher.publishTaskCompleted(TaskCompletedEvent.builder()
                    .correlationId(event.getCorrelationId())
                    .workflowId(event.getWorkflowId())
                    .executionId(event.getExecutionId())
                    .taskId(event.getTaskId())
                    .workerId(workerId)
                    .attemptNumber(attemptNumber)
                    .result(result)
                    .durationMs(duration)
                    .build());

            log.info("Task completed: executionId={}, taskId={}, attempt={}, duration={}ms",
                    event.getExecutionId(), event.getTaskId(), attemptNumber, duration);
        } finally {
            markAvailable();
        }
    }

    /**
     * Registry bookkeeping is best effort: it must never fail a task that is otherwise fine.
     */
    private void markBusy(TaskReadyEvent event) {
        try {
            workerRegistry.markWorkerBusy(workerId, event.getExecutionId(), event.getTaskId());
        } catch (Exception e) {
            log.warn("Could not mark worker busy: workerId={}, error={}", workerId, e.getMessage());
        }
    }

    private void markAvailable() {
        try {
            workerRegistry.markWorkerAvailable(workerId);
        } catch (Exception e) {
            log.warn("Could not mark worker available: workerId={}, error={}", workerId, e.getMessage());
        }
    }

    private void publishFailure(TaskReadyEvent event, int attemptNumber, int maxRetries,
                                Exception error, boolean retriable, long durationMs) {
        eventPublisher.publishTaskFailed(TaskFailedEvent.builder()
                .correlationId(event.getCorrelationId())
                .workflowId(event.getWorkflowId())
                .executionId(event.getExecutionId())
                .taskId(event.getTaskId())
                .workerId(workerId)
                .attemptNumber(attemptNumber)
                .errorMessage(error.getMessage())
                .errorType(error.getClass().getSimpleName())
                .retriable(retriable)
                .durationMs(durationMs)
                .build());

        // The scheduler retries while attempts remain; otherwise the task is dead-lettered
        if (!retriable || attemptNumber >= maxRetries) {
            handlePermanentFailure(event, attemptNumber, maxRetries, error);
        }
    }

    private void handlePermanentFailure(TaskReadyEvent event, int finalAttempt,
                                        int maxRetries, Exception error) {
        log.error("Task permanently failed: executionId={}, taskId={}, attempts={}, reason={}",
                event.getExecutionId(), event.getTaskId(), finalAttempt, error.getMessage());

        TaskFailedPermanentlyEvent dlqEvent = TaskFailedPermanentlyEvent.builder()
                .correlationId(event.getCorrelationId())
                .workflowId(event.getWorkflowId())
                .executionId(event.getExecutionId())
                .taskId(event.getTaskId())
                .taskType(event.getTaskType())
                .totalAttempts(finalAttempt)
                .maxAttempts(maxRetries)
                .lastWorkerId(workerId)
                .lastErrorMessage(error.getMessage())
                .lastErrorType(error.getClass().getSimpleName())
                .configuration(event.getConfiguration())
                .lastAttemptAt(Instant.now())
                .build();

        dlqPublisher.publishToDeadLetterQueue(dlqEvent);
        metrics.recordDeadLettered();
    }

    private boolean isRetriable(Exception error) {
        // Retriable: transient network/IO failures
        if (error instanceof java.net.SocketTimeoutException ||
            error instanceof java.net.ConnectException ||
            error instanceof java.io.IOException ||
            error instanceof org.springframework.dao.TransientDataAccessException) {
            return true;
        }

        // Non-retriable: logic errors
        if (error instanceof IllegalArgumentException ||
            error instanceof NullPointerException ||
            error instanceof SecurityException ||
            error instanceof UnsupportedOperationException) {
            return false;
        }

        return true; // Default to retriable
    }

    /**
     * Run the attempt, aborting it (interrupt) if it exceeds the task's timeoutMs.
     */
    private Map<String, Object> executeWithTimeout(TaskReadyEvent event, int attemptNumber) throws Exception {
        Long timeoutMs = event.getTimeoutMs();
        if (timeoutMs == null || timeoutMs <= 0) {
            return executeTask(event, attemptNumber);
        }
        Future<Map<String, Object>> attempt = taskExecutor.submit(() -> executeTask(event, attemptNumber));
        try {
            return attempt.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            attempt.cancel(true);
            throw new TaskTimeoutException("Task exceeded its timeout of " + timeoutMs + "ms");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    @PreDestroy
    void shutdownExecutor() {
        taskExecutor.shutdownNow();
    }

    /**
     * Simulated task execution.
     *
     * Supported configuration keys (for testing failure handling):
     * - simulateDurationMs: how long the task runs (default 1000)
     * - failUntilAttempt: fail with a retriable error while attempt <= this value
     * - failPermanently: fail with a non-retriable error
     */
    private Map<String, Object> executeTask(TaskReadyEvent event, int attemptNumber) {
        log.info("Executing task: type={}, executionId={}, taskId={}, attempt={}",
                event.getTaskType(), event.getExecutionId(), event.getTaskId(), attemptNumber);

        Map<String, Object> config = event.getConfiguration() != null ? event.getConfiguration() : Map.of();

        try {
            Thread.sleep(asLong(config.get("simulateDurationMs"), 1000L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task interrupted", e);
        }

        if (Boolean.parseBoolean(String.valueOf(config.get("failPermanently")))) {
            throw new IllegalArgumentException("Simulated permanent failure");
        }
        if (attemptNumber <= asLong(config.get("failUntilAttempt"), 0L)) {
            throw new IllegalStateException("Simulated transient failure on attempt " + attemptNumber);
        }

        return Map.of(
                "status", "success",
                "taskId", event.getTaskId(),
                "attempt", attemptNumber,
                "processedBy", workerId
        );
    }

    private static long asLong(Object value, long defaultValue) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value != null) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }
        return defaultValue;
    }
}
