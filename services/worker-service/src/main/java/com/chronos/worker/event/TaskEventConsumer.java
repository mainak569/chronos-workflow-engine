package com.chronos.worker.event;

import com.chronos.worker.config.KafkaTopics;
import com.chronos.worker.lock.LockToken;
import com.chronos.worker.lock.TaskLockService;
import com.chronos.worker.service.WorkerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer for task ready events.
 * Workers pick up tasks they can execute based on task type.
 * Uses distributed locking to ensure only one worker processes each task.
 * 
 * Concurrency Safety:
 * - Multiple workers may receive the same TaskReadyEvent
 * - TaskLockService ensures atomic task claiming (only one succeeds)
 * - Lock tokens prevent accidental release by other workers
 * - Idempotency tracking prevents duplicate processing
 */
@Component
public class TaskEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskEventConsumer.class);

    @Value("${worker.id}")
    private String workerId;

    @Value("#{'${worker.supported-task-types}'.split(',')}")
    private List<String> supportedTaskTypes;

    private final WorkerEventPublisher eventPublisher;
    private final TaskLockService taskLockService;
    private final WorkerRegistry workerRegistry;

    // In-memory tracking of processing eventIds for idempotency
    // In production, use Redis with TTL
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    public TaskEventConsumer(
            WorkerEventPublisher eventPublisher,
            TaskLockService taskLockService,
            WorkerRegistry workerRegistry) {
        this.eventPublisher = eventPublisher;
        this.taskLockService = taskLockService;
        this.workerRegistry = workerRegistry;
    }

    /**
     * Handle TaskReadyEvent.
     * Executes task if worker supports the task type and can claim it atomically.
     *
     * @param event The task ready event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
     */
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

        log.info("Received TaskReadyEvent: executionId={}, taskId={}, taskType={}, eventId={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getTaskType(),
                event.getEventId(), partition, offset);

        try {
            // Idempotency check - ensure we haven't processed this event before
            if (processedEventIds.contains(event.getEventId())) {
                log.info("Event already processed (idempotent skip): eventId={}", event.getEventId());
                acknowledgment.acknowledge();
                return;
            }

            // Check if worker supports this task type
            if (!supportedTaskTypes.contains(event.getTaskType())) {
                log.debug("Task type {} not supported by this worker, skipping", event.getTaskType());
                acknowledgment.acknowledge();
                return;
            }

            // Attempt to claim the task atomically
            Optional<LockToken> lockToken = taskLockService.claimTask(event.getTaskId(), workerId);
            
            if (lockToken.isEmpty()) {
                log.info("Task already claimed by another worker: taskId={}, currentHolder={}",
                        event.getTaskId(), 
                        taskLockService.getTaskLockHolder(event.getTaskId()).orElse("unknown"));
                acknowledgment.acknowledge();
                return;
            }
            
            log.info("Task claimed successfully: taskId={}, workerId={}, token={}",
                    event.getTaskId(), workerId, lockToken.get().getToken());
            
            try {
                // Process the task with lock held
                processTask(event, lockToken.get());
            } finally {
                // Always release the lock
                boolean released = taskLockService.releaseTask(event.getTaskId(), lockToken.get());
                if (!released) {
                    log.warn("Failed to release lock (may have expired): taskId={}, token={}",
                            event.getTaskId(), lockToken.get().getToken());
                }
            }

            // Mark event as processed for idempotency
            processedEventIds.add(event.getEventId());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing TaskReadyEvent: executionId={}, taskId={}, error={}",
                    event.getExecutionId(), event.getTaskId(), e.getMessage(), e);
            
            // Don't acknowledge - message will be redelivered
            throw new RuntimeException("Failed to process TaskReadyEvent", e);
        }
    }

    /**
     * Process the task execution with lock held.
     * Transitions worker state: AVAILABLE → BUSY → AVAILABLE
     * 
     * @param event The task ready event
     * @param lockToken The lock token proving ownership
     */
    private void processTask(TaskReadyEvent event, LockToken lockToken) {
        log.info("Worker {} picked up task: executionId={}, taskId={}, taskType={}, token={}",
                workerId, event.getExecutionId(), event.getTaskId(), event.getTaskType(), lockToken.getToken());

        // Mark worker as busy
        workerRegistry.markWorkerBusy(workerId, event.getExecutionId(), event.getTaskId());

        try {
            // Publish TaskStartedEvent
            TaskStartedEvent startedEvent = TaskStartedEvent.builder()
                    .correlationId(event.getCorrelationId())
                    .workflowId(event.getWorkflowId())
                    .executionId(event.getExecutionId())
                    .taskId(event.getTaskId())
                    .workerId(workerId)
                    .attemptNumber(1) // TODO: Get from execution state
                    .build();
            eventPublisher.publishTaskStarted(startedEvent);

            // Execute the task
            long startTime = System.currentTimeMillis();
            try {
                Map<String, Object> result = executeTask(event);
                long duration = System.currentTimeMillis() - startTime;

                // Publish TaskCompletedEvent
                TaskCompletedEvent completedEvent = TaskCompletedEvent.builder()
                        .correlationId(event.getCorrelationId())
                        .workflowId(event.getWorkflowId())
                        .executionId(event.getExecutionId())
                        .taskId(event.getTaskId())
                        .workerId(workerId)
                        .attemptNumber(1)
                        .result(result)
                        .durationMs(duration)
                        .build();
                eventPublisher.publishTaskCompleted(completedEvent);

                log.info("Task completed successfully: executionId={}, taskId={}, durationMs={}",
                        event.getExecutionId(), event.getTaskId(), duration);

            } catch (Exception taskError) {
                long duration = System.currentTimeMillis() - startTime;

                // Publish TaskFailedEvent
                TaskFailedEvent failedEvent = TaskFailedEvent.builder()
                        .correlationId(event.getCorrelationId())
                        .workflowId(event.getWorkflowId())
                        .executionId(event.getExecutionId())
                        .taskId(event.getTaskId())
                        .workerId(workerId)
                        .attemptNumber(1)
                        .errorMessage(taskError.getMessage())
                        .errorType(taskError.getClass().getSimpleName())
                        .retriable(true) // TODO: Determine based on error type
                        .durationMs(duration)
                        .build();
                eventPublisher.publishTaskFailed(failedEvent);

                log.error("Task failed: executionId={}, taskId={}, error={}",
                        event.getExecutionId(), event.getTaskId(), taskError.getMessage(), taskError);
            }
        } finally {
            // Always mark worker as available after task completion
            workerRegistry.markWorkerAvailable(workerId);
        }
    }

    /**
     * Execute the actual task logic.
     * This is a placeholder - actual implementation would delegate to task handlers.
     *
     * @param event The task ready event
     * @return Task execution result
     */
    private Map<String, Object> executeTask(TaskReadyEvent event) {
        log.info("Executing task: taskType={}, taskId={}, config={}",
                event.getTaskType(), event.getTaskId(), event.getConfiguration());

        // Simulate task execution
        try {
            Thread.sleep(1000); // Simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution interrupted", e);
        }

        // Return mock result
        return Map.of(
                "status", "success",
                "taskId", event.getTaskId(),
                "processedBy", workerId
        );
    }
}
