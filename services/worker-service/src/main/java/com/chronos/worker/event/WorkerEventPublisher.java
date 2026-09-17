package com.chronos.worker.event;

import com.chronos.worker.config.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publisher for task and worker lifecycle events.
 * Uses executionId as message key for task events, workerId for worker events.
 */
@Component
public class WorkerEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkerEventPublisher.class);

    private final KafkaTemplate<String, TaskStartedEvent> taskStartedTemplate;
    private final KafkaTemplate<String, TaskCompletedEvent> taskCompletedTemplate;
    private final KafkaTemplate<String, TaskFailedEvent> taskFailedTemplate;
    private final KafkaTemplate<String, WorkerRegisteredEvent> workerRegisteredTemplate;
    private final KafkaTemplate<String, WorkerUnavailableEvent> workerUnavailableTemplate;

    public WorkerEventPublisher(
            KafkaTemplate<String, TaskStartedEvent> taskStartedTemplate,
            KafkaTemplate<String, TaskCompletedEvent> taskCompletedTemplate,
            KafkaTemplate<String, TaskFailedEvent> taskFailedTemplate,
            KafkaTemplate<String, WorkerRegisteredEvent> workerRegisteredTemplate,
            KafkaTemplate<String, WorkerUnavailableEvent> workerUnavailableTemplate) {
        this.taskStartedTemplate = taskStartedTemplate;
        this.taskCompletedTemplate = taskCompletedTemplate;
        this.taskFailedTemplate = taskFailedTemplate;
        this.workerRegisteredTemplate = workerRegisteredTemplate;
        this.workerUnavailableTemplate = workerUnavailableTemplate;
    }

    /**
     * Publish TaskStartedEvent to Kafka.
     * Uses executionId as message key for ordering.
     *
     * @param event The task started event
     */
    public void publishTaskStarted(TaskStartedEvent event) {
        String messageKey = event.getExecutionId();

        log.info("Publishing TaskStartedEvent: executionId={}, taskId={}, workerId={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), event.getEventId());

        CompletableFuture<SendResult<String, TaskStartedEvent>> future =
                taskStartedTemplate.send(KafkaTopics.TASK_STARTED, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published TaskStartedEvent: executionId={}, taskId={}, partition={}, offset={}",
                        event.getExecutionId(), event.getTaskId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish TaskStartedEvent: executionId={}, taskId={}, error={}",
                        event.getExecutionId(), event.getTaskId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publish TaskCompletedEvent to Kafka.
     * Uses executionId as message key for ordering.
     *
     * @param event The task completed event
     */
    public void publishTaskCompleted(TaskCompletedEvent event) {
        String messageKey = event.getExecutionId();

        log.info("Publishing TaskCompletedEvent: executionId={}, taskId={}, workerId={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), event.getEventId());

        CompletableFuture<SendResult<String, TaskCompletedEvent>> future =
                taskCompletedTemplate.send(KafkaTopics.TASK_COMPLETED, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published TaskCompletedEvent: executionId={}, taskId={}, partition={}, offset={}",
                        event.getExecutionId(), event.getTaskId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish TaskCompletedEvent: executionId={}, taskId={}, error={}",
                        event.getExecutionId(), event.getTaskId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publish TaskFailedEvent to Kafka.
     * Uses executionId as message key for ordering.
     *
     * @param event The task failed event
     */
    public void publishTaskFailed(TaskFailedEvent event) {
        String messageKey = event.getExecutionId();

        log.info("Publishing TaskFailedEvent: executionId={}, taskId={}, workerId={}, retriable={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), 
                event.isRetriable(), event.getEventId());

        CompletableFuture<SendResult<String, TaskFailedEvent>> future =
                taskFailedTemplate.send(KafkaTopics.TASK_FAILED, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published TaskFailedEvent: executionId={}, taskId={}, partition={}, offset={}",
                        event.getExecutionId(), event.getTaskId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish TaskFailedEvent: executionId={}, taskId={}, error={}",
                        event.getExecutionId(), event.getTaskId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publish WorkerRegisteredEvent to Kafka.
     * Uses workerId as message key for consistent worker tracking.
     *
     * @param event The worker registered event
     */
    public void publishWorkerRegistered(WorkerRegisteredEvent event) {
        String messageKey = event.getWorkerId();

        log.info("Publishing WorkerRegisteredEvent: workerId={}, taskTypes={}, eventId={}",
                event.getWorkerId(), event.getSupportedTaskTypes(), event.getEventId());

        CompletableFuture<SendResult<String, WorkerRegisteredEvent>> future =
                workerRegisteredTemplate.send(KafkaTopics.WORKER_REGISTERED, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published WorkerRegisteredEvent: workerId={}, partition={}, offset={}",
                        event.getWorkerId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish WorkerRegisteredEvent: workerId={}, error={}",
                        event.getWorkerId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publish WorkerUnavailableEvent to Kafka.
     * Uses workerId as message key for consistent worker tracking.
     *
     * @param event The worker unavailable event
     */
    public void publishWorkerUnavailable(WorkerUnavailableEvent event) {
        String messageKey = event.getWorkerId();

        log.info("Publishing WorkerUnavailableEvent: workerId={}, reason={}, eventId={}",
                event.getWorkerId(), event.getReason(), event.getEventId());

        CompletableFuture<SendResult<String, WorkerUnavailableEvent>> future =
                workerUnavailableTemplate.send(KafkaTopics.WORKER_UNAVAILABLE, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published WorkerUnavailableEvent: workerId={}, partition={}, offset={}",
                        event.getWorkerId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish WorkerUnavailableEvent: workerId={}, error={}",
                        event.getWorkerId(), ex.getMessage(), ex);
            }
        });
    }
}
