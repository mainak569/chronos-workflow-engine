package com.chronos.worker.event;

import com.chronos.worker.config.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publisher for task and worker lifecycle events.
 * Uses executionId as message key for task events, workerId for worker events.
 *
 * Task completion and failure events are sent synchronously: if the broker does not
 * confirm them, an exception is thrown so the TaskReady record is not acknowledged
 * and will be redelivered, instead of the result being silently lost.
 */
@Component
public class WorkerEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkerEventPublisher.class);

    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public WorkerEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish TaskStartedEvent (best effort; the completion event also implies the start).
     */
    public void publishTaskStarted(TaskStartedEvent event) {
        log.info("Publishing TaskStartedEvent: executionId={}, taskId={}, workerId={}, attempt={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(),
                event.getAttemptNumber(), event.getEventId());
        sendAsync(KafkaTopics.TASK_STARTED, event.getExecutionId(), event, "TaskStartedEvent");
    }

    /**
     * Publish TaskCompletedEvent and wait for broker confirmation.
     */
    public void publishTaskCompleted(TaskCompletedEvent event) {
        log.info("Publishing TaskCompletedEvent: executionId={}, taskId={}, workerId={}, attempt={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(),
                event.getAttemptNumber(), event.getEventId());
        sendSync(KafkaTopics.TASK_COMPLETED, event.getExecutionId(), event, "TaskCompletedEvent");
    }

    /**
     * Publish TaskFailedEvent and wait for broker confirmation.
     */
    public void publishTaskFailed(TaskFailedEvent event) {
        log.info("Publishing TaskFailedEvent: executionId={}, taskId={}, workerId={}, attempt={}, retriable={}, eventId={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(),
                event.getAttemptNumber(), event.isRetriable(), event.getEventId());
        sendSync(KafkaTopics.TASK_FAILED, event.getExecutionId(), event, "TaskFailedEvent");
    }

    /**
     * Publish WorkerRegisteredEvent.
     */
    public void publishWorkerRegistered(WorkerRegisteredEvent event) {
        log.info("Publishing WorkerRegisteredEvent: workerId={}, taskTypes={}, eventId={}",
                event.getWorkerId(), event.getSupportedTaskTypes(), event.getEventId());
        sendAsync(KafkaTopics.WORKER_REGISTERED, event.getWorkerId(), event, "WorkerRegisteredEvent");
    }

    /**
     * Publish WorkerUnavailableEvent and wait for confirmation (sent during shutdown).
     */
    public void publishWorkerUnavailable(WorkerUnavailableEvent event) {
        log.info("Publishing WorkerUnavailableEvent: workerId={}, reason={}, eventId={}",
                event.getWorkerId(), event.getReason(), event.getEventId());
        sendSync(KafkaTopics.WORKER_UNAVAILABLE, event.getWorkerId(), event, "WorkerUnavailableEvent");
    }

    private void sendAsync(String topic, String key, Object event, String eventName) {
        kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
            if (ex == null) {
                logPublished(eventName, key, result);
            } else {
                log.error("Failed to publish {}: key={}, error={}", eventName, key, ex.getMessage(), ex);
            }
        });
    }

    private void sendSync(String topic, String key, Object event, String eventName) {
        try {
            SendResult<String, Object> result = kafkaTemplate.send(topic, key, event)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logPublished(eventName, key, result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + eventName, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish {}: key={}, error={}", eventName, key, e.getMessage(), e);
            throw new IllegalStateException("Failed to publish " + eventName, e);
        }
    }

    private void logPublished(String eventName, String key, SendResult<String, Object> result) {
        log.debug("Published {}: key={}, topic={}, partition={}, offset={}",
                eventName, key, result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }
}
