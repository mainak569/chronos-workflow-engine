package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publisher for task lifecycle events from scheduler.
 * Uses executionId as message key for consistent partitioning.
 */
@Component
public class TaskEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(TaskEventPublisher.class);

    private final KafkaTemplate<String, TaskReadyEvent> kafkaTemplate;

    public TaskEventPublisher(KafkaTemplate<String, TaskReadyEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish TaskReadyEvent to Kafka.
     * Uses executionId as message key to ensure tasks from the same execution
     * are processed in order within their partition.
     *
     * @param event The task ready event
     */
    public void publishTaskReady(TaskReadyEvent event) {
        // Use executionId as message key for consistent partitioning
        // This ensures tasks from the same workflow execution maintain order
        String messageKey = event.getExecutionId();

        log.info("Publishing TaskReadyEvent: workflowId={}, executionId={}, taskId={}, eventId={}",
                event.getWorkflowId(), event.getExecutionId(), event.getTaskId(), event.getEventId());

        CompletableFuture<SendResult<String, TaskReadyEvent>> future =
                kafkaTemplate.send(KafkaTopics.TASK_READY, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published TaskReadyEvent: executionId={}, taskId={}, partition={}, offset={}",
                        event.getExecutionId(),
                        event.getTaskId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish TaskReadyEvent: executionId={}, taskId={}, error={}",
                        event.getExecutionId(), event.getTaskId(), ex.getMessage(), ex);
            }
        });
    }
}
