package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
import com.chronos.scheduler.service.ExecutionOrchestrationService;
import com.chronos.scheduler.service.TaskDispatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer for task status events published by workers.
 * Updates task/execution state and dispatches tasks that became ready.
 *
 * Exceptions propagate to the container error handler, which retries and finally
 * sends the record to the dead-letter topic.
 */
@Component
public class TaskStatusEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskStatusEventConsumer.class);

    private final ExecutionOrchestrationService orchestrationService;
    private final TaskDispatchService taskDispatchService;

    public TaskStatusEventConsumer(
            ExecutionOrchestrationService orchestrationService,
            TaskDispatchService taskDispatchService) {
        this.orchestrationService = orchestrationService;
        this.taskDispatchService = taskDispatchService;
    }

    /**
     * Handle TaskStartedEvent: mark the task as RUNNING.
     */
    @KafkaListener(
            topics = KafkaTopics.TASK_STARTED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "taskStartedKafkaListenerContainerFactory"
    )
    public void handleTaskStarted(
            @Payload TaskStartedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received TaskStartedEvent: executionId={}, taskId={}, workerId={}, attempt={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(),
                event.getAttemptNumber(), partition, offset);

        orchestrationService.markTaskAsRunning(
                event.getExecutionId(),
                event.getTaskId(),
                event.getWorkerId(),
                event.getAttemptNumber()
        );

        acknowledgment.acknowledge();
    }

    /**
     * Handle TaskCompletedEvent: mark the task as COMPLETED and dispatch downstream tasks.
     */
    @KafkaListener(
            topics = KafkaTopics.TASK_COMPLETED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "taskCompletedKafkaListenerContainerFactory"
    )
    public void handleTaskCompleted(
            @Payload TaskCompletedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received TaskCompletedEvent: executionId={}, taskId={}, workerId={}, attempt={}, durationMs={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(),
                event.getAttemptNumber(), event.getDurationMs(), partition, offset);

        orchestrationService.markTaskAsCompleted(
                event.getExecutionId(),
                event.getTaskId(),
                event.getWorkerId(),
                event.getAttemptNumber(),
                event.getResult()
        );

        int dispatched = taskDispatchService.dispatchReadyTasks(event.getExecutionId());

        log.info("Processed TaskCompletedEvent: executionId={}, taskId={}, newlyDispatched={}",
                event.getExecutionId(), event.getTaskId(), dispatched);

        acknowledgment.acknowledge();
    }

    /**
     * Handle TaskFailedEvent: schedule a retry or fail the task (and with it the execution).
     * Retries are dispatched by the recovery sweep once their backoff has elapsed.
     */
    @KafkaListener(
            topics = KafkaTopics.TASK_FAILED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "taskFailedKafkaListenerContainerFactory"
    )
    public void handleTaskFailed(
            @Payload TaskFailedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received TaskFailedEvent: executionId={}, taskId={}, workerId={}, attempt={}, retriable={}, error={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), event.getAttemptNumber(),
                event.isRetriable(), event.getErrorMessage(), partition, offset);

        orchestrationService.markTaskAsFailed(
                event.getExecutionId(),
                event.getTaskId(),
                event.getWorkerId(),
                event.getAttemptNumber(),
                event.getErrorMessage(),
                event.getErrorType(),
                event.isRetriable()
        );

        acknowledgment.acknowledge();
    }
}
