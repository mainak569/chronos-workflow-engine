package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.service.ExecutionOrchestrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumer for task status events from workers.
 * Handles TaskCompletedEvent and TaskFailedEvent to update workflow execution state.
 */
@Component
public class TaskStatusEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskStatusEventConsumer.class);

    private final ExecutionOrchestrationService orchestrationService;
    private final TaskEventPublisher taskEventPublisher;

    public TaskStatusEventConsumer(
            ExecutionOrchestrationService orchestrationService,
            TaskEventPublisher taskEventPublisher) {
        this.orchestrationService = orchestrationService;
        this.taskEventPublisher = taskEventPublisher;
    }

    /**
     * Handle TaskCompletedEvent.
     * Updates task status and schedules dependent tasks if ready.
     *
     * @param event The task completed event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
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

        log.info("Received TaskCompletedEvent: executionId={}, taskId={}, workerId={}, eventId={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), 
                event.getEventId(), partition, offset);

        try {
            // Idempotency check: verify we haven't processed this event before
            // In production, check Redis/MongoDB for processed eventId
            // For now, we'll process optimistically
            
            // Update task execution state to COMPLETED
            orchestrationService.markTaskAsCompleted(
                    event.getExecutionId(),
                    event.getTaskId(),
                    event.getResult()
            );
            
            log.info("Marked task as completed: executionId={}, taskId={}, durationMs={}",
                    event.getExecutionId(), event.getTaskId(), event.getDurationMs());
            
            // Check if dependent tasks are now ready to execute
            List<TaskExecution> readyTasks = orchestrationService.getReadyTasks(event.getExecutionId());
            
            log.info("Found {} newly ready tasks after completion: executionId={}, taskId={}", 
                    readyTasks.size(), event.getExecutionId(), event.getTaskId());
            
            // Publish TaskReadyEvent for each newly ready task
            for (TaskExecution task : readyTasks) {
                TaskReadyEvent taskReadyEvent = TaskReadyEvent.builder()
                        .correlationId(event.getCorrelationId())
                        .workflowId(event.getWorkflowId())
                        .executionId(event.getExecutionId())
                        .taskId(task.getTaskId())
                        .taskType(task.getTaskType())
                        .configuration(task.getConfiguration())
                        .build();

                log.info("Publishing TaskReadyEvent for downstream task: executionId={}, taskId={}", 
                        event.getExecutionId(), task.getTaskId());
                taskEventPublisher.publishTaskReady(taskReadyEvent);
            }
            
            log.info("Successfully processed TaskCompletedEvent: executionId={}, taskId={}, newReadyTasks={}",
                    event.getExecutionId(), event.getTaskId(), readyTasks.size());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing TaskCompletedEvent: executionId={}, taskId={}, error={}",
                    event.getExecutionId(), event.getTaskId(), e.getMessage(), e);
            throw new RuntimeException("Failed to process TaskCompletedEvent", e);
        }
    }

    /**
     * Handle TaskFailedEvent.
     * Updates task status and determines retry or failure propagation.
     *
     * @param event The task failed event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
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

        log.info("Received TaskFailedEvent: executionId={}, taskId={}, workerId={}, retriable={}, eventId={}, partition={}, offset={}",
                event.getExecutionId(), event.getTaskId(), event.getWorkerId(), 
                event.isRetriable(), event.getEventId(), partition, offset);

        try {
            // Idempotency check: verify we haven't processed this event before
            // In production, check Redis/MongoDB for processed eventId
            // For now, we'll process optimistically
            
            // Update task execution state to FAILED
            orchestrationService.markTaskAsFailed(
                    event.getExecutionId(),
                    event.getTaskId(),
                    event.getErrorMessage(),
                    event.getErrorType(),
                    event.isRetriable()
            );
            
            log.info("Marked task as failed: executionId={}, taskId={}, retriable={}, attempt={}",
                    event.getExecutionId(), event.getTaskId(), event.isRetriable(), event.getAttemptNumber());
            
            // Determine retry logic
            // Note: The task state is already marked as FAILED
            // Retry logic would require resetting state to PENDING or implementing a separate retry mechanism
            // For now, we just log and let the workflow fail if task is not retriable
            
            if (!event.isRetriable()) {
                log.warn("Task failed permanently (not retriable): executionId={}, taskId={}, attempts={}",
                        event.getExecutionId(), event.getTaskId(), event.getAttemptNumber());
                // Workflow execution status will be updated by updateExecutionStatus() in orchestrationService
            } else {
                log.info("Task failed but is retriable: executionId={}, taskId={}, attempts={}",
                        event.getExecutionId(), event.getTaskId(), event.getAttemptNumber());
                // Future: Implement retry mechanism with exponential backoff
            }

            log.info("Successfully processed TaskFailedEvent: executionId={}, taskId={}",
                    event.getExecutionId(), event.getTaskId());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing TaskFailedEvent: executionId={}, taskId={}, error={}",
                    event.getExecutionId(), event.getTaskId(), e.getMessage(), e);
            throw new RuntimeException("Failed to process TaskFailedEvent", e);
        }
    }
}
