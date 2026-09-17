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
 * Consumer for workflow lifecycle events.
 * Handles WorkflowCreatedEvent to initiate workflow execution planning.
 */
@Component
public class WorkflowEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEventConsumer.class);

    private final TaskEventPublisher taskEventPublisher;
    private final ExecutionOrchestrationService orchestrationService;

    public WorkflowEventConsumer(
            TaskEventPublisher taskEventPublisher,
            ExecutionOrchestrationService orchestrationService) {
        this.taskEventPublisher = taskEventPublisher;
        this.orchestrationService = orchestrationService;
    }

    /**
     * Handle WorkflowCreatedEvent.
     * Loads execution state from MongoDB and publishes TaskReadyEvents for tasks with satisfied dependencies.
     *
     * @param event The workflow created event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
     */
    @KafkaListener(
            topics = KafkaTopics.WORKFLOW_CREATED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "workflowCreatedKafkaListenerContainerFactory"
    )
    public void handleWorkflowCreated(
            @Payload WorkflowCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received WorkflowCreatedEvent: workflowId={}, executionId={}, eventId={}, partition={}, offset={}",
                event.getWorkflowId(), event.getExecutionId(), event.getEventId(), partition, offset);

        try {
            // Idempotency check: verify we haven't processed this event before
            // In production, check Redis/MongoDB for processed eventId
            // For now, we'll process optimistically
            
            String executionId = event.getExecutionId();
            
            // Get ready tasks from execution state (dependency resolution)
            List<TaskExecution> readyTasks = orchestrationService.getReadyTasks(executionId);
            
            log.info("Found {} ready tasks for executionId={}", readyTasks.size(), executionId);
            
            // Publish TaskReadyEvent for each ready task
            for (TaskExecution task : readyTasks) {
                TaskReadyEvent taskReadyEvent = TaskReadyEvent.builder()
                        .correlationId(event.getCorrelationId())
                        .workflowId(event.getWorkflowId())
                        .executionId(executionId)
                        .taskId(task.getTaskId())
                        .taskType(task.getTaskType())
                        .configuration(task.getConfiguration())
                        .build();

                log.info("Publishing TaskReadyEvent: executionId={}, taskId={}", 
                        executionId, task.getTaskId());
                taskEventPublisher.publishTaskReady(taskReadyEvent);
            }

            log.info("Successfully processed WorkflowCreatedEvent: workflowId={}, executionId={}, readyTasks={}",
                    event.getWorkflowId(), executionId, readyTasks.size());

            // Manual acknowledgment for at-least-once delivery
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing WorkflowCreatedEvent: workflowId={}, executionId={}, eventId={}, error={}",
                    event.getWorkflowId(), event.getExecutionId(), event.getEventId(), e.getMessage(), e);
            
            // Don't acknowledge - message will be redelivered
            // After max retries, it will go to DLT if configured
            throw new RuntimeException("Failed to process WorkflowCreatedEvent", e);
        }
    }
}
