package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
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
 * Consumer for workflow lifecycle events.
 * Handles WorkflowCreatedEvent by dispatching the execution's initial runnable tasks.
 */
@Component
public class WorkflowEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEventConsumer.class);

    private final TaskDispatchService taskDispatchService;

    public WorkflowEventConsumer(TaskDispatchService taskDispatchService) {
        this.taskDispatchService = taskDispatchService;
    }

    /**
     * Handle WorkflowCreatedEvent.
     * The workflow service has already stored the execution and its task executions;
     * tasks without dependencies are dispatched here. Redelivery is harmless because
     * each task attempt can only be dispatched once.
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

        int dispatched = taskDispatchService.dispatchReadyTasks(event.getExecutionId());

        log.info("Processed WorkflowCreatedEvent: workflowId={}, executionId={}, dispatchedTasks={}",
                event.getWorkflowId(), event.getExecutionId(), dispatched);

        acknowledgment.acknowledge();
    }
}
