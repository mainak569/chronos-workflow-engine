package com.chronos.workflow.event;

import com.chronos.workflow.config.KafkaTopics;
import com.chronos.workflow.domain.WorkflowExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publisher for workflow lifecycle events.
 * Uses stable message keys (workflowId) for partitioning.
 */
@Component
public class WorkflowEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEventPublisher.class);

    private final KafkaTemplate<String, WorkflowCreatedEvent> kafkaTemplate;

    public WorkflowEventPublisher(KafkaTemplate<String, WorkflowCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish WorkflowCreatedEvent to Kafka.
     * Uses workflowId as message key for consistent partitioning.
     *
     * @param execution The workflow execution
     */
    public void publishWorkflowCreated(WorkflowExecution execution) {
        WorkflowCreatedEvent event = WorkflowCreatedEvent.builder()
                .correlationId(execution.getId())
                .workflowId(execution.getWorkflowId())
                .executionId(execution.getId())
                .ownerId(execution.getOwnerId())
                .workflowName(execution.getWorkflowName())
                .build();

        // Key by executionId, like all task events, so events of one execution stay ordered
        String messageKey = execution.getId();

        log.info("Publishing WorkflowCreatedEvent: workflowId={}, executionId={}, eventId={}, correlationId={}",
                event.getWorkflowId(), event.getExecutionId(), event.getEventId(), event.getCorrelationId());

        CompletableFuture<SendResult<String, WorkflowCreatedEvent>> future =
                kafkaTemplate.send(KafkaTopics.WORKFLOW_CREATED, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published WorkflowCreatedEvent: workflowId={}, executionId={}, partition={}, offset={}",
                        event.getWorkflowId(),
                        event.getExecutionId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish WorkflowCreatedEvent: workflowId={}, executionId={}, error={}",
                        event.getWorkflowId(), event.getExecutionId(), ex.getMessage(), ex);
            }
        });
    }
}
