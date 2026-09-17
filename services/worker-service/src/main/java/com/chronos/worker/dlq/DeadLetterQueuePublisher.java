package com.chronos.worker.dlq;

import com.chronos.worker.config.KafkaTopics;
import com.chronos.worker.event.TaskFailedPermanentlyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service for publishing permanently failed tasks to the Dead Letter Queue (DLQ).
 * 
 * Tasks are moved to the DLQ when:
 * - Maximum retry attempts have been exhausted
 * - Task fails with a non-retriable error
 * - Task enters a permanent failure state
 * 
 * The DLQ allows:
 * - Manual inspection and intervention
 * - Alerting and monitoring of critical failures
 * - Analysis of failure patterns
 * - Reprocessing after fixes are applied
 * 
 * DLQ events contain:
 * - Task execution metadata
 * - All retry attempts and their errors
 * - Worker and timing information
 * - Configuration for reproducing the failure
 */
@Service
public class DeadLetterQueuePublisher {
    
    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueuePublisher.class);
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    public DeadLetterQueuePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Publish a permanently failed task to the DLQ.
     * 
     * @param event the permanently failed task event
     */
    public void publishToDeadLetterQueue(TaskFailedPermanentlyEvent event) {
        log.warn("Publishing task to Dead Letter Queue: taskId={}, executionId={}, attemptNumber={}, reason={}",
                event.getTaskId(), event.getExecutionId(), event.getTotalAttempts(), event.getLastErrorMessage());
        
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                    KafkaTopics.TASK_FAILED_PERMANENTLY,
                    event.getTaskId(),
                    event
            );
            
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish to DLQ: taskId={}, executionId={}",
                            event.getTaskId(), event.getExecutionId(), ex);
                } else {
                    log.info("Successfully published to DLQ: taskId={}, executionId={}, partition={}, offset={}",
                            event.getTaskId(),
                            event.getExecutionId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
            
        } catch (Exception e) {
            log.error("Exception while publishing to DLQ: taskId={}, executionId={}",
                    event.getTaskId(), event.getExecutionId(), e);
        }
    }
    
    /**
     * Publish a permanently failed task to the DLQ synchronously.
     * Blocks until the message is sent or fails.
     * 
     * Use this when you need to ensure the DLQ message is persisted before continuing.
     * 
     * @param event the permanently failed task event
     * @return true if published successfully, false otherwise
     */
    public boolean publishToDeadLetterQueueSync(TaskFailedPermanentlyEvent event) {
        log.warn("Publishing task to Dead Letter Queue (sync): taskId={}, executionId={}, attemptNumber={}, reason={}",
                event.getTaskId(), event.getExecutionId(), event.getTotalAttempts(), event.getLastErrorMessage());
        
        try {
            SendResult<String, Object> result = kafkaTemplate.send(
                    KafkaTopics.TASK_FAILED_PERMANENTLY,
                    event.getTaskId(),
                    event
            ).get(); // Blocking call
            
            log.info("Successfully published to DLQ (sync): taskId={}, executionId={}, partition={}, offset={}",
                    event.getTaskId(),
                    event.getExecutionId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            
            return true;
            
        } catch (Exception e) {
            log.error("Failed to publish to DLQ (sync): taskId={}, executionId={}",
                    event.getTaskId(), event.getExecutionId(), e);
            return false;
        }
    }
}
