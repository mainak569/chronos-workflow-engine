package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer for worker status events.
 * Maintains worker registry for task assignment decisions.
 */
@Component
public class WorkerStatusEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkerStatusEventConsumer.class);

    /**
     * Handle WorkerRegisteredEvent.
     * Adds worker to available worker pool.
     *
     * @param event The worker registered event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
     */
    @KafkaListener(
            topics = KafkaTopics.WORKER_REGISTERED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "workerRegisteredKafkaListenerContainerFactory"
    )
    public void handleWorkerRegistered(
            @Payload WorkerRegisteredEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received WorkerRegisteredEvent: workerId={}, taskTypes={}, eventId={}, partition={}, offset={}",
                event.getWorkerId(), event.getSupportedTaskTypes(), 
                event.getEventId(), partition, offset);

        try {
            // Idempotency check
            
            // Register worker in Redis with supported task types
            // TODO: Store in Redis with TTL matching heartbeat timeout
            // Key: "worker:{workerId}", Value: {taskTypes, hostname, port, lastSeen}
            
            log.info("Successfully registered worker: workerId={}, taskTypes={}",
                    event.getWorkerId(), event.getSupportedTaskTypes());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing WorkerRegisteredEvent: workerId={}, error={}",
                    event.getWorkerId(), e.getMessage(), e);
            throw new RuntimeException("Failed to process WorkerRegisteredEvent", e);
        }
    }

    /**
     * Handle WorkerUnavailableEvent.
     * Removes worker from available pool and handles in-progress tasks.
     *
     * @param event The worker unavailable event
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param acknowledgment Manual acknowledgment handle
     */
    @KafkaListener(
            topics = KafkaTopics.WORKER_UNAVAILABLE,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "workerUnavailableKafkaListenerContainerFactory"
    )
    public void handleWorkerUnavailable(
            @Payload WorkerUnavailableEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received WorkerUnavailableEvent: workerId={}, reason={}, eventId={}, partition={}, offset={}",
                event.getWorkerId(), event.getReason(), 
                event.getEventId(), partition, offset);

        try {
            // Idempotency check
            
            // Remove worker from Redis
            // TODO: Delete "worker:{workerId}" key
            
            // Find tasks assigned to this worker and reschedule them
            // TODO: Query execution state for in-progress tasks by workerId
            // TODO: Publish TaskReadyEvents to reschedule those tasks
            
            log.info("Successfully processed worker unavailability: workerId={}, reason={}",
                    event.getWorkerId(), event.getReason());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error processing WorkerUnavailableEvent: workerId={}, error={}",
                    event.getWorkerId(), e.getMessage(), e);
            throw new RuntimeException("Failed to process WorkerUnavailableEvent", e);
        }
    }
}
