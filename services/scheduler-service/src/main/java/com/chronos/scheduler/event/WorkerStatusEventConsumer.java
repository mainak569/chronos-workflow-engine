package com.chronos.scheduler.event;

import com.chronos.scheduler.config.KafkaTopics;
import com.chronos.scheduler.service.WorkerFailureHandler;
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
 * Workers maintain their own registry entries in Redis; the scheduler reacts to
 * workers leaving by requeueing the tasks they were running.
 */
@Component
public class WorkerStatusEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkerStatusEventConsumer.class);

    private final WorkerFailureHandler workerFailureHandler;

    public WorkerStatusEventConsumer(WorkerFailureHandler workerFailureHandler) {
        this.workerFailureHandler = workerFailureHandler;
    }

    /**
     * Handle WorkerRegisteredEvent (informational).
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

        log.info("Worker registered: workerId={}, taskTypes={}, eventId={}, partition={}, offset={}",
                event.getWorkerId(), event.getSupportedTaskTypes(),
                event.getEventId(), partition, offset);

        acknowledgment.acknowledge();
    }

    /**
     * Handle WorkerUnavailableEvent: requeue tasks still RUNNING on that worker and dispatch them again.
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

        int requeued = workerFailureHandler.handleWorkerLost(event.getWorkerId());

        log.info("Processed worker unavailability: workerId={}, requeuedTasks={}",
                event.getWorkerId(), requeued);

        acknowledgment.acknowledge();
    }
}
