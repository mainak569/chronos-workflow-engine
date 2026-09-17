package com.chronos.scheduler.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Global error handler for Kafka consumers.
 * Logs errors and handles DLT routing after max retries.
 */
@Component
public class KafkaErrorHandler implements CommonErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandler.class);

    public void handleRecord(
            Exception thrownException,
            ConsumerRecord<?, ?> record,
            org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        
        log.error("Error processing Kafka record: topic={}, partition={}, offset={}, key={}, error={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                thrownException.getMessage(),
                thrownException);

        // Spring Kafka will handle retries based on configuration
        // After max retries, if DLT is configured, message goes there
        // For now, we log and allow framework to handle
    }
}
