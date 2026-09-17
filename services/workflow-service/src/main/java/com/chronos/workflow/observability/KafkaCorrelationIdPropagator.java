package com.chronos.workflow.observability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Utility for propagating correlation IDs through Kafka messages.
 * 
 * Producer: Adds correlation ID from MDC to message headers
 * Consumer: Extracts correlation ID from message headers to MDC
 */
@Component
public class KafkaCorrelationIdPropagator {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaCorrelationIdPropagator.class);
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String MDC_KEY = "correlation_id";
    
    /**
     * Add correlation ID from MDC to Kafka message headers.
     *
     * @param record Producer record
     * @param <K> Key type
     * @param <V> Value type
     */
    public <K, V> void addCorrelationIdToRecord(ProducerRecord<K, V> record) {
        String correlationId = MDC.get(MDC_KEY);
        
        if (correlationId != null && !correlationId.isEmpty()) {
            record.headers().add(CORRELATION_ID_HEADER, 
                    correlationId.getBytes(StandardCharsets.UTF_8));
            log.trace("Added correlation ID to Kafka record: {}", correlationId);
        } else {
            log.warn("No correlation ID in MDC when producing Kafka message");
        }
    }
    
    /**
     * Extract correlation ID from Kafka message headers and store in MDC.
     *
     * @param record Consumer record
     * @param <K> Key type
     * @param <V> Value type
     * @return Extracted correlation ID, or null if not present
     */
    public <K, V> String extractCorrelationIdFromRecord(ConsumerRecord<K, V> record) {
        Header correlationHeader = record.headers().lastHeader(CORRELATION_ID_HEADER);
        
        if (correlationHeader != null) {
            String correlationId = new String(correlationHeader.value(), StandardCharsets.UTF_8);
            MDC.put(MDC_KEY, correlationId);
            log.trace("Extracted correlation ID from Kafka record: {}", correlationId);
            return correlationId;
        } else {
            log.warn("No correlation ID header in Kafka message from topic: {}", record.topic());
            return null;
        }
    }
    
    /**
     * Clear correlation ID from MDC.
     * Call this in finally block after processing Kafka message.
     */
    public void clearCorrelationId() {
        MDC.remove(MDC_KEY);
    }
}
