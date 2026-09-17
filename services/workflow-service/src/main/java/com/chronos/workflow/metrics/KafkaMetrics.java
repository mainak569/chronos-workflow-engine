package com.chronos.workflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Metrics for Kafka message processing.
 * Tracks messages processed, processing duration, and errors.
 */
@Component
public class KafkaMetrics {
    
    private final MeterRegistry registry;
    
    public KafkaMetrics(MeterRegistry registry) {
        this.registry = registry;
    }
    
    /**
     * Record Kafka message processed.
     *
     * @param topic Kafka topic
     * @param consumerGroup Consumer group ID
     */
    public void recordMessageProcessed(String topic, String consumerGroup) {
        Counter.builder("chronos.kafka.messages.processed")
                .description("Total Kafka messages processed")
                .tag("topic", topic)
                .tag("consumer_group", consumerGroup)
                .register(registry)
                .increment();
    }
    
    /**
     * Record Kafka message processing duration.
     *
     * @param topic Kafka topic
     * @param startTime Processing start time
     */
    public void recordMessageProcessingDuration(String topic, Instant startTime) {
        Duration duration = Duration.between(startTime, Instant.now());
        
        Timer.builder("chronos.kafka.message.processing.duration")
                .description("Kafka message processing duration")
                .tag("topic", topic)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry)
                .record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record Kafka processing error.
     *
     * @param topic Kafka topic
     * @param errorType Type of error
     */
    public void recordKafkaError(String topic, String errorType) {
        Counter.builder("chronos.kafka.errors")
                .description("Kafka processing errors")
                .tag("topic", topic)
                .tag("error_type", errorType)
                .register(registry)
                .increment();
    }
    
    /**
     * Record message sent to Kafka.
     *
     * @param topic Kafka topic
     */
    public void recordMessageSent(String topic) {
        Counter.builder("chronos.kafka.messages.sent")
                .description("Total messages sent to Kafka")
                .tag("topic", topic)
                .register(registry)
                .increment();
    }
    
    /**
     * Record Kafka send failure.
     *
     * @param topic Kafka topic
     * @param errorType Type of error
     */
    public void recordSendFailure(String topic, String errorType) {
        Counter.builder("chronos.kafka.send.failures")
                .description("Kafka send failures")
                .tag("topic", topic)
                .tag("error_type", errorType)
                .register(registry)
                .increment();
    }
}
