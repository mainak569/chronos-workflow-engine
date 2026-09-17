package com.chronos.scheduler.outbox;

/**
 * Status of an outbox message.
 */
public enum OutboxStatus {
    /**
     * Message created but not yet published.
     */
    PENDING,
    
    /**
     * Message successfully published to Kafka.
     */
    PUBLISHED,
    
    /**
     * Message publishing failed after max retries.
     */
    FAILED
}
