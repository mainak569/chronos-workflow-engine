package com.chronos.scheduler.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Transactional outbox pattern message.
 * 
 * The outbox pattern ensures atomic write to database + publish to Kafka:
 * 1. Write business data + outbox message in same MongoDB transaction
 * 2. Separate publisher polls outbox and publishes to Kafka
 * 3. Mark message as published after Kafka confirms
 * 
 * This guarantees at-least-once delivery (message may be published multiple times
 * if publisher crashes after Kafka send but before marking as published).
 * 
 * Downstream consumers must handle idempotency.
 */
@Document(collection = "outbox_messages")
public class OutboxMessage {
    
    @Id
    private String id;
    
    /**
     * Kafka topic to publish to.
     */
    @Indexed
    private String topic;
    
    /**
     * Kafka message key (for partitioning).
     */
    private String messageKey;
    
    /**
     * Message payload (serialized JSON).
     */
    private String payload;
    
    /**
     * Message type for deserialization.
     */
    private String messageType;
    
    /**
     * Publishing status.
     */
    @Indexed
    private OutboxStatus status;
    
    /**
     * When the message was created.
     */
    @Indexed
    private Instant createdAt;
    
    /**
     * When the message was published to Kafka.
     */
    private Instant publishedAt;
    
    /**
     * Number of publish attempts.
     */
    private int attemptCount;
    
    /**
     * Last error message (if publish failed).
     */
    private String lastError;
    
    /**
     * Optimistic locking version.
     */
    @Version
    private Long version;
    
    public OutboxMessage() {
        this.status = OutboxStatus.PENDING;
        this.createdAt = Instant.now();
        this.attemptCount = 0;
    }
    
    public OutboxMessage(String topic, String messageKey, String payload, String messageType) {
        this();
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.messageType = messageType;
    }
    
    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }
    
    public void markFailed(String error) {
        this.status = OutboxStatus.FAILED;
        this.lastError = error;
        this.attemptCount++;
    }
    
    public void incrementAttempt() {
        this.attemptCount++;
    }
    
    public boolean canRetry(int maxAttempts) {
        return this.attemptCount < maxAttempts;
    }
    
    // Getters and setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getMessageKey() {
        return messageKey;
    }
    
    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }
    
    public String getPayload() {
        return payload;
    }
    
    public void setPayload(String payload) {
        this.payload = payload;
    }
    
    public String getMessageType() {
        return messageType;
    }
    
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
    
    public OutboxStatus getStatus() {
        return status;
    }
    
    public void setStatus(OutboxStatus status) {
        this.status = status;
    }
    
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
    
    public Instant getPublishedAt() {
        return publishedAt;
    }
    
    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }
    
    public int getAttemptCount() {
        return attemptCount;
    }
    
    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }
    
    public String getLastError() {
        return lastError;
    }
    
    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    @Override
    public String toString() {
        return String.format("OutboxMessage{id='%s', topic='%s', messageKey='%s', " +
                        "status=%s, attemptCount=%d, createdAt=%s}",
                id, topic, messageKey, status, attemptCount, createdAt);
    }
}
