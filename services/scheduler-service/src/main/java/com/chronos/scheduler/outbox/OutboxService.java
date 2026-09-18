package com.chronos.scheduler.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Service for managing outbox messages.
 * 
 * Stores messages durably in MongoDB before they are sent to Kafka, so a message
 * survives Kafka outages and scheduler restarts. The actual publishing is done by
 * OutboxPublisherService (at-least-once; consumers must be idempotent).
 */
@Service
public class OutboxService {
    
    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);
    
    private final OutboxMessageRepository outboxMessageRepository;
    private final ObjectMapper objectMapper;
    
    public OutboxService(OutboxMessageRepository outboxMessageRepository, ObjectMapper objectMapper) {
        this.outboxMessageRepository = outboxMessageRepository;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Create an outbox message for later publishing.
     * 
     * @param topic Kafka topic
     * @param messageKey Kafka message key
     * @param payload message payload object (will be serialized to JSON)
     * @param messageType message type for deserialization
     * @return the created outbox message
     */
    public OutboxMessage createMessage(String topic, String messageKey, Object payload, String messageType) {
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            
            OutboxMessage message = new OutboxMessage(topic, messageKey, payloadJson, messageType);
            message.setId(UUID.randomUUID().toString());
            
            OutboxMessage saved = outboxMessageRepository.save(message);
            
            log.debug("Created outbox message: id={}, topic={}, messageKey={}, messageType={}",
                    saved.getId(), topic, messageKey, messageType);
            
            return saved;
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize outbox message payload: topic={}, messageKey={}", 
                    topic, messageKey, e);
            throw new RuntimeException("Failed to create outbox message", e);
        }
    }
    
    /**
     * Get statistics about outbox messages.
     * 
     * @return outbox statistics
     */
    public OutboxStatistics getStatistics() {
        long pending = outboxMessageRepository.countByStatus(OutboxStatus.PENDING);
        long published = outboxMessageRepository.countByStatus(OutboxStatus.PUBLISHED);
        long failed = outboxMessageRepository.countByStatus(OutboxStatus.FAILED);
        
        return new OutboxStatistics(pending, published, failed);
    }
    
    /**
     * Statistics about outbox messages.
     */
    public static class OutboxStatistics {
        private final long pendingCount;
        private final long publishedCount;
        private final long failedCount;
        
        public OutboxStatistics(long pendingCount, long publishedCount, long failedCount) {
            this.pendingCount = pendingCount;
            this.publishedCount = publishedCount;
            this.failedCount = failedCount;
        }
        
        public long getPendingCount() {
            return pendingCount;
        }
        
        public long getPublishedCount() {
            return publishedCount;
        }
        
        public long getFailedCount() {
            return failedCount;
        }
        
        @Override
        public String toString() {
            return String.format("OutboxStatistics{pending=%d, published=%d, failed=%d}",
                    pendingCount, publishedCount, failedCount);
        }
    }
}
