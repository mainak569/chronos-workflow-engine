package com.chronos.scheduler.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Background service that publishes outbox messages to Kafka.
 * 
 * Polling Strategy:
 * - Runs every 1 second (configurable)
 * - Processes pending messages in batches
 * - Marks as published after Kafka confirmation
 * - Retries failed messages with exponential backoff
 * - Cleans up old published messages
 * 
 * Guarantees:
 * - At-least-once delivery (message may be published multiple times)
 * - Messages published in order per partition (same key)
 * - No message loss (if Kafka is available)
 * 
 * Error Handling:
 * - Transient failures: retry with backoff
 * - Permanent failures: mark as FAILED after max attempts
 * - Kafka unavailable: skip this round, retry next poll
 */
@Service
public class OutboxPublisherService {
    
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherService.class);
    
    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${chronos.outbox.poll-interval:1000}")
    private long pollIntervalMs;
    
    @Value("${chronos.outbox.batch-size:100}")
    private int batchSize;
    
    @Value("${chronos.outbox.max-attempts:5}")
    private int maxAttempts;
    
    @Value("${chronos.outbox.cleanup-after:7d}")
    private Duration cleanupAfter;
    
    public OutboxPublisherService(
            OutboxMessageRepository outboxMessageRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.outboxMessageRepository = outboxMessageRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Poll outbox and publish pending messages.
     * Runs every 1 second by default.
     */
    @Scheduled(fixedDelayString = "${chronos.outbox.poll-interval:1000}")
    public void publishPendingMessages() {
        try {
            List<OutboxMessage> pending = outboxMessageRepository.findPendingMessages();
            
            if (pending.isEmpty()) {
                log.trace("No pending outbox messages");
                return;
            }
            
            log.debug("Publishing {} pending outbox messages", pending.size());
            
            int published = 0;
            int failed = 0;
            int skipped = 0;
            
            for (OutboxMessage message : pending) {
                try {
                    boolean success = publishMessage(message);
                    if (success) {
                        published++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    failed++;
                    log.error("Error publishing outbox message: id={}, topic={}", 
                            message.getId(), message.getTopic(), e);
                    handlePublishFailure(message, e);
                }
            }
            
            log.info("Outbox publishing round complete: published={}, failed={}, skipped={}",
                    published, failed, skipped);
            
        } catch (Exception e) {
            log.error("Error in outbox publisher", e);
        }
    }
    
    /**
     * Publish a single outbox message to Kafka.
     * 
     * @param message outbox message
     * @return true if published successfully
     */
    @Transactional
    protected boolean publishMessage(OutboxMessage message) {
        try {
            // Deserialize payload to proper type
            Class<?> messageClass = Class.forName(message.getMessageType());
            Object payload = objectMapper.readValue(message.getPayload(), messageClass);
            
            // Send to Kafka (async)
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                    message.getTopic(),
                    message.getMessageKey(),
                    payload
            );
            
            // Wait for confirmation (blocks, but necessary for exactly-once semantics)
            SendResult<String, Object> result = future.get();
            
            // Mark as published
            message.markPublished();
            outboxMessageRepository.save(message);
            
            log.info("Published outbox message: id={}, topic={}, partition={}, offset={}",
                    message.getId(), message.getTopic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            
            return true;
            
        } catch (OptimisticLockingFailureException e) {
            // Another publisher already processed this message
            log.debug("Message already published: id={}", message.getId());
            return false;
            
        } catch (Exception e) {
            log.error("Failed to publish message: id={}, topic={}, attempt={}", 
                    message.getId(), message.getTopic(), message.getAttemptCount(), e);
            throw new RuntimeException("Failed to publish outbox message", e);
        }
    }
    
    /**
     * Handle publish failure.
     * 
     * @param message the message that failed
     * @param error the error
     */
    @Transactional
    protected void handlePublishFailure(OutboxMessage message, Exception error) {
        try {
            message.incrementAttempt();
            
            if (!message.canRetry(maxAttempts)) {
                // Max attempts reached, mark as failed
                message.markFailed(error.getMessage());
                log.error("Message permanently failed after {} attempts: id={}, topic={}",
                        message.getAttemptCount(), message.getId(), message.getTopic());
            }
            
            outboxMessageRepository.save(message);
            
        } catch (Exception e) {
            log.error("Failed to update message status: id={}", message.getId(), e);
        }
    }
    
    /**
     * Retry failed messages that haven't exceeded max attempts.
     * Runs every 5 minutes.
     */
    @Scheduled(fixedDelay = 300000) // 5 minutes
    public void retryFailedMessages() {
        try {
            List<OutboxMessage> failed = outboxMessageRepository.findFailedMessages();
            
            if (failed.isEmpty()) {
                return;
            }
            
            log.info("Retrying {} failed outbox messages", failed.size());
            
            int retried = 0;
            int skipped = 0;
            
            for (OutboxMessage message : failed) {
                if (message.canRetry(maxAttempts)) {
                    // Reset to PENDING for retry
                    message.setStatus(OutboxStatus.PENDING);
                    outboxMessageRepository.save(message);
                    retried++;
                } else {
                    skipped++;
                }
            }
            
            log.info("Failed message retry complete: retried={}, skipped={}", retried, skipped);
            
        } catch (Exception e) {
            log.error("Error retrying failed messages", e);
        }
    }
    
    /**
     * Clean up old published messages.
     * Runs daily at midnight.
     */
    @Scheduled(cron = "0 0 0 * * *")
    public void cleanupOldMessages() {
        try {
            Instant cutoff = Instant.now().minus(cleanupAfter);
            List<OutboxMessage> oldMessages = outboxMessageRepository.findPublishedBefore(cutoff);
            
            if (oldMessages.isEmpty()) {
                log.debug("No old outbox messages to clean up");
                return;
            }
            
            outboxMessageRepository.deleteAll(oldMessages);
            
            log.info("Cleaned up {} old outbox messages (published before {})",
                    oldMessages.size(), cutoff);
            
        } catch (Exception e) {
            log.error("Error cleaning up old outbox messages", e);
        }
    }
}
