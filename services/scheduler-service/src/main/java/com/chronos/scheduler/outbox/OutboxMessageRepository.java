package com.chronos.scheduler.outbox;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * Repository for outbox messages.
 */
@Repository
public interface OutboxMessageRepository extends MongoRepository<OutboxMessage, String> {
    
    /**
     * Find pending messages ordered by creation time.
     * 
     * @param limit maximum number of messages to return
     * @return list of pending messages
     */
    @Query("{ 'status': 'PENDING' }")
    List<OutboxMessage> findPendingMessages();
    
    /**
     * Find failed messages that can be retried.
     * 
     * @return list of failed messages
     */
    @Query("{ 'status': 'FAILED' }")
    List<OutboxMessage> findFailedMessages();
    
    /**
     * Find published messages older than the given time.
     * Used for cleanup.
     * 
     * @param before timestamp to compare against
     * @return list of old published messages
     */
    @Query("{ 'status': 'PUBLISHED', 'publishedAt': { $lt: ?0 } }")
    List<OutboxMessage> findPublishedBefore(Instant before);
    
    /**
     * Count pending messages.
     * 
     * @return number of pending messages
     */
    long countByStatus(OutboxStatus status);
}
