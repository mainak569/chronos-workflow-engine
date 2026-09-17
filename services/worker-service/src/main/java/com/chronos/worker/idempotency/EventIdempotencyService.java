package com.chronos.worker.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Service for detecting and preventing duplicate event processing.
 * 
 * Kafka provides at-least-once delivery, which means the same event may be delivered multiple times.
 * This service uses Redis to track processed event IDs and prevent duplicate processing.
 * 
 * Redis Key Structure:
 * - chronos:event:processed:{eventId} → "1" with TTL
 * 
 * How it works:
 * 1. Before processing an event, check if it was already processed
 * 2. If not processed, mark it as processed with a TTL
 * 3. If already processed, skip processing
 * 
 * TTL Considerations:
 * - Must be longer than Kafka's retention period to catch duplicates
 * - Must be short enough to avoid Redis memory issues
 * - Default: 7 days (matching typical Kafka retention)
 * 
 * Guarantees:
 * - Duplicate events within the TTL window will be detected and skipped
 * - No guarantee for duplicates outside the TTL window (very rare)
 * - At-least-once processing becomes effectively once within TTL window
 * 
 * Limitations:
 * - Redis failures will allow duplicates (graceful degradation)
 * - Not suitable for exactly-once semantics (use idempotent operations instead)
 */
@Service
public class EventIdempotencyService {
    
    private static final Logger log = LoggerFactory.getLogger(EventIdempotencyService.class);
    
    private static final String PROCESSED_EVENT_KEY_PREFIX = "chronos:event:processed:";
    
    private final RedisTemplate<String, String> redisTemplate;
    
    @Value("${chronos.event.idempotency-ttl:7d}")
    private Duration idempotencyTtl;
    
    public EventIdempotencyService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    /**
     * Check if an event has already been processed.
     * 
     * @param eventId the unique event identifier
     * @return true if the event was already processed, false otherwise
     */
    public boolean isProcessed(String eventId) {
        if (eventId == null || eventId.isEmpty()) {
            log.warn("Cannot check idempotency: eventId is null or empty");
            return false; // Allow processing if eventId is missing
        }
        
        String key = buildEventKey(eventId);
        Boolean exists = redisTemplate.hasKey(key);
        
        boolean processed = Boolean.TRUE.equals(exists);
        
        if (processed) {
            log.info("Duplicate event detected: eventId={}", eventId);
        }
        
        return processed;
    }
    
    /**
     * Mark an event as processed to prevent duplicate processing.
     * Uses SET NX (set if not exists) to atomically check and mark.
     * 
     * @param eventId the unique event identifier
     * @return true if this is the first time marking the event (should process),
     *         false if the event was already marked (duplicate, skip processing)
     */
    public boolean markAsProcessed(String eventId) {
        if (eventId == null || eventId.isEmpty()) {
            log.warn("Cannot mark event as processed: eventId is null or empty");
            return true; // Allow processing if eventId is missing
        }
        
        String key = buildEventKey(eventId);
        
        // Use SET NX EX to atomically set only if not exists
        Boolean wasSet = redisTemplate.opsForValue().setIfAbsent(
                key,
                "1",
                idempotencyTtl.toMillis(),
                TimeUnit.MILLISECONDS
        );
        
        boolean isFirstProcessing = Boolean.TRUE.equals(wasSet);
        
        if (isFirstProcessing) {
            log.debug("Event marked as processed: eventId={}, ttl={}", eventId, idempotencyTtl);
        } else {
            log.warn("Duplicate event detected during marking: eventId={}", eventId);
        }
        
        return isFirstProcessing;
    }
    
    /**
     * Check and mark an event as processed in a single atomic operation.
     * This is the recommended method for idempotency checking.
     * 
     * @param eventId the unique event identifier
     * @return true if the event should be processed (first time),
     *         false if the event is a duplicate (skip processing)
     */
    public boolean checkAndMark(String eventId) {
        return markAsProcessed(eventId);
    }
    
    /**
     * Manually remove an event from the processed set (admin operation).
     * Use this to reprocess a specific event after fixing an issue.
     * 
     * @param eventId the unique event identifier
     * @return true if the event was removed, false if it wasn't marked as processed
     */
    public boolean unmarkEvent(String eventId) {
        if (eventId == null || eventId.isEmpty()) {
            log.warn("Cannot unmark event: eventId is null or empty");
            return false;
        }
        
        String key = buildEventKey(eventId);
        Boolean deleted = redisTemplate.delete(key);
        
        boolean wasUnmarked = Boolean.TRUE.equals(deleted);
        
        if (wasUnmarked) {
            log.info("Event unmarked for reprocessing: eventId={}", eventId);
        } else {
            log.warn("Event was not marked as processed: eventId={}", eventId);
        }
        
        return wasUnmarked;
    }
    
    /**
     * Get the TTL remaining for an event's idempotency record.
     * 
     * @param eventId the unique event identifier
     * @return remaining TTL in milliseconds, or null if not processed or expired
     */
    public Long getRemainingTtl(String eventId) {
        if (eventId == null || eventId.isEmpty()) {
            return null;
        }
        
        String key = buildEventKey(eventId);
        Long ttl = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
        
        // Redis returns -2 if key doesn't exist, -1 if no TTL
        if (ttl != null && ttl < 0) {
            return null;
        }
        
        return ttl;
    }
    
    /**
     * Clear all processed event records (admin operation).
     * WARNING: This will allow all events to be reprocessed, potentially causing duplicates.
     * Only use for testing or emergency recovery.
     * 
     * @return number of records cleared
     */
    public long clearAll() {
        log.warn("Clearing ALL processed event records - this may cause duplicates!");
        
        var keys = redisTemplate.keys(PROCESSED_EVENT_KEY_PREFIX + "*");
        
        if (keys == null || keys.isEmpty()) {
            return 0;
        }
        
        Long deleted = redisTemplate.delete(keys);
        long count = deleted != null ? deleted : 0;
        
        log.warn("Cleared {} processed event records", count);
        return count;
    }
    
    private String buildEventKey(String eventId) {
        return PROCESSED_EVENT_KEY_PREFIX + eventId;
    }
}
