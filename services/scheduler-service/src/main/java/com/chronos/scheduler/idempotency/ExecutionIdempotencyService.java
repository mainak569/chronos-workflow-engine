package com.chronos.scheduler.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Prevents duplicate workflow executions using Redis-based idempotency.
 * 
 * When multiple schedulers race to schedule the same workflow at the same time,
 * this service ensures only one execution is created.
 * 
 * Key Format: chronos:execution:idempotency:{workflowId}:{timeSlot}
 * Value: executionId
 * TTL: 1 hour (longer than max execution time)
 * 
 * Example:
 * Key: chronos:execution:idempotency:workflow-123:2026-09-15T10:00:00
* Value: exec-456
 * TTL: 1 hour
 * 
 * Time Slot Format:
 * - Truncated to the second (cron schedules have second precision, so every run gets its own slot)
*/
@Service
public class ExecutionIdempotencyService {
    
    private static final Logger log = LoggerFactory.getLogger(ExecutionIdempotencyService.class);
    
    private static final String IDEMPOTENCY_KEY_PREFIX = "chronos:execution:idempotency:";
    private static final DateTimeFormatter TIME_SLOT_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.of("UTC"));
    
    private final RedisTemplate<String, String> redisTemplate;
    
    @Value("${chronos.scheduler.execution-idempotency.ttl:1h}")
    private Duration idempotencyTtl = Duration.ofHours(1);
    
    public ExecutionIdempotencyService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    /**
     * Get existing execution ID for this workflow/time slot, or null if none exists.
     * 
     * @param workflowId workflow identifier
     * @param scheduledTime scheduled execution time
     * @return Optional containing execution ID if already created
     */
    public Optional<String> getExistingExecution(String workflowId, Instant scheduledTime) {
        String key = buildIdempotencyKey(workflowId, scheduledTime);
        String executionId = redisTemplate.opsForValue().get(key);
        
        if (executionId != null) {
            log.debug("Found existing execution: workflowId={}, timeSlot={}, executionId={}",
                    workflowId, formatTimeSlot(scheduledTime), executionId);
        }
        
        return Optional.ofNullable(executionId);
    }
    
    /**
     * Record execution for idempotency tracking.
     * Uses SET NX to atomically check-and-set.
     * 
     * @param workflowId workflow identifier
     * @param scheduledTime scheduled execution time
     * @param executionId execution identifier
     * @return true if this is the first execution for this time slot, false if duplicate
     */
    public boolean recordExecution(String workflowId, Instant scheduledTime, String executionId) {
        String key = buildIdempotencyKey(workflowId, scheduledTime);
        
        Boolean wasSet = redisTemplate.opsForValue().setIfAbsent(
                key,
                executionId,
                idempotencyTtl.toMillis(),
                TimeUnit.MILLISECONDS
        );
        
        boolean isFirst = Boolean.TRUE.equals(wasSet);
        
        if (isFirst) {
            log.debug("Recorded new execution: workflowId={}, timeSlot={}, executionId={}",
                    workflowId, formatTimeSlot(scheduledTime), executionId);
        } else {
            String existingExecution = redisTemplate.opsForValue().get(key);
            log.info("Duplicate execution prevented: workflowId={}, timeSlot={}, " +
                            "existingExecution={}, attemptedExecution={}",
                    workflowId, formatTimeSlot(scheduledTime), existingExecution, executionId);
        }
        
        return isFirst;
    }
    
    /**
     * Get or create execution ID with idempotency.
     * First checks if execution already exists, returns existing ID if found.
     * Otherwise creates new execution and records it.
     * 
     * This is a convenience method that combines get + record.
     * 
     * @param workflowId workflow identifier
     * @param scheduledTime scheduled execution time
     * @param executionIdSupplier function to create new execution ID
     * @return execution ID (existing or newly created)
     */
    public String getOrCreateExecution(String workflowId, Instant scheduledTime,
                                      java.util.function.Supplier<String> executionIdSupplier) {
        // Check if already exists
        Optional<String> existing = getExistingExecution(workflowId, scheduledTime);
        if (existing.isPresent()) {
            return existing.get();
        }
        
        // Create new execution
        String newExecutionId = executionIdSupplier.get();
        
        // Record with idempotency check
        boolean recorded = recordExecution(workflowId, scheduledTime, newExecutionId);
        
        if (!recorded) {
            // Another scheduler created execution between our check and record
            // Return the existing one
            return getExistingExecution(workflowId, scheduledTime)
                    .orElse(newExecutionId); // Fallback to new ID if somehow missing
        }
        
        return newExecutionId;
    }
    
    /**
     * Remove idempotency record (admin operation).
     * Use this to allow re-execution of a specific time slot.
     * 
     * @param workflowId workflow identifier
     * @param scheduledTime scheduled execution time
     * @return true if record was removed
     */
    public boolean removeIdempotencyRecord(String workflowId, Instant scheduledTime) {
        String key = buildIdempotencyKey(workflowId, scheduledTime);
        Boolean deleted = redisTemplate.delete(key);
        
        boolean removed = Boolean.TRUE.equals(deleted);
        if (removed) {
            log.info("Removed idempotency record: workflowId={}, timeSlot={}",
                    workflowId, formatTimeSlot(scheduledTime));
        }
        
        return removed;
    }
    
    /**
     * Get remaining TTL for idempotency record.
     * 
     * @param workflowId workflow identifier
     * @param scheduledTime scheduled execution time
     * @return remaining TTL in seconds, or null if not found
     */
    public Long getRemainingTtl(String workflowId, Instant scheduledTime) {
        String key = buildIdempotencyKey(workflowId, scheduledTime);
        Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
        
        // Redis returns -2 if key doesn't exist, -1 if no TTL
        if (ttl != null && ttl < 0) {
            return null;
        }
        
        return ttl;
    }
    
    /**
     * Build idempotency key from workflow ID and scheduled time.
     * 
     * Format: chronos:execution:idempotency:{workflowId}:{timeSlot}
     * Example: chronos:execution:idempotency:workflow-123:2026-09-15T10:00:00
     */
    private String buildIdempotencyKey(String workflowId, Instant scheduledTime) {
        String timeSlot = formatTimeSlot(scheduledTime);
        return IDEMPOTENCY_KEY_PREFIX + workflowId + ":" + timeSlot;
    }
    
    /**
     * Format time slot for idempotency key.
     * Truncates to the second (removes nanos), matching cron's precision.
     *
     * @param time instant to format
     * @return formatted time slot (YYYY-MM-DDTHH:MM:SS)
     */
    private String formatTimeSlot(Instant time) {
        return TIME_SLOT_FORMATTER.format(Instant.ofEpochSecond(time.getEpochSecond()));
    }
}
