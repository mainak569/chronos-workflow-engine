package com.chronos.scheduler.idempotency;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ExecutionIdempotencyService.
 */
@ExtendWith(MockitoExtension.class)
class ExecutionIdempotencyServiceTest {
    
    private static final String WORKFLOW_ID = "workflow-123";
    private static final String EXECUTION_ID = "exec-456";
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    private ExecutionIdempotencyService idempotencyService;
    
    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        idempotencyService = new ExecutionIdempotencyService(redisTemplate);
    }
    
    @Test
    void testGetExistingExecution_Found() {
        // Given: Execution already exists
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(valueOperations.get(idempotencyKey)).thenReturn(EXECUTION_ID);
        
        // When: Get existing execution
        Optional<String> result = idempotencyService.getExistingExecution(WORKFLOW_ID, scheduledTime);
        
        // Then: Returns existing execution ID
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(EXECUTION_ID);
    }
    
    @Test
    void testGetExistingExecution_NotFound() {
        // Given: No execution exists
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(valueOperations.get(idempotencyKey)).thenReturn(null);
        
        // When: Get existing execution
        Optional<String> result = idempotencyService.getExistingExecution(WORKFLOW_ID, scheduledTime);
        
        // Then: Returns empty
        assertThat(result).isEmpty();
    }
    
    @Test
    void testRecordExecution_FirstTime() {
        // Given: No existing execution
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(valueOperations.setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(true);
        
        // When: Record execution
        boolean isFirst = idempotencyService.recordExecution(WORKFLOW_ID, scheduledTime, EXECUTION_ID);
        
        // Then: Returns true (first execution)
        assertThat(isFirst).isTrue();
        verify(valueOperations).setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        );
    }
    
    @Test
    void testRecordExecution_Duplicate() {
        // Given: Execution already exists
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(valueOperations.setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(false);
        when(valueOperations.get(idempotencyKey)).thenReturn("exec-existing");
        
        // When: Try to record execution
        boolean isFirst = idempotencyService.recordExecution(WORKFLOW_ID, scheduledTime, EXECUTION_ID);
        
        // Then: Returns false (duplicate)
        assertThat(isFirst).isFalse();
    }
    
    @Test
    void testGetOrCreateExecution_ExistingFound() {
        // Given: Execution already exists
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        String existingExecutionId = "exec-existing";
        
        when(valueOperations.get(idempotencyKey)).thenReturn(existingExecutionId);
        
        // When: Get or create execution
        String result = idempotencyService.getOrCreateExecution(
                WORKFLOW_ID,
                scheduledTime,
                () -> EXECUTION_ID
        );
        
        // Then: Returns existing execution ID (supplier not called)
        assertThat(result).isEqualTo(existingExecutionId);
        verify(valueOperations, never()).setIfAbsent(any(), any(), anyLong(), any());
    }
    
    @Test
    void testGetOrCreateExecution_NewCreated() {
        // Given: No existing execution
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(valueOperations.get(idempotencyKey)).thenReturn(null);
        when(valueOperations.setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(true);
        
        // When: Get or create execution
        String result = idempotencyService.getOrCreateExecution(
                WORKFLOW_ID,
                scheduledTime,
                () -> EXECUTION_ID
        );
        
        // Then: Creates and returns new execution ID
        assertThat(result).isEqualTo(EXECUTION_ID);
        verify(valueOperations).setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        );
    }
    
    @Test
    void testGetOrCreateExecution_RaceCondition() {
        // Given: Execution doesn't exist on first check, but created by another scheduler before record
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        String existingExecutionId = "exec-from-other-scheduler";
        
        when(valueOperations.get(idempotencyKey))
                .thenReturn(null)  // First check: not found
                .thenReturn(existingExecutionId);  // Second check: found (after race)
        
        when(valueOperations.setIfAbsent(
                eq(idempotencyKey),
                eq(EXECUTION_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(false);  // Failed to record (already exists)
        
        // When: Get or create execution
        String result = idempotencyService.getOrCreateExecution(
                WORKFLOW_ID,
                scheduledTime,
                () -> EXECUTION_ID
        );
        
        // Then: Returns existing execution ID from other scheduler
        assertThat(result).isEqualTo(existingExecutionId);
    }
    
    @Test
    void testRemoveIdempotencyRecord() {
        // Given: Idempotency record exists
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(redisTemplate.delete(idempotencyKey)).thenReturn(true);
        
        // When: Remove idempotency record
        boolean removed = idempotencyService.removeIdempotencyRecord(WORKFLOW_ID, scheduledTime);
        
        // Then: Returns true
        assertThat(removed).isTrue();
        verify(redisTemplate).delete(idempotencyKey);
    }
    
    @Test
    void testGetRemainingTtl_KeyExists() {
        // Given: Key exists with TTL
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(redisTemplate.getExpire(idempotencyKey, TimeUnit.SECONDS)).thenReturn(3000L);
        
        // When: Get remaining TTL
        Long ttl = idempotencyService.getRemainingTtl(WORKFLOW_ID, scheduledTime);
        
        // Then: Returns TTL
        assertThat(ttl).isEqualTo(3000L);
    }
    
    @Test
    void testGetRemainingTtl_KeyDoesNotExist() {
        // Given: Key doesn't exist
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        String idempotencyKey = "chronos:execution:idempotency:" + WORKFLOW_ID + ":2026-09-15T10:00";
        
        when(redisTemplate.getExpire(idempotencyKey, TimeUnit.SECONDS)).thenReturn(-2L);
        
        // When: Get remaining TTL
        Long ttl = idempotencyService.getRemainingTtl(WORKFLOW_ID, scheduledTime);
        
        // Then: Returns null
        assertThat(ttl).isNull();
    }
}
