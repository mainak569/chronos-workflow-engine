package com.chronos.scheduler.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for LeaderElectionService.
 */
@ExtendWith(MockitoExtension.class)
class LeaderElectionServiceTest {
    
    private static final String LEADER_KEY = "chronos:scheduler:leader";
    private static final String SCHEDULER_ID = "test-scheduler-1";
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    private LeaderElectionService leaderElectionService;
    
    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        
        // Create service with test scheduler ID
        leaderElectionService = new LeaderElectionService(redisTemplate, SCHEDULER_ID);
    }
    
    @Test
    void testTryAcquireLeadership_Success() {
        // Given: Leadership is available
        when(valueOperations.setIfAbsent(
                eq(LEADER_KEY),
                anyString(),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(true);
        
        // When: Try to acquire leadership
        boolean acquired = leaderElectionService.tryAcquireLeadership();
        
        // Then: Leadership acquired
        assertThat(acquired).isTrue();
        verify(valueOperations).setIfAbsent(
                eq(LEADER_KEY),
                contains(SCHEDULER_ID),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        );
    }
    
    @Test
    void testTryAcquireLeadership_AlreadyHeld() {
        // Given: Leadership already held by another scheduler
        when(valueOperations.setIfAbsent(
                eq(LEADER_KEY),
                anyString(),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(false);
        when(valueOperations.get(LEADER_KEY)).thenReturn("other-scheduler:timestamp");
        
        // When: Try to acquire leadership
        boolean acquired = leaderElectionService.tryAcquireLeadership();
        
        // Then: Leadership not acquired
        assertThat(acquired).isFalse();
    }
    
    @Test
    void testIsLeader_WhenLeader() {
        // Given: This scheduler is the leader
        when(valueOperations.get(LEADER_KEY)).thenReturn(SCHEDULER_ID + ":timestamp");
        
        // When: Check leadership
        boolean isLeader = leaderElectionService.isLeader();
        
        // Then: Returns true
        assertThat(isLeader).isTrue();
    }
    
    @Test
    void testIsLeader_WhenNotLeader() {
        // Given: Another scheduler is the leader
        when(valueOperations.get(LEADER_KEY)).thenReturn("other-scheduler:timestamp");
        
        // When: Check leadership
        boolean isLeader = leaderElectionService.isLeader();
        
        // Then: Returns false
        assertThat(isLeader).isFalse();
    }
    
    @Test
    void testIsLeader_NoLeader() {
        // Given: No leader exists
        when(valueOperations.get(LEADER_KEY)).thenReturn(null);
        
        // When: Check leadership
        boolean isLeader = leaderElectionService.isLeader();
        
        // Then: Returns false
        assertThat(isLeader).isFalse();
    }
    
    @Test
    void testGetCurrentLeader_LeaderExists() {
        // Given: A leader exists
        String leaderValue = "scheduler-2:2026-09-15T10:00:00Z";
        when(valueOperations.get(LEADER_KEY)).thenReturn(leaderValue);
        
        // When: Get current leader
        Optional<String> leader = leaderElectionService.getCurrentLeader();
        
        // Then: Returns leader value
        assertThat(leader).isPresent();
        assertThat(leader.get()).isEqualTo(leaderValue);
    }
    
    @Test
    void testGetCurrentLeader_NoLeader() {
        // Given: No leader exists
        when(valueOperations.get(LEADER_KEY)).thenReturn(null);
        
        // When: Get current leader
        Optional<String> leader = leaderElectionService.getCurrentLeader();
        
        // Then: Returns empty
        assertThat(leader).isEmpty();
    }
    
    @Test
    void testRenewLeadership_WhenLeader() {
        // Given: The renewal script confirms this scheduler holds leadership
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(LEADER_KEY)),
                eq(SCHEDULER_ID), contains(SCHEDULER_ID), anyString())).thenReturn(1L);
        
        // When: Renew leadership
        boolean renewed = leaderElectionService.renewLeadership();
        
        // Then: Leadership renewed without trying to acquire
        assertThat(renewed).isTrue();
        verify(valueOperations, never()).setIfAbsent(anyString(), anyString(), anyLong(), any());
    }
    
    @Test
    void testRenewLeadership_WhenNotLeader() {
        // Given: Another scheduler is the leader
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(LEADER_KEY)),
                eq(SCHEDULER_ID), anyString(), anyString())).thenReturn(0L);
        when(valueOperations.setIfAbsent(
                eq(LEADER_KEY),
                anyString(),
                anyLong(),
                eq(TimeUnit.MILLISECONDS)
        )).thenReturn(false);
        
        // When: Try to renew (will attempt to acquire)
        boolean renewed = leaderElectionService.renewLeadership();
        
        // Then: Failed (not leader)
        assertThat(renewed).isFalse();
    }
    
    @Test
    void testReleaseLeadership_WhenLeader() {
        // Given: This scheduler is the leader
        when(valueOperations.get(LEADER_KEY)).thenReturn(SCHEDULER_ID + ":timestamp");
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of(LEADER_KEY)), eq(SCHEDULER_ID)))
                .thenReturn(1L);
        
        // When: Release leadership
        leaderElectionService.releaseLeadership();
        
        // Then: Leadership released atomically (compare-and-delete script, not a blind DELETE)
        verify(redisTemplate).execute(any(RedisScript.class), eq(List.of(LEADER_KEY)), eq(SCHEDULER_ID));
        verify(redisTemplate, never()).delete(LEADER_KEY);
    }
    
    @Test
    void testReleaseLeadership_WhenNotLeader() {
        // Given: Another scheduler is the leader
        when(valueOperations.get(LEADER_KEY)).thenReturn("other-scheduler:timestamp");
        
        // When: Try to release leadership
        leaderElectionService.releaseLeadership();
        
        // Then: Does not delete (not the leader)
        verify(redisTemplate, never()).delete(LEADER_KEY);
        verify(redisTemplate, never()).execute(any(RedisScript.class), anyList(), any());
    }
    
    @Test
    void testIsLeader_SchedulerIdWithColons() {
        // Given: A scheduler ID that itself contains ':' (e.g. host:port)
        LeaderElectionService service = new LeaderElectionService(redisTemplate, "host:8082");
        when(valueOperations.get(LEADER_KEY)).thenReturn("host:8082:2026-09-17T10:00:00Z");
        
        // Then: Recognised as leader; a different ID sharing the prefix is not
        assertThat(service.isLeader()).isTrue();
        when(valueOperations.get(LEADER_KEY)).thenReturn("host:80821:2026-09-17T10:00:00Z");
        assertThat(service.isLeader()).isFalse();
    }
    
    @Test
    void testForceReleaseLeadership() {
        // Given: Any leadership state
        when(redisTemplate.delete(LEADER_KEY)).thenReturn(true);
        
        // When: Force release
        leaderElectionService.forceReleaseLeadership();
        
        // Then: Leadership deleted
        verify(redisTemplate).delete(LEADER_KEY);
    }
    
    @Test
    void testGetLeadershipStatus() {
        // Given: This scheduler is the leader with 25 seconds TTL
        when(valueOperations.get(LEADER_KEY)).thenReturn(SCHEDULER_ID + ":timestamp");
        when(redisTemplate.getExpire(LEADER_KEY, TimeUnit.SECONDS)).thenReturn(25L);
        
        // When: Get status
        LeaderElectionService.LeadershipStatus status = leaderElectionService.getLeadershipStatus();
        
        // Then: Status reflects current state
        assertThat(status.getSchedulerId()).isEqualTo(SCHEDULER_ID);
        assertThat(status.getCurrentLeader()).contains(SCHEDULER_ID);
        assertThat(status.isLeader()).isTrue();
        assertThat(status.getTtlSeconds()).isEqualTo(25L);
    }
}
