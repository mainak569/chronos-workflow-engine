package com.chronos.scheduler.integration;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.chronos.scheduler.service.WorkflowScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration tests for scheduler components.
 * 
 * Tests scenarios:
 * - Scheduled execution
 * - Duplicate prevention
 * - Multiple scheduler instances
 * - Leader failover
 */
@SpringBootTest
@ActiveProfiles("test")
class SchedulerIntegrationTest {
    
    @Autowired
    private LeaderElectionService leaderElectionService;
    
    @Autowired
    private SchedulerStateRepository schedulerStateRepository;
    
    @Autowired
    private ExecutionIdempotencyService idempotencyService;
    
    @Autowired
    private WorkflowScheduler workflowScheduler;
    
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        // Clear database
        schedulerStateRepository.deleteAll();
        
        // Mock Kafka sends
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(null);
    }
    
    @Test
    void testScheduledExecution() {
        // Given: A workflow scheduled to run now
        SchedulerState state = SchedulerState.builder()
                .workflowId("workflow-test-1")
                .schedule("0 * * * *")  // Every hour
                .timezone("UTC")
                .enabled(true)
                .nextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .build();
        
        schedulerStateRepository.save(state);
        
        // And: This scheduler is the leader
        leaderElectionService.tryAcquireLeadership();
        
        // When: Scheduling loop runs
        workflowScheduler.scheduleWorkflows();
        
        // Then: Workflow is scheduled
        SchedulerState updated = schedulerStateRepository.findByWorkflowId("workflow-test-1")
                .orElseThrow();
        
        assertThat(updated.getLastExecutionId()).isNotNull();
        assertThat(updated.getLastExecutionTime()).isNotNull();
        assertThat(updated.getNextScheduledTime()).isAfter(Instant.now());
        assertThat(updated.getExecutionCount()).isEqualTo(1);
    }
    
    @Test
    void testDuplicatePrevention_SameScheduler() {
        // Given: A workflow scheduled to run now
        SchedulerState state = SchedulerState.builder()
                .workflowId("workflow-test-2")
                .schedule("0 * * * *")
                .timezone("UTC")
                .enabled(true)
                .nextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .build();
        
        schedulerStateRepository.save(state);
        leaderElectionService.tryAcquireLeadership();
        
        // When: Scheduling loop runs twice
        workflowScheduler.scheduleWorkflows();
        
        // Reload state (simulating fresh read)
        state = schedulerStateRepository.findByWorkflowId("workflow-test-2").orElseThrow();
        
        // Reset nextScheduledTime to simulate race condition
        state.setNextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES));
        
        // Try to schedule again (should be prevented by optimistic locking)
        try {
            schedulerStateRepository.save(state);
            workflowScheduler.scheduleWorkflows();
        } catch (OptimisticLockingFailureException e) {
            // Expected - version mismatch
        }
        
        // Then: Only one execution created
        SchedulerState finalState = schedulerStateRepository.findByWorkflowId("workflow-test-2")
                .orElseThrow();
        
        assertThat(finalState.getExecutionCount()).isEqualTo(1);
    }
    
    @Test
    void testIdempotency_PreventsDuplicateExecutions() {
        // Given: A workflow and scheduled time
        String workflowId = "workflow-test-3";
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        
        // When: Two schedulers try to create execution for same time slot
        String execution1 = idempotencyService.getOrCreateExecution(
                workflowId,
                scheduledTime,
                () -> "exec-1"
        );
        
        String execution2 = idempotencyService.getOrCreateExecution(
                workflowId,
                scheduledTime,
                () -> "exec-2"
        );
        
        // Then: Both get the same execution ID
        assertThat(execution1).isEqualTo(execution2);
        
        // And: Existing execution can be retrieved
        Optional<String> existing = idempotencyService.getExistingExecution(workflowId, scheduledTime);
        assertThat(existing).isPresent();
        assertThat(existing.get()).isEqualTo(execution1);
    }
    
    @Test
    void testLeaderElection_OnlyLeaderSchedules() {
        // Given: A workflow ready to execute
        SchedulerState state = SchedulerState.builder()
                .workflowId("workflow-test-4")
                .schedule("0 * * * *")
                .timezone("UTC")
                .enabled(true)
                .nextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .build();
        
        schedulerStateRepository.save(state);
        
        // When: Non-leader tries to schedule
        leaderElectionService.releaseLeadership();  // Ensure not leader
        workflowScheduler.scheduleWorkflows();
        
        // Then: Workflow not scheduled
        SchedulerState notScheduled = schedulerStateRepository.findByWorkflowId("workflow-test-4")
                .orElseThrow();
        
        assertThat(notScheduled.getLastExecutionId()).isNull();
        
        // When: Become leader and schedule
        leaderElectionService.tryAcquireLeadership();
        workflowScheduler.scheduleWorkflows();
        
        // Then: Workflow is scheduled
        SchedulerState scheduled = schedulerStateRepository.findByWorkflowId("workflow-test-4")
                .orElseThrow();
        
        assertThat(scheduled.getLastExecutionId()).isNotNull();
    }
    
    @Test
    void testDisabledWorkflow_NotScheduled() {
        // Given: A disabled workflow ready to execute
        SchedulerState state = SchedulerState.builder()
                .workflowId("workflow-test-5")
                .schedule("0 * * * *")
                .timezone("UTC")
                .enabled(false)  // Disabled
                .nextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                .build();
        
        schedulerStateRepository.save(state);
        leaderElectionService.tryAcquireLeadership();
        
        // When: Scheduling loop runs
        workflowScheduler.scheduleWorkflows();
        
        // Then: Workflow not scheduled
        SchedulerState notScheduled = schedulerStateRepository.findByWorkflowId("workflow-test-5")
                .orElseThrow();
        
        assertThat(notScheduled.getLastExecutionId()).isNull();
        assertThat(notScheduled.getExecutionCount()).isEqualTo(0);
    }
    
    @Test
    void testLeadershipTransition() {
        // Given: Scheduler 1 is leader
        boolean acquired1 = leaderElectionService.tryAcquireLeadership();
        assertThat(acquired1).isTrue();
        assertThat(leaderElectionService.isLeader()).isTrue();
        
        // When: Scheduler 1 releases leadership
        leaderElectionService.releaseLeadership();
        
        // Then: No longer leader
        assertThat(leaderElectionService.isLeader()).isFalse();
        
        // When: Try to acquire again
        boolean acquired2 = leaderElectionService.tryAcquireLeadership();
        
        // Then: Can acquire leadership again
        assertThat(acquired2).isTrue();
        assertThat(leaderElectionService.isLeader()).isTrue();
    }
}
