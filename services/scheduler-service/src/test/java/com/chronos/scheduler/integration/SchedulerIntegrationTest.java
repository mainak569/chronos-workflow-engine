package com.chronos.scheduler.integration;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowDefinition;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.outbox.OutboxMessageRepository;
import com.chronos.scheduler.outbox.OutboxStatus;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import com.chronos.scheduler.service.WorkflowScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for scheduler components.
 *
 * Tests scenarios:
 * - Scheduled execution (execution + tasks created, ready tasks dispatched via outbox)
 * - Duplicate prevention
 * - Only the leader schedules
 * - Schedule synchronisation from workflow definitions
 */
@SpringBootTest
@Testcontainers
class SchedulerIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0");

    @Container
    static GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", redisContainer::getFirstMappedPort);
        // Drive scheduling manually and keep Kafka out of the picture
        registry.add("chronos.kafka.listener.auto-startup", () -> "false");
        registry.add("chronos.scheduler.scheduling.interval", () -> "3600000");
        registry.add("chronos.scheduler.scheduling.sync-interval", () -> "3600000");
        registry.add("chronos.scheduler.restart-recovery.enabled", () -> "false");
        registry.add("chronos.outbox.poll-interval", () -> "3600000");
    }

    @Autowired
    private LeaderElectionService leaderElectionService;

    @Autowired
    private SchedulerStateRepository schedulerStateRepository;

    @Autowired
    private WorkflowExecutionRepository workflowExecutionRepository;

    @Autowired
    private TaskExecutionRepository taskExecutionRepository;

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @Autowired
    private ExecutionIdempotencyService idempotencyService;

    @Autowired
    private WorkflowScheduler workflowScheduler;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void setUp() {
        schedulerStateRepository.deleteAll();
        workflowExecutionRepository.deleteAll();
        taskExecutionRepository.deleteAll();
        outboxMessageRepository.deleteAll();
        mongoTemplate.dropCollection(WorkflowDefinition.class);
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    void testScheduledExecution() {
        // Given: A scheduled workflow (a -> b) that is due
        saveWorkflow("workflow-test-1", "0 0 * * * *");
        saveDueState("workflow-test-1", true);
        leaderElectionService.tryAcquireLeadership();

        // When: Scheduling loop runs
        workflowScheduler.scheduleWorkflows();

        // Then: Scheduler state advanced
        SchedulerState updated = schedulerStateRepository.findByWorkflowId("workflow-test-1").orElseThrow();
        assertThat(updated.getLastExecutionId()).isNotNull();
        assertThat(updated.getLastExecutionTime()).isNotNull();
        assertThat(updated.getNextScheduledTime()).isAfter(Instant.now());
        assertThat(updated.getExecutionCount()).isEqualTo(1);

        // And: Execution with all task executions was created
        WorkflowExecution execution = workflowExecutionRepository.findById(updated.getLastExecutionId()).orElseThrow();
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.PENDING);
        assertThat(execution.getOwnerId()).isEqualTo("owner-1");
        assertThat(execution.getTriggeredBy()).isEqualTo("scheduler");
        List<TaskExecution> tasks = taskExecutionRepository.findByExecutionId(execution.getId());
        assertThat(tasks).extracting(TaskExecution::getTaskId).containsExactlyInAnyOrder("a", "b");

        // And: Only the task without dependencies was dispatched through the outbox
        TaskExecution a = taskExecutionRepository.findByExecutionIdAndTaskId(execution.getId(), "a").orElseThrow();
        TaskExecution b = taskExecutionRepository.findByExecutionIdAndTaskId(execution.getId(), "b").orElseThrow();
        assertThat(a.getDispatchedAt()).isNotNull();
        assertThat(b.getDispatchedAt()).isNull();
        assertThat(outboxMessageRepository.countByStatus(OutboxStatus.PENDING)).isEqualTo(1);
    }

    @Test
    void testDuplicatePrevention_SameSlot() {
        // Given: A due scheduled workflow
        saveWorkflow("workflow-test-2", "0 0 * * * *");
        SchedulerState state = saveDueState("workflow-test-2", true);
        Instant slot = state.getNextScheduledTime();
        leaderElectionService.tryAcquireLeadership();

        workflowScheduler.scheduleWorkflows();

        // When: The same slot is scheduled again (e.g. a retry after a crash before the state was saved)
        SchedulerState reloaded = schedulerStateRepository.findByWorkflowId("workflow-test-2").orElseThrow();
        reloaded.setNextScheduledTime(slot);
        schedulerStateRepository.save(reloaded);
        workflowScheduler.scheduleWorkflows();

        // Then: The same execution is reused and its tasks are not dispatched twice
        assertThat(workflowExecutionRepository.findAll()).hasSize(1);
        assertThat(taskExecutionRepository.findAll()).hasSize(2);
        assertThat(outboxMessageRepository.count()).isEqualTo(1);
    }

    @Test
    void testIdempotency_PreventsDuplicateExecutions() {
        // Given: A workflow and scheduled time
        String workflowId = "workflow-test-3";
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");

        // When: Two schedulers try to create execution for same time slot
        String execution1 = idempotencyService.getOrCreateExecution(workflowId, scheduledTime, () -> "exec-1");
        String execution2 = idempotencyService.getOrCreateExecution(workflowId, scheduledTime, () -> "exec-2");

        // Then: Both get the same execution ID
        assertThat(execution1).isEqualTo(execution2);

        // And: Existing execution can be retrieved
        Optional<String> existing = idempotencyService.getExistingExecution(workflowId, scheduledTime);
        assertThat(existing).hasValue(execution1);
    }

    @Test
    void testLeaderElection_OnlyLeaderSchedules() {
        // Given: A workflow ready to execute
        saveWorkflow("workflow-test-4", "0 0 * * * *");
        saveDueState("workflow-test-4", true);

        // When: Non-leader tries to schedule
        leaderElectionService.releaseLeadership();
        redisTemplate.opsForValue().set("chronos:scheduler:leader", "another-scheduler:" + Instant.now());
        workflowScheduler.scheduleWorkflows();

        // Then: Workflow not scheduled
        assertThat(schedulerStateRepository.findByWorkflowId("workflow-test-4").orElseThrow()
                .getLastExecutionId()).isNull();

        // When: Become leader and schedule
        redisTemplate.delete("chronos:scheduler:leader");
        assertThat(leaderElectionService.tryAcquireLeadership()).isTrue();
        workflowScheduler.scheduleWorkflows();

        // Then: Workflow is scheduled
        assertThat(schedulerStateRepository.findByWorkflowId("workflow-test-4").orElseThrow()
                .getLastExecutionId()).isNotNull();
    }

    @Test
    void testDisabledWorkflow_NotScheduled() {
        // Given: A disabled workflow ready to execute
        saveWorkflow("workflow-test-5", "0 0 * * * *");
        saveDueState("workflow-test-5", false);
        leaderElectionService.tryAcquireLeadership();

        // When: Scheduling loop runs
        workflowScheduler.scheduleWorkflows();

        // Then: Workflow not scheduled
        SchedulerState notScheduled = schedulerStateRepository.findByWorkflowId("workflow-test-5").orElseThrow();
        assertThat(notScheduled.getLastExecutionId()).isNull();
        assertThat(notScheduled.getExecutionCount()).isEqualTo(0);
    }

    @Test
    void testScheduleSync_CreatesAndRemovesState() {
        // Given: One scheduled workflow and stale state for a workflow that no longer exists
        saveWorkflow("workflow-sync", "0 30 * * * *");
        saveDueState("workflow-deleted", true);
        leaderElectionService.tryAcquireLeadership();

        // When: Schedules are synchronised
        workflowScheduler.syncSchedules();

        // Then: State exists for the scheduled workflow with a future run time, stale state removed
        SchedulerState synced = schedulerStateRepository.findByWorkflowId("workflow-sync").orElseThrow();
        assertThat(synced.isEnabled()).isTrue();
        assertThat(synced.getSchedule()).isEqualTo("0 30 * * * *");
        assertThat(synced.getNextScheduledTime()).isAfter(Instant.now());
        assertThat(schedulerStateRepository.findByWorkflowId("workflow-deleted")).isEmpty();
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

    private void saveWorkflow(String workflowId, String cron) {
        WorkflowDefinition.TaskSpec a = new WorkflowDefinition.TaskSpec();
        a.setTaskId("a");
        a.setName("A");
        a.setTaskType("DATA_PROCESSING");

        WorkflowDefinition.TaskSpec b = new WorkflowDefinition.TaskSpec();
        b.setTaskId("b");
        b.setName("B");
        b.setTaskType("DATA_VALIDATION");
        b.setDependencies(Set.of("a"));
        b.setConfiguration(Map.of("threshold", 5));

        WorkflowDefinition workflow = new WorkflowDefinition();
        workflow.setId(workflowId);
        workflow.setOwnerId("owner-1");
        workflow.setName("Scheduled " + workflowId);
        workflow.setSchedule(cron);
        workflow.setTimezone("UTC");
        workflow.setTasks(List.of(a, b));
        mongoTemplate.save(workflow);
    }

    private SchedulerState saveDueState(String workflowId, boolean enabled) {
        SchedulerState state = SchedulerState.builder()
                .workflowId(workflowId)
                .schedule("0 0 * * * *")
                .timezone("UTC")
                .enabled(enabled)
                .nextScheduledTime(Instant.now().minus(1, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS))
                .build();
        return schedulerStateRepository.save(state);
    }
}
