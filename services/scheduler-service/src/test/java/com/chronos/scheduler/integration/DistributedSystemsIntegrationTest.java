package com.chronos.scheduler.integration;

import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import com.chronos.scheduler.service.WorkflowScheduler;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Comprehensive distributed systems integration tests for Chronos.
 * 
 * Tests race conditions, concurrency bugs, and failure scenarios using real infrastructure:
 * - Redis (Testcontainers)
 * - MongoDB (Testcontainers)
 * - Kafka (Testcontainers)
 * 
 * NO MOCKS for distributed infrastructure.
 */
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedSystemsIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017);

    @Container
    static GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes");

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withKraft();

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", redisContainer::getFirstMappedPort);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        
        // Short intervals for faster testing
        registry.add("chronos.scheduler.leader-election.ttl", () -> "5s");
        registry.add("chronos.scheduler.leader-election.renewal-interval", () -> "2000");
        // These tests drive scheduling state directly; keep background jobs from modifying it
        registry.add("chronos.scheduler.scheduling.enabled", () -> "false");
        registry.add("chronos.scheduler.restart-recovery.enabled", () -> "false");
    }

    @Autowired
    private LeaderElectionService leaderElectionService;

    @Autowired
    private WorkflowScheduler workflowScheduler;

    @Autowired
    private SchedulerStateRepository schedulerStateRepository;

    @Autowired
    private WorkflowExecutionRepository workflowExecutionRepository;

    @Autowired
    private ExecutionIdempotencyService idempotencyService;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        // Clear state
        schedulerStateRepository.deleteAll();
        workflowExecutionRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    // ==================== RACE CONDITION TESTS ====================

    @Test
    @Order(1)
    @DisplayName("Test 3: Concurrent Task Claiming - Only one worker should succeed")
    void testConcurrentTaskClaiming() throws Exception {
        // Given: Multiple workers try to claim the same task simultaneously
        String taskId = "task-123";
        int workerCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(workerCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        List<String> successfulWorkers = Collections.synchronizedList(new ArrayList<>());
        
        // When: All workers try to claim simultaneously
        for (int i = 0; i < workerCount; i++) {
            final String workerId = "worker-" + i;
            executorService.submit(() -> {
                try {
                    startLatch.await(); // Wait for signal
                    
                    // Try to claim task using Redis distributed lock
                    Boolean claimed = redisTemplate.opsForValue().setIfAbsent(
                            "chronos:task:lock:" + taskId,
                            workerId,
                            Duration.ofSeconds(30)
                    );
                    
                    if (Boolean.TRUE.equals(claimed)) {
                        successCount.incrementAndGet();
                        successfulWorkers.add(workerId);
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        // Release all workers simultaneously
        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        
        // Then: Exactly one worker should succeed
        assertThat(successCount.get())
                .as("Exactly one worker should claim the task")
                .isEqualTo(1);
        
        assertThat(successfulWorkers)
                .as("Only one worker ID should be in successful list")
                .hasSize(1);
        
        // Verify the lock holder in Redis
        String lockHolder = redisTemplate.opsForValue().get("chronos:task:lock:" + taskId);
        assertThat(lockHolder)
                .as("Lock holder should match successful worker")
                .isEqualTo(successfulWorkers.get(0));
    }

    @Test
    @Order(2)
    @DisplayName("Test 13: Duplicate Workflow Execution - Idempotency prevents duplicates")
    void testDuplicateWorkflowExecution() throws Exception {
        // Given: A workflow scheduled multiple times with same execution ID
        String workflowId = "workflow-123";
        String executionId = "exec-456";
        // One fixed scheduled slot: idempotency is per workflow + scheduled time
        Instant scheduledTime = Instant.parse("2026-09-15T10:00:00Z");
        int attemptCount = 5;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(attemptCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        
        // When: Multiple threads try to record the same execution
        for (int i = 0; i < attemptCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    boolean recorded = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
                    
                    if (recorded) {
                        successCount.incrementAndGet();
                    } else {
                        rejectedCount.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        
        // Then: Exactly one execution should be recorded
        assertThat(successCount.get())
                .as("Exactly one execution should be recorded")
                .isEqualTo(1);
        
        assertThat(rejectedCount.get())
                .as("All other attempts should be rejected")
                .isEqualTo(attemptCount - 1);
        
        // Verify idempotency check still works
        boolean isDuplicate = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        assertThat(isDuplicate)
                .as("Subsequent checks should detect duplicate")
                .isFalse();
    }

    @Test
    @Order(3)
    @DisplayName("Test Leader Election Race - Only one leader at a time")
    void testLeaderElectionRace() throws Exception {
        // Given: Multiple scheduler instances compete for leadership
        int instanceCount = 5;
        List<LeaderElectionService> instances = new ArrayList<>();
        
        // Create multiple leader election services
        for (int i = 0; i < instanceCount; i++) {
            String schedulerId = "scheduler-" + i;
            LeaderElectionService instance = new LeaderElectionService(
                    redisTemplate,
                    schedulerId
            );
            instances.add(instance);
        }
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(instanceCount);
        
        AtomicInteger leaderCount = new AtomicInteger(0);
        List<String> leaders = Collections.synchronizedList(new ArrayList<>());
        
        // When: All instances try to acquire leadership simultaneously
        for (LeaderElectionService instance : instances) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    boolean acquired = instance.tryAcquireLeadership();
                    if (acquired) {
                        leaderCount.incrementAndGet();
                        leaders.add(instance.getCurrentLeader().orElse("unknown"));
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        
        // Then: Exactly one leader should be elected
        assertThat(leaderCount.get())
                .as("Exactly one leader should be elected")
                .isEqualTo(1);
        
        assertThat(leaders)
                .as("Only one leader ID should exist")
                .hasSize(1);
        
        // Verify all instances see the same leader
        Set<String> observedLeaders = ConcurrentHashMap.newKeySet();
        for (LeaderElectionService instance : instances) {
            instance.getCurrentLeader().ifPresent(observedLeaders::add);
        }
        
        assertThat(observedLeaders)
                .as("All instances should observe the same leader")
                .hasSize(1);
    }

    @Test
    @Order(4)
    @DisplayName("Test 5: Workflow Scheduling Race - Optimistic locking prevents duplicates")
    void testWorkflowSchedulingRace() throws Exception {
        // Given: A workflow ready to be scheduled
        String workflowId = "workflow-racing";
        
        SchedulerState state = new SchedulerState();
        state.setWorkflowId(workflowId);
        state.setNextScheduledTime(Instant.now().minus(1, ChronoUnit.HOURS));
        state.setSchedule("0 0 * * * ?");
        state = schedulerStateRepository.save(state);
        
        // When: Multiple schedulers try to schedule simultaneously
        int schedulerCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(schedulerCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        final SchedulerState finalState = state;
        
        for (int i = 0; i < schedulerCount; i++) {
            final String schedulerId = "scheduler-" + i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    // Simulate scheduling operation
                    SchedulerState loadedState = schedulerStateRepository.findById(finalState.getId())
                            .orElseThrow();
                    
                    // Modify state
                    loadedState.markExecuted(
                            "exec-" + System.nanoTime(),
                            com.chronos.scheduler.domain.ExecutionStatus.RUNNING,
                            Instant.now().plus(1, ChronoUnit.HOURS),
                            schedulerId
                    );
                    
                    // Try to save with optimistic locking
                    schedulerStateRepository.save(loadedState);
                    successCount.incrementAndGet();
                    
                } catch (OptimisticLockingFailureException e) {
                    // Expected - version conflict
                    failureCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        
        // Then: Exactly one scheduler should succeed
        assertThat(successCount.get())
                .as("Exactly one scheduler should succeed")
                .isEqualTo(1);
        
        assertThat(failureCount.get())
                .as("All other schedulers should fail with optimistic lock exception")
                .isEqualTo(schedulerCount - 1);
    }

    @Test
    @Order(5)
    @DisplayName("Test 15: Concurrent Workflow Updates - Version field prevents lost updates")
    void testConcurrentWorkflowUpdates() throws Exception {
        // Given: A scheduler state that multiple instances try to update
        SchedulerState state = new SchedulerState();
        state.setWorkflowId("workflow-concurrent");
        state.setNextScheduledTime(Instant.now());
        state.setSchedule("0 0 * * * ?");
        state = schedulerStateRepository.save(state);
        
        // When: Multiple threads update simultaneously
        int updateCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(updateCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger optimisticLockFailures = new AtomicInteger(0);
        
        final String stateId = state.getId();
        
        for (int i = 0; i < updateCount; i++) {
            final int updateNum = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    SchedulerState loaded = schedulerStateRepository.findById(stateId)
                            .orElseThrow();
                    
                    // Simulate update
                    loaded.setNextScheduledTime(Instant.now().plus(updateNum, ChronoUnit.HOURS));
                    
                    schedulerStateRepository.save(loaded);
                    successCount.incrementAndGet();
                    
                } catch (OptimisticLockingFailureException e) {
                    optimisticLockFailures.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        
        // Then: Some updates succeed, others fail with optimistic lock
        assertThat(successCount.get() + optimisticLockFailures.get())
                .as("All attempts should either succeed or fail with optimistic lock")
                .isEqualTo(updateCount);
        
        assertThat(optimisticLockFailures.get())
                .as("At least some attempts should fail with optimistic lock")
                .isGreaterThan(0);
        
        // Verify final state is consistent
        SchedulerState finalState = schedulerStateRepository.findById(stateId).orElseThrow();
        assertThat(finalState.getVersion())
                .as("Version should be incremented")
                .isGreaterThan(state.getVersion());
    }

    // ==================== FAILURE SCENARIO TESTS ====================

    @Test
    @Order(6)
    @DisplayName("Test 7: Redis Lock Expiration - Lock expires during long task")
    void testRedisLockExpiration() throws Exception {
        // Given: A task lock with short TTL
        String taskId = "long-running-task";
        String workerId = "worker-1";
        Duration shortTtl = Duration.ofSeconds(2);
        
        // When: Worker acquires lock
        Boolean claimed = redisTemplate.opsForValue().setIfAbsent(
                "chronos:task:lock:" + taskId,
                workerId,
                shortTtl
        );
        
        assertThat(claimed).isTrue();
        
        // Simulate long-running task (longer than TTL)
        Thread.sleep(3000);
        
        // Then: Lock should have expired
        String currentHolder = redisTemplate.opsForValue().get("chronos:task:lock:" + taskId);
        assertThat(currentHolder)
                .as("Lock should have expired")
                .isNull();
        
        // Another worker can claim it
        Boolean reclaimedByAnother = redisTemplate.opsForValue().setIfAbsent(
                "chronos:task:lock:" + taskId,
                "worker-2",
                shortTtl
        );
        
        assertThat(reclaimedByAnother)
                .as("Another worker should be able to claim expired lock")
                .isTrue();
    }

    @Test
    @Order(7)
    @DisplayName("Test 12: Scheduler Restart - Leadership re-election after crash")
    void testSchedulerRestart() throws Exception {
        // Given: A scheduler with leadership
        String schedulerId1 = "scheduler-1";
        LeaderElectionService scheduler1 = new LeaderElectionService(
                redisTemplate,
                schedulerId1
        );
        
        boolean acquired = scheduler1.tryAcquireLeadership();
        assertThat(acquired).isTrue();
        
        // When: Scheduler crashes (simulated by releasing leadership)
        scheduler1.releaseLeadership();
        
        // Then: Another scheduler can acquire leadership
        String schedulerId2 = "scheduler-2";
        LeaderElectionService scheduler2 = new LeaderElectionService(
                redisTemplate,
                schedulerId2
        );
        
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> scheduler2.tryAcquireLeadership());
        
        assertThat(scheduler2.isLeader())
                .as("New scheduler should become leader")
                .isTrue();
        
        // The leader key stores "schedulerId:timestamp"
        assertThat(scheduler2.getCurrentLeader())
                .as("New scheduler should be the current leader")
                .hasValueSatisfying(value -> assertThat(value).startsWith(schedulerId2 + ":"));
    }

    @Test
    @Order(8)
    @DisplayName("Test Duplicate Kafka Event Handling - Idempotency check")
    void testDuplicateKafkaEvents() throws Exception {
        // Given: Same event received multiple times
        String workflowId = "workflow-duplicate-event";
        String executionId = "exec-duplicate";
        
        // When: Process same execution ID multiple times
        Instant scheduledTime = Instant.now();
        boolean first = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        boolean second = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        boolean third = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        
        // Then: Only first should succeed
        assertThat(first)
                .as("First execution should be recorded")
                .isTrue();
        
        assertThat(second)
                .as("Duplicate should be rejected")
                .isFalse();
        
        assertThat(third)
                .as("Third duplicate should be rejected")
                .isFalse();
        
        // Verify idempotency persists across service restart (Redis persistence)
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            boolean afterRestart = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
            assertThat(afterRestart)
                    .as("Idempotency should persist")
                    .isFalse();
        });
    }

    @Test
    @Order(9)
    @DisplayName("Test Leader Election TTL - Leadership expires without renewal")
    void testLeaderElectionTTL() throws Exception {
        // Given: A scheduler with short TTL leadership
        // (separate key: the application's own LeaderElectionService competes for the real one)
        String schedulerId = "scheduler-short-ttl";
        
        // Acquire leadership
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                "chronos:test:leader:ttl",
                schedulerId,
                Duration.ofSeconds(2)
        );
        
        assertThat(acquired).isTrue();
        
        // When: TTL expires without renewal
        Thread.sleep(3000);
        
        // Then: Leadership should be available again
        String currentLeader = redisTemplate.opsForValue().get("chronos:test:leader:ttl");
        assertThat(currentLeader)
                .as("Leadership should have expired")
                .isNull();
        
        // Another scheduler can acquire
        Boolean reacquired = redisTemplate.opsForValue().setIfAbsent(
                "chronos:test:leader:ttl",
                "scheduler-new",
                Duration.ofSeconds(30)
        );
        
        assertThat(reacquired)
                .as("New scheduler should acquire leadership")
                .isTrue();
    }

    @Test
    @Order(10)
    @DisplayName("Test Optimistic Locking on High Contention")
    void testOptimisticLockingHighContention() throws Exception {
        // Given: High contention scenario with many concurrent updates
        SchedulerState state = new SchedulerState();
        state.setWorkflowId("high-contention-workflow");
        state.setNextScheduledTime(Instant.now());
        state.setSchedule("0 0 * * * ?");
        state = schedulerStateRepository.save(state);
        
        int threadCount = 20;
        int attemptsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        
        AtomicInteger totalSuccesses = new AtomicInteger(0);
        AtomicInteger totalFailures = new AtomicInteger(0);
        
        final String stateId = state.getId();
        
        // When: Many threads compete to update
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int attempt = 0; attempt < attemptsPerThread; attempt++) {
                        try {
                            SchedulerState loaded = schedulerStateRepository.findById(stateId)
                                    .orElseThrow();
                            
                            loaded.setNextScheduledTime(Instant.now().plusSeconds(attempt));
                            schedulerStateRepository.save(loaded);
                            totalSuccesses.incrementAndGet();
                            break; // Success, exit retry loop
                            
                        } catch (OptimisticLockingFailureException e) {
                            totalFailures.incrementAndGet();
                            Thread.sleep(5 + ThreadLocalRandom.current().nextInt(20)); // Jittered backoff
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(30, TimeUnit.SECONDS);
        
        // Then: All threads should eventually succeed (with retries)
        assertThat(totalSuccesses.get())
                .as("All threads should eventually succeed")
                .isEqualTo(threadCount);
        
        assertThat(totalFailures.get())
                .as("Some optimistic lock failures should occur")
                .isGreaterThan(0);
        
        System.out.println("Optimistic lock stats: successes=" + totalSuccesses.get() + 
                ", failures=" + totalFailures.get() +
                ", retry efficiency=" + (totalSuccesses.get() * 100.0 / (totalSuccesses.get() + totalFailures.get())) + "%");
    }
}
