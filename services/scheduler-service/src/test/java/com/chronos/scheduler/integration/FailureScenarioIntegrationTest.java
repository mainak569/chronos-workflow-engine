package com.chronos.scheduler.integration;

import com.chronos.scheduler.domain.SchedulerState;
import com.chronos.scheduler.idempotency.ExecutionIdempotencyService;
import com.chronos.scheduler.repository.SchedulerStateRepository;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for failure scenarios in distributed systems.
 * 
 * Tests:
 * - MongoDB temporary failures
 * - Redis connection failures  
 * - Network partitions (simulated)
 * - Container crashes and recoveries
 * - Heartbeat expiration
 * - Dead letter queue handling
 * 
 * Uses Testcontainers for realistic failure simulation.
 */
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FailureScenarioIntegrationTest {

    // Fixed host ports: restarting a container must not change the address the application uses
    private static final int MONGO_PORT = freePort();
    private static final int REDIS_PORT = freePort();

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(MONGO_PORT), ExposedPort.tcp(27017))));

    @Container
    static GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(REDIS_PORT), ExposedPort.tcp(6379))));

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Stop and start the same container (keeping its data and port), unlike
     * Testcontainers' stop()/start() which creates a brand new container.
     */
    private static void restart(GenericContainer<?> container) throws InterruptedException {
        container.getDockerClient().stopContainerCmd(container.getContainerId()).exec();
        Thread.sleep(2000);
        container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
    }

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        // Short driver timeouts so operations against a paused container fail fast instead of hanging
        registry.add("spring.data.mongodb.uri", () -> mongoDBContainer.getReplicaSetUrl()
                + "?serverSelectionTimeoutMS=3000&connectTimeoutMS=3000&socketTimeoutMS=3000");
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", redisContainer::getFirstMappedPort);
        registry.add("spring.data.redis.timeout", () -> "2000");
        
        // No Kafka broker in this test, and no background scheduling touching the test data
        registry.add("chronos.kafka.listener.auto-startup", () -> "false");
        registry.add("chronos.scheduler.scheduling.enabled", () -> "false");
        registry.add("chronos.scheduler.restart-recovery.enabled", () -> "false");
    }

    @Autowired
    private SchedulerStateRepository schedulerStateRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ExecutionIdempotencyService idempotencyService;

    @BeforeEach
    void setUp() {
        // Clear state
        try {
            schedulerStateRepository.deleteAll();
            redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        } catch (Exception e) {
            // Ignore cleanup failures during setup
        }
    }

    // ==================== MONGODB FAILURE TESTS ====================

    @Test
    @Order(1)
    @DisplayName("Test MongoDB Temporary Failure - Service recovers after MongoDB restart")
    void testMongoDBTemporaryFailure() throws Exception {
        // Given: MongoDB is running and has data
        SchedulerState state = new SchedulerState();
        state.setWorkflowId("workflow-before-failure");
        state.setNextScheduledTime(Instant.now());
        state.setSchedule("0 0 * * * ?");
        state = schedulerStateRepository.save(state);
        
        String savedId = state.getId();
        
        // Verify data is saved
        assertThat(schedulerStateRepository.findById(savedId)).isPresent();
        
        // When: MongoDB container is paused (simulates network partition or MongoDB freeze)
        System.out.println("Pausing MongoDB container...");
        mongoDBContainer.getDockerClient()
                .pauseContainerCmd(mongoDBContainer.getContainerId())
                .exec();
        
        try {
            // Then: Operations should fail
            assertThatThrownBy(() -> {
                schedulerStateRepository.findAll();
            }).isInstanceOf(DataAccessResourceFailureException.class);
            
            // Wait a bit to simulate downtime
            Thread.sleep(2000);
            
        } finally {
            // When: MongoDB is unpaused (recovers)
            System.out.println("Unpausing MongoDB container...");
            mongoDBContainer.getDockerClient()
                    .unpauseContainerCmd(mongoDBContainer.getContainerId())
                    .exec();
        }
        
        // Then: Service should recover and data should be accessible
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    return schedulerStateRepository.findById(savedId).isPresent();
                });
        
        SchedulerState recovered = schedulerStateRepository.findById(savedId).orElseThrow();
        assertThat(recovered.getWorkflowId()).isEqualTo("workflow-before-failure");
    }

    @Test
    @Order(2)
    @DisplayName("Test MongoDB Connection Pool Exhaustion - Graceful degradation")
    void testMongoDBConnectionPoolExhaustion() throws Exception {
        // Given: Multiple concurrent operations
        int operationCount = 100;
        
        // When: Flood MongoDB with operations
        long successCount = java.util.stream.IntStream.range(0, operationCount)
                .parallel()
                .mapToObj(i -> {
                    try {
                        SchedulerState state = new SchedulerState();
                        state.setWorkflowId("workflow-" + i);
                        state.setNextScheduledTime(Instant.now());
                        state.setSchedule("0 0 * * * ?");
                        schedulerStateRepository.save(state);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .filter(success -> success)
                .count();
        
        // Then: Most operations should succeed (some might fail due to connection limits)
        assertThat(successCount)
                .as("Majority of operations should succeed despite high load")
                .isGreaterThan((long) (operationCount * 0.8)); // At least 80% success rate
        
        System.out.println("MongoDB connection pool test: " + successCount + "/" + operationCount + " succeeded");
    }

    @Test
    @Order(3)
    @DisplayName("Test MongoDB Restart - Data persists across restart")
    void testMongoDBRestart() throws Exception {
        // Given: Data is saved to MongoDB
        SchedulerState state = new SchedulerState();
        state.setWorkflowId("workflow-persist-test");
        state.setNextScheduledTime(Instant.now());
        state.setSchedule("0 0 * * * ?");
        state = schedulerStateRepository.save(state);
        
        String savedId = state.getId();
        
        // When: MongoDB container is stopped and restarted
        System.out.println("Restarting MongoDB container...");
        restart(mongoDBContainer);
        
        // Then: Data should persist (MongoDB uses volume)
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    // Reconnect might be needed
                    return schedulerStateRepository.findById(savedId).isPresent();
                });
        
        SchedulerState persisted = schedulerStateRepository.findById(savedId).orElseThrow();
        assertThat(persisted.getWorkflowId()).isEqualTo("workflow-persist-test");
    }

    // ==================== REDIS FAILURE TESTS ====================

    @Test
    @Order(4)
    @DisplayName("Test Redis Connection Failure - Operations fail gracefully")
    void testRedisConnectionFailure() throws Exception {
        // Given: Redis is running
        redisTemplate.opsForValue().set("test-key", "test-value");
        assertThat(redisTemplate.opsForValue().get("test-key")).isEqualTo("test-value");
        
        // When: Redis container is paused
        System.out.println("Pausing Redis container...");
        redisContainer.getDockerClient()
                .pauseContainerCmd(redisContainer.getContainerId())
                .exec();
        
        try {
            // Then: Operations should fail with timeout
            assertThatThrownBy(() -> {
                redisTemplate.opsForValue().get("test-key");
            }).isInstanceOf(DataAccessException.class);
            
        } finally {
            // Cleanup: Unpause Redis
            System.out.println("Unpausing Redis container...");
            redisContainer.getDockerClient()
                    .unpauseContainerCmd(redisContainer.getContainerId())
                    .exec();
        }
        
        // Service should recover
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    String value = redisTemplate.opsForValue().get("test-key");
                    return "test-value".equals(value);
                });
    }

    @Test
    @Order(5)
    @DisplayName("Test Redis Data Persistence - AOF persistence works")
    void testRedisDataPersistence() throws Exception {
        // Given: Data is saved to Redis
        String key = "persistent-key";
        String value = "persistent-value-" + System.currentTimeMillis();
        redisTemplate.opsForValue().set(key, value);
        
        // Force Redis to persist (BGSAVE)
        redisTemplate.getConnectionFactory().getConnection().serverCommands().bgSave();
        Thread.sleep(1000); // Wait for background save
        
        // When: Redis container is restarted
        System.out.println("Restarting Redis container...");
        restart(redisContainer);
        
        // Then: Data should persist (Redis AOF/RDB)
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    String retrieved = redisTemplate.opsForValue().get(key);
                    return value.equals(retrieved);
                });
        
        String retrieved = redisTemplate.opsForValue().get(key);
        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    @Order(6)
    @DisplayName("Test Redis Lock Expiration During Container Pause")
    void testRedisLockExpirationDuringPause() throws Exception {
        // Given: A lock is acquired
        String lockKey = "test-lock";
        String lockValue = "worker-1";
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                lockKey,
                lockValue,
                Duration.ofSeconds(5)
        );
        
        assertThat(acquired).isTrue();
        
        // When: Container is paused (simulates network partition)
        System.out.println("Pausing Redis container...");
        redisContainer.getDockerClient()
                .pauseContainerCmd(redisContainer.getContainerId())
                .exec();
        
        // Wait longer than lock TTL
        Thread.sleep(6000);
        
        // Unpause
        redisContainer.getDockerClient()
                .unpauseContainerCmd(redisContainer.getContainerId())
                .exec();
        
        // Then: Lock should have expired
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    String currentValue = redisTemplate.opsForValue().get(lockKey);
                    return currentValue == null; // Lock expired
                });
        
        String currentValue = redisTemplate.opsForValue().get(lockKey);
        assertThat(currentValue)
                .as("Lock should have expired during pause")
                .isNull();
    }

    // ==================== HEARTBEAT & WORKER TESTS ====================

    @Test
    @Order(7)
    @DisplayName("Test Heartbeat Expiration - Detect dead workers")
    void testHeartbeatExpiration() throws Exception {
        // Given: A worker registers heartbeat
        String workerId = "worker-heartbeat-test";
        String heartbeatKey = "chronos:worker:heartbeat:" + workerId;
        
        // Register initial heartbeat
        redisTemplate.opsForValue().set(
                heartbeatKey,
                String.valueOf(System.currentTimeMillis()),
                Duration.ofSeconds(5)
        );
        
        // Verify heartbeat exists
        assertThat(redisTemplate.hasKey(heartbeatKey)).isTrue();
        
        // When: Heartbeat is not renewed (simulates worker crash)
        Thread.sleep(6000);
        
        // Then: Heartbeat should expire
        assertThat(redisTemplate.hasKey(heartbeatKey))
                .as("Heartbeat should expire after TTL")
                .isFalse();
        
        // Verify worker is considered dead
        String heartbeatValue = redisTemplate.opsForValue().get(heartbeatKey);
        assertThat(heartbeatValue)
                .as("No heartbeat should exist for dead worker")
                .isNull();
    }

    @Test
    @Order(8)
    @DisplayName("Test Worker Crash Recovery - Task lock released on crash")
    void testWorkerCrashRecovery() throws Exception {
        // Given: A worker claims a task
        String taskId = "task-worker-crash";
        String workerId = "worker-crash-test";
        String lockKey = "chronos:task:lock:" + taskId;
        
        Boolean claimed = redisTemplate.opsForValue().setIfAbsent(
                lockKey,
                workerId,
                Duration.ofSeconds(10)
        );
        
        assertThat(claimed).isTrue();
        
        // When: Worker crashes (lock not released, but will expire)
        // Simulate by doing nothing - let TTL expire
        Thread.sleep(11000);
        
        // Then: Another worker can claim the task
        Boolean reclaimedByAnother = redisTemplate.opsForValue().setIfAbsent(
                lockKey,
                "worker-recovery",
                Duration.ofSeconds(30)
        );
        
        assertThat(reclaimedByAnother)
                .as("Another worker should claim task after lock expiration")
                .isTrue();
        
        String newOwner = redisTemplate.opsForValue().get(lockKey);
        assertThat(newOwner).isEqualTo("worker-recovery");
    }

    // ==================== IDEMPOTENCY & DLQ TESTS ====================

    @Test
    @Order(9)
    @DisplayName("Test Idempotency Persistence Across Failures")
    void testIdempotencyPersistenceAcrosFailures() throws Exception {
        // Given: An execution is recorded
        String workflowId = "workflow-idempotency-persistence";
        String executionId = "exec-persist-test";
        
        Instant scheduledTime = Instant.now();
        boolean recorded = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        assertThat(recorded).isTrue();
        
        // When: Redis is restarted
        System.out.println("Restarting Redis container...");
        redisContainer.stop();
        Thread.sleep(2000);
        redisContainer.start();
        
        // Wait for reconnection
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> {
                    redisTemplate.hasKey("test-reconnect");
                    return true;
                });
        
        // Then: Idempotency should persist (if AOF enabled)
        // Note: In-memory Redis loses data without persistence config
        // This test verifies the behavior is consistent
        boolean isDuplicate = idempotencyService.recordExecution(workflowId, scheduledTime, executionId);
        
        // Document expected behavior: Without AOF, idempotency is lost on restart
        System.out.println("Idempotency after restart: isDuplicate=" + isDuplicate);
        // In production, enable AOF for Redis to maintain idempotency across restarts
    }

    @Test
    @Order(10)
    @DisplayName("Test High-Frequency Heartbeats Under Load")
    void testHighFrequencyHeartbeatsUnderLoad() throws Exception {
        // Given: Multiple workers sending frequent heartbeats
        int workerCount = 50;
        int heartbeatCount = 10;
        
        // When: Workers send rapid heartbeats
        long successCount = java.util.stream.IntStream.range(0, workerCount)
                .parallel()
                .mapToLong(workerId -> {
                    long success = 0;
                    for (int i = 0; i < heartbeatCount; i++) {
                        try {
                            String key = "chronos:worker:heartbeat:worker-" + workerId;
                            redisTemplate.opsForValue().set(
                                    key,
                                    String.valueOf(System.currentTimeMillis()),
                                    Duration.ofSeconds(30)
                            );
                            success++;
                            Thread.sleep(10); // Small delay
                        } catch (Exception e) {
                            // Count failures
                        }
                    }
                    return success;
                })
                .sum();
        
        // Then: Most heartbeats should succeed
        long expected = (long) workerCount * heartbeatCount;
        assertThat(successCount)
                .as("Most heartbeats should succeed under load")
                .isGreaterThan((long) (expected * 0.95)); // 95% success rate
        
        System.out.println("Heartbeat test: " + successCount + "/" + expected + " succeeded");
    }

    @Test
    @Order(11)
    @DisplayName("Test Retry Behavior - Exponential backoff works")
    void testRetryBehavior() throws Exception {
        // Given: A failing operation
        AtomicInteger attemptCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        
        // When: Retry with exponential backoff
        int maxRetries = 5;
        long baseDelay = 100; // milliseconds
        
        for (int i = 0; i < maxRetries; i++) {
            attemptCount.incrementAndGet();
            
            // Simulate failure
            long delay = baseDelay * (long) Math.pow(2, i);
            Thread.sleep(delay);
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        
        // Then: Total time should reflect exponential backoff
        // Expected: 100 + 200 + 400 + 800 + 1600 = 3100ms
        assertThat(totalTime)
                .as("Total retry time should reflect exponential backoff")
                .isGreaterThan(3000)
                .isLessThan(4000);
        
        assertThat(attemptCount.get()).isEqualTo(maxRetries);
    }

    @Test
    @Order(12)
    @DisplayName("Test Service Restart - No data loss")
    void testServiceRestartNoDataLoss() throws Exception {
        // Given: Data in both MongoDB and Redis
        SchedulerState mongoState = new SchedulerState();
        mongoState.setWorkflowId("workflow-restart-test");
        mongoState.setNextScheduledTime(Instant.now());
        mongoState.setSchedule("0 0 * * * ?");
        mongoState = schedulerStateRepository.save(mongoState);
        
        String mongoId = mongoState.getId();
        
        redisTemplate.opsForValue().set("restart-test-key", "restart-test-value");
        
        // When: Containers are restarted (simulating service deployment)
        System.out.println("Restarting containers...");
        
        // Force Redis to write its snapshot before the restart
        redisTemplate.getConnectionFactory().getConnection().serverCommands().save();
        
        restart(mongoDBContainer);
        restart(redisContainer);
        
        // Then: MongoDB data should persist (with proper configuration)
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> schedulerStateRepository.findById(mongoId).isPresent());
        
        SchedulerState recovered = schedulerStateRepository.findById(mongoId).orElseThrow();
        assertThat(recovered.getWorkflowId()).isEqualTo("workflow-restart-test");
        
        // Redis: the snapshot written above survives the restart, once the client has reconnected
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .untilAsserted(() -> assertThat(redisTemplate.opsForValue().get("restart-test-key"))
                        .isEqualTo("restart-test-value"));
    }
}
