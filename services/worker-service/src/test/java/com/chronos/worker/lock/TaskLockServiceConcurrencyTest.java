package com.chronos.worker.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Concurrency tests for TaskLockService demonstrating distributed locking behavior.
 * 
 * These tests simulate multiple workers attempting to claim the same task simultaneously.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("TaskLockService Concurrency Tests")
class TaskLockServiceConcurrencyTest {
    
    @Mock
    private DistributedLock distributedLock;
    
    private TaskLockService taskLockService;
    
    @BeforeEach
    void setUp() {
        taskLockService = new TaskLockService(distributedLock);
    }
    
    @Test
    @DisplayName("Only one worker should successfully claim a task when multiple workers compete")
    void onlyOneWorkerShouldClaimTask() {
        // Given: 5 workers trying to claim the same task
        String taskId = "task-123";
        int workerCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(workerCount);
        
        List<String> successfulWorkers = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        // Simulate Redis: first call succeeds, rest fail
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true)  // First worker succeeds
                .thenReturn(false) // Rest fail
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false);
        
        // When: Multiple workers attempt to claim simultaneously
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        
        for (int i = 0; i < workerCount; i++) {
            final String workerId = "worker-" + i;
            executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();
                    
                    // Attempt to claim the task
                    Optional<LockToken> token = taskLockService.claimTask(taskId, workerId);
                    
                    if (token.isPresent()) {
                        successfulWorkers.add(workerId);
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all to complete
        try {
            boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        
        // Then: Only one worker should succeed
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(failureCount.get()).isEqualTo(4);
        assertThat(successfulWorkers).hasSize(1);
    }
    
    @Test
    @DisplayName("Second worker should succeed only after first worker releases the lock")
    void secondWorkerSucceedsAfterRelease() throws Exception {
        // Given: Task locked by worker-1
        String taskId = "task-456";
        String worker1 = "worker-1";
        String worker2 = "worker-2";
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true)   // Worker 1 acquires
                .thenReturn(false)  // Worker 2 fails (lock held)
                .thenReturn(true);  // Worker 2 succeeds (after release)
        
        when(distributedLock.release(anyString(), anyString()))
                .thenReturn(true);    // Release succeeds
        
        when(distributedLock.getLockHolder(anyString()))
                .thenReturn(Optional.of("worker-1:uuid1"))
                .thenReturn(Optional.empty());  // After release
        
        // When: Worker 1 claims the task
        Optional<LockToken> token1 = taskLockService.claimTask(taskId, worker1);
        assertThat(token1).isPresent();
        
        // Worker 2 tries to claim (should fail)
        Optional<LockToken> token2Attempt1 = taskLockService.claimTask(taskId, worker2);
        assertThat(token2Attempt1).isEmpty();
        
        // Worker 1 releases the lock
        boolean released = taskLockService.releaseTask(taskId, token1.get());
        assertThat(released).isTrue();
        
        // Worker 2 tries again (should succeed)
        Optional<LockToken> token2Attempt2 = taskLockService.claimTask(taskId, worker2);
        
        // Then: Worker 2 should now succeed
        assertThat(token2Attempt2).isPresent();
        assertThat(token2Attempt2.get().getWorkerId()).isEqualTo(worker2);
    }
    
    @Test
    @DisplayName("Worker cannot release a lock owned by another worker")
    void workerCannotReleaseOtherWorkersLock() {
        // Given: Worker 1 holds the lock
        String taskId = "task-789";
        String worker1 = "worker-1";
        String worker2 = "worker-2";
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);
        
        when(distributedLock.release(anyString(), anyString()))
                .thenReturn(false);  // Release fails (token mismatch)
        
        // When: Worker 1 claims the task
        Optional<LockToken> token1 = taskLockService.claimTask(taskId, worker1);
        assertThat(token1).isPresent();
        
        // Worker 2 creates a fake token and tries to release
        LockToken fakeToken = LockToken.create(worker2);
        boolean released = taskLockService.releaseTask(taskId, fakeToken);
        
        // Then: Release should fail (wrong owner)
        assertThat(released).isFalse();
    }
    
    @Test
    @DisplayName("Lock tokens are unique even for same worker")
    void lockTokensAreUnique() {
        // Given: Same worker claims twice
        String workerId = "worker-1";
        
        // When: Create two tokens
        LockToken token1 = LockToken.create(workerId);
        LockToken token2 = LockToken.create(workerId);
        
        // Then: Tokens should be different (different UUIDs)
        assertThat(token1.getToken()).isNotEqualTo(token2.getToken());
        assertThat(token1.getWorkerId()).isEqualTo(token2.getWorkerId());
        assertThat(token1.getUuid()).isNotEqualTo(token2.getUuid());
    }
    
    @Test
    @DisplayName("Lock expiration allows another worker to claim the task")
    void lockExpirationAllowsNewClaim() {
        // Given: Task locked by worker-1
        String taskId = "task-expired";
        String worker1 = "worker-1";
        String worker2 = "worker-2";
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true)   // Worker 1 acquires
                .thenReturn(false)  // Worker 2 fails (lock still held)
                .thenReturn(true);  // Worker 2 succeeds (after expiry)
        
        when(distributedLock.getLockHolder(anyString()))
                .thenReturn(Optional.of("worker-1:uuid1"))
                .thenReturn(Optional.empty());  // Simulates lock expiry
        
        // When: Worker 1 claims the task
        Optional<LockToken> token1 = taskLockService.claimTask(taskId, worker1);
        assertThat(token1).isPresent();
        
        // Worker 2 tries immediately (fails - lock held)
        Optional<LockToken> token2Attempt1 = taskLockService.claimTask(taskId, worker2);
        assertThat(token2Attempt1).isEmpty();
        
        // Simulate lock expiry (Redis TTL expired)
        // Worker 2 tries again
        Optional<LockToken> token2Attempt2 = taskLockService.claimTask(taskId, worker2);
        
        // Then: Worker 2 should succeed after expiry
        assertThat(token2Attempt2).isPresent();
        assertThat(token2Attempt2.get().getWorkerId()).isEqualTo(worker2);
    }
    
    @Test
    @DisplayName("Multiple workers can claim different tasks simultaneously")
    void multipleWorkersCanClaimDifferentTasks() throws Exception {
        // Given: 3 workers, 3 different tasks
        int workerCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(workerCount);
        
        List<String> successfulClaims = new CopyOnWriteArrayList<>();
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);  // All succeed (different tasks)
        
        // When: Each worker claims a different task
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        
        for (int i = 0; i < workerCount; i++) {
            final String workerId = "worker-" + i;
            final String taskId = "task-" + i;
            
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    Optional<LockToken> token = taskLockService.claimTask(taskId, workerId);
                    
                    if (token.isPresent()) {
                        successfulClaims.add(workerId + ":" + taskId);
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Then: All workers should succeed (different locks)
        assertThat(completed).isTrue();
        assertThat(successfulClaims).hasSize(3);
    }
    
    @Test
    @DisplayName("Lock can be extended by the owner")
    void lockCanBeExtendedByOwner() {
        // Given: Task locked by worker-1
        String taskId = "task-extend";
        String workerId = "worker-1";
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);
        
        when(distributedLock.extend(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);  // Extend succeeds
        
        // When: Worker claims and extends
        Optional<LockToken> token = taskLockService.claimTask(taskId, workerId);
        assertThat(token).isPresent();
        
        boolean extended = taskLockService.extendLock(taskId, token.get(), Duration.ofMinutes(5));
        
        // Then: Extension should succeed
        assertThat(extended).isTrue();
    }
    
    @Test
    @DisplayName("Lock cannot be extended with wrong token")
    void lockCannotBeExtendedWithWrongToken() {
        // Given: Task locked by worker-1
        String taskId = "task-extend-fail";
        String worker1 = "worker-1";
        String worker2 = "worker-2";
        
        when(distributedLock.tryAcquire(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);
        
        when(distributedLock.extend(anyString(), anyString(), any(Duration.class)))
                .thenReturn(false);  // Extend fails (wrong token)
        
        // When: Worker 1 claims, Worker 2 tries to extend
        Optional<LockToken> token1 = taskLockService.claimTask(taskId, worker1);
        assertThat(token1).isPresent();
        
        LockToken fakeToken = LockToken.create(worker2);
        boolean extended = taskLockService.extendLock(taskId, fakeToken, Duration.ofMinutes(5));
        
        // Then: Extension should fail
        assertThat(extended).isFalse();
    }
}
