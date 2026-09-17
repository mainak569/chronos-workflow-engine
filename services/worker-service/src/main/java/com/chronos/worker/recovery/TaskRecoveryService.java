package com.chronos.worker.recovery;

import com.chronos.worker.config.KafkaTopics;
import com.chronos.worker.event.TaskReadyEvent;
import com.chronos.worker.lock.TaskLease;
import com.chronos.worker.lock.TaskLeaseService;
import com.chronos.worker.lock.TaskLockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for detecting and recovering failed/abandoned tasks.
 * 
 * Recovery Process:
 * 1. Periodically scan for expired leases (tasks whose workers have crashed)
 * 2. Force-release the expired lock to allow reassignment
 * 3. Delete the expired lease to clean up metadata
 * 4. Republish TaskReadyEvent to trigger reassignment by available workers
 * 
 * This ensures tasks don't get stuck when workers crash or become unresponsive.
 * 
 * Configuration:
 * - chronos.recovery.scan-interval: how often to scan for expired leases (default: 30s)
 * - chronos.recovery.enabled: enable/disable recovery (default: true)
 * 
 * Guarantees:
 * - At-least-once task execution (may execute multiple times if worker crashes)
 * - Tasks will eventually be completed or moved to DLQ
 * - No deadlocks from crashed workers
 * 
 * Limitations:
 * - Race conditions possible if lease expires while worker is still executing
 * - Multiple recovery instances could republish the same task (Kafka dedup handles this)
 */
@Service
public class TaskRecoveryService {
    
    private static final Logger log = LoggerFactory.getLogger(TaskRecoveryService.class);
    
    private final TaskLeaseService taskLeaseService;
    private final TaskLockService taskLockService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    public TaskRecoveryService(
            TaskLeaseService taskLeaseService,
            TaskLockService taskLockService,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.taskLeaseService = taskLeaseService;
        this.taskLockService = taskLockService;
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Scheduled task that scans for expired leases and triggers recovery.
     * Runs every 30 seconds by default.
     */
    @Scheduled(
            fixedDelayString = "${chronos.recovery.scan-interval:30000}",
            initialDelayString = "${chronos.recovery.initial-delay:60000}"
    )
    public void scanAndRecover() {
        log.debug("Starting expired lease scan");
        
        List<TaskLease> expiredLeases = taskLeaseService.findExpiredLeases();
        
        if (expiredLeases.isEmpty()) {
            log.debug("No expired leases found");
            return;
        }
        
        log.info("Found {} expired leases, starting recovery", expiredLeases.size());
        
        int recovered = 0;
        int failed = 0;
        
        for (TaskLease lease : expiredLeases) {
            try {
                recoverTask(lease);
                recovered++;
            } catch (Exception e) {
                log.error("Failed to recover task: taskId={}, executionId={}, workerId={}",
                        lease.getTaskId(), lease.getExecutionId(), lease.getWorkerId(), e);
                failed++;
            }
        }
        
        log.info("Recovery scan complete: recovered={}, failed={}", recovered, failed);
    }
    
    /**
     * Recover a single task from an expired lease.
     * 
     * Steps:
     * 1. Force-release the lock (allow other workers to claim)
     * 2. Delete the lease (clean up metadata)
     * 3. Republish TaskReadyEvent (trigger reassignment)
     * 
     * @param lease the expired lease to recover
     */
    public void recoverTask(TaskLease lease) {
        String taskId = lease.getTaskId();
        String executionId = lease.getExecutionId();
        String workerId = lease.getWorkerId();
        
        log.info("Recovering task: taskId={}, executionId={}, workerId={}, attemptNumber={}",
                taskId, executionId, workerId, lease.getAttemptNumber());
        
        // Step 1: Force-release the lock
        boolean lockReleased = taskLockService.forceRelease(taskId);
        if (lockReleased) {
            log.info("Force-released lock for task: taskId={}", taskId);
        } else {
            log.warn("Lock already released or expired for task: taskId={}", taskId);
        }
        
        // Step 2: Force-release the lease
        boolean leaseReleased = taskLeaseService.forceReleaseLease(taskId);
        if (leaseReleased) {
            log.info("Force-released lease for task: taskId={}", taskId);
        } else {
            log.warn("Lease already released for task: taskId={}", taskId);
        }
        
        // Step 3: Republish TaskReadyEvent to trigger reassignment
        republishTaskReadyEvent(lease);
        
        log.info("Task recovery complete: taskId={}, executionId={}", taskId, executionId);
    }
    
    /**
     * Republish a TaskReadyEvent to trigger task reassignment.
     * 
     * Creates a new event with:
     * - New eventId and timestamp (fresh event)
     * - Original task metadata (executionId, taskId, taskType)
     * - Same correlation ID (for tracing)
     * 
     * @param lease the expired lease containing task metadata
     */
    private void republishTaskReadyEvent(TaskLease lease) {
        TaskReadyEvent event = new TaskReadyEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setCorrelationId(lease.getExecutionId()); // Use executionId for correlation
        event.setTimestamp(Instant.now());
        event.setExecutionId(lease.getExecutionId());
        event.setTaskId(lease.getTaskId());
        
        // Note: We don't have taskType, configuration, timeoutMs, maxRetries from the lease
        // These should ideally be fetched from the TaskExecution in the database
        // For now, we'll publish a minimal event and let the consumer fetch details
        // TODO: Enhance TaskLease to store taskType or fetch from DB before republishing
        
        log.info("Republishing TaskReadyEvent: taskId={}, executionId={}, eventId={}",
                lease.getTaskId(), lease.getExecutionId(), event.getEventId());
        
        kafkaTemplate.send(KafkaTopics.TASK_READY, lease.getTaskId(), event);
    }
    
    /**
     * Manually trigger recovery for a specific task (admin operation).
     * 
     * @param taskId the task identifier
     * @return true if recovery was triggered, false if no lease found
     */
    public boolean recoverTaskById(String taskId) {
        TaskLease lease = taskLeaseService.getLease(taskId);
        
        if (lease == null) {
            log.warn("Cannot recover task: no lease found. taskId={}", taskId);
            return false;
        }
        
        log.info("Manual recovery triggered for task: taskId={}", taskId);
        recoverTask(lease);
        return true;
    }
    
    /**
     * Get recovery statistics for monitoring.
     * 
     * @return recovery stats (count of expired leases awaiting recovery)
     */
    public RecoveryStats getStats() {
        List<TaskLease> expiredLeases = taskLeaseService.findExpiredLeases();
        
        return new RecoveryStats(
                expiredLeases.size(),
                Instant.now()
        );
    }
    
    /**
     * Recovery statistics for monitoring and alerting.
     */
    public static class RecoveryStats {
        private final int expiredLeaseCount;
        private final Instant timestamp;
        
        public RecoveryStats(int expiredLeaseCount, Instant timestamp) {
            this.expiredLeaseCount = expiredLeaseCount;
            this.timestamp = timestamp;
        }
        
        public int getExpiredLeaseCount() {
            return expiredLeaseCount;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("RecoveryStats{expiredLeaseCount=%d, timestamp=%s}",
                    expiredLeaseCount, timestamp);
        }
    }
}
