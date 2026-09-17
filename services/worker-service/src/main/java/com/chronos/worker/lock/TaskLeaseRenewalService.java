package com.chronos.worker.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Automatic lease renewal service for long-running tasks.
 * 
 * Problem: Task locks/leases expire after a fixed duration (e.g., 30 minutes).
 * For long-running tasks that take longer than the lock TTL, the lock expires
 * before the task completes, allowing another worker to claim the same task.
 * 
 * Solution: Background service that periodically renews leases for active tasks.
 * 
 * Features:
 * - Tracks active tasks with their lock tokens
 * - Automatically renews leases every 2 minutes (configurable)
 * - Extends lease by 3 minutes each time (configurable)
 * - Thread-safe tracking of active tasks
 * - Cleanup of completed tasks
 * 
 * Usage:
 * 1. Call registerTask() when starting task execution
 * 2. Background scheduler automatically renews the lease
 * 3. Call unregisterTask() when task completes/fails
 * 
 * Guarantees:
 * - Long-running tasks maintain their exclusive lock
 * - No race conditions from expired locks
 * - Automatic cleanup prevents memory leaks
 */
@Service
public class TaskLeaseRenewalService {
    
    private static final Logger log = LoggerFactory.getLogger(TaskLeaseRenewalService.class);
    
    private final TaskLeaseService taskLeaseService;
    private final TaskLockService taskLockService;
    
    // Track active tasks: taskId -> ActiveTask
    private final Map<String, ActiveTask> activeTasks = new ConcurrentHashMap<>();
    
    @Value("${chronos.task.lease-renewal-interval:2m}")
    private Duration renewalInterval;
    
    @Value("${chronos.task.lease-extension-duration:3m}")
    private Duration extensionDuration;
    
    @Value("${worker.id}")
    private String workerId;
    
    public TaskLeaseRenewalService(TaskLeaseService taskLeaseService, TaskLockService taskLockService) {
        this.taskLeaseService = taskLeaseService;
        this.taskLockService = taskLockService;
    }
    
    /**
     * Register a task for automatic lease renewal.
     * Call this immediately after acquiring the task lock and lease.
     * 
     * @param taskId the task identifier
     * @param lockToken the lock token proving ownership
     */
    public void registerTask(String taskId, LockToken lockToken) {
        ActiveTask activeTask = new ActiveTask(taskId, lockToken.getToken(), workerId);
        activeTasks.put(taskId, activeTask);
        
        log.info("Task registered for lease renewal: taskId={}, workerId={}, activeTasks={}",
                taskId, workerId, activeTasks.size());
    }
    
    /**
     * Unregister a task from automatic lease renewal.
     * Call this when task completes or fails.
     * 
     * @param taskId the task identifier
     */
    public void unregisterTask(String taskId) {
        ActiveTask removed = activeTasks.remove(taskId);
        
        if (removed != null) {
            log.info("Task unregistered from lease renewal: taskId={}, workerId={}, activeTasks={}",
                    taskId, workerId, activeTasks.size());
        } else {
            log.debug("Task not found in renewal registry: taskId={}", taskId);
        }
    }
    
    /**
     * Get the number of active tasks being tracked.
     * 
     * @return count of active tasks
     */
    public int getActiveTaskCount() {
        return activeTasks.size();
    }
    
    /**
     * Check if a task is currently registered for renewal.
     * 
     * @param taskId the task identifier
     * @return true if the task is registered
     */
    public boolean isTaskRegistered(String taskId) {
        return activeTasks.containsKey(taskId);
    }
    
    /**
     * Background job that renews leases for all active tasks.
     * Runs every 2 minutes by default (configurable via chronos.task.lease-renewal-interval).
     * 
     * For each active task:
     * 1. Verify we still hold the lease
     * 2. Extend the lease by 3 minutes (configurable)
     * 3. Remove from tracking if lease lost
     */
    @Scheduled(fixedDelayString = "${chronos.task.lease-renewal-interval:120000}")
    public void renewActiveTaskLeases() {
        if (activeTasks.isEmpty()) {
            log.trace("No active tasks to renew");
            return;
        }
        
        log.debug("Starting lease renewal for {} active tasks", activeTasks.size());
        
        int renewed = 0;
        int failed = 0;
        int removed = 0;
        
        for (Map.Entry<String, ActiveTask> entry : activeTasks.entrySet()) {
            String taskId = entry.getKey();
            ActiveTask activeTask = entry.getValue();
            
            try {
                // Extend the lease
                boolean extended = taskLeaseService.extendLease(
                        taskId,
                        activeTask.getLockToken(),
                        extensionDuration
                );
                
                if (extended) {
                    renewed++;
                    activeTask.recordRenewal();
                    log.debug("Lease renewed: taskId={}, renewalCount={}, extension={}",
                            taskId, activeTask.getRenewalCount(), extensionDuration);
                } else {
                    // Lease extension failed - task may have completed or been stolen
                    log.warn("Failed to renew lease: taskId={}, removing from tracking", taskId);
                    activeTasks.remove(taskId);
                    removed++;
                }
                
            } catch (Exception e) {
                log.error("Error renewing lease: taskId={}", taskId, e);
                failed++;
                
                // Check if lease still exists
                if (!taskLeaseService.hasActiveLease(taskId)) {
                    log.warn("Lease no longer exists: taskId={}, removing from tracking", taskId);
                    activeTasks.remove(taskId);
                    removed++;
                }
            }
        }
        
        log.info("Lease renewal completed: renewed={}, failed={}, removed={}, remaining={}",
                renewed, failed, removed, activeTasks.size());
    }
    
    /**
     * Clear all registered tasks.
     * Used for testing or emergency cleanup.
     */
    public void clearAll() {
        int count = activeTasks.size();
        activeTasks.clear();
        log.warn("Cleared all active task registrations: count={}", count);
    }
    
    /**
     * Get statistics about lease renewal activity.
     * 
     * @return renewal statistics
     */
    public RenewalStatistics getStatistics() {
        long totalRenewals = activeTasks.values().stream()
                .mapToLong(ActiveTask::getRenewalCount)
                .sum();
        
        return new RenewalStatistics(
                activeTasks.size(),
                totalRenewals,
                renewalInterval,
                extensionDuration
        );
    }
    
    /**
     * Represents an active task being tracked for renewal.
     */
    private static class ActiveTask {
        private final String taskId;
        private final String lockToken;
        private final String workerId;
        private final long registeredAt;
        private long renewalCount;
        private long lastRenewalAt;
        
        public ActiveTask(String taskId, String lockToken, String workerId) {
            this.taskId = taskId;
            this.lockToken = lockToken;
            this.workerId = workerId;
            this.registeredAt = System.currentTimeMillis();
            this.renewalCount = 0;
            this.lastRenewalAt = 0;
        }
        
        public String getTaskId() {
            return taskId;
        }
        
        public String getLockToken() {
            return lockToken;
        }
        
        public String getWorkerId() {
            return workerId;
        }
        
        public long getRenewalCount() {
            return renewalCount;
        }
        
        public void recordRenewal() {
            this.renewalCount++;
            this.lastRenewalAt = System.currentTimeMillis();
        }
        
        public long getLastRenewalAt() {
            return lastRenewalAt;
        }
        
        public long getRegisteredAt() {
            return registeredAt;
        }
    }
    
    /**
     * Statistics about lease renewal activity.
     */
    public static class RenewalStatistics {
        private final int activeTaskCount;
        private final long totalRenewals;
        private final Duration renewalInterval;
        private final Duration extensionDuration;
        
        public RenewalStatistics(int activeTaskCount, long totalRenewals,
                                Duration renewalInterval, Duration extensionDuration) {
            this.activeTaskCount = activeTaskCount;
            this.totalRenewals = totalRenewals;
            this.renewalInterval = renewalInterval;
            this.extensionDuration = extensionDuration;
        }
        
        public int getActiveTaskCount() {
            return activeTaskCount;
        }
        
        public long getTotalRenewals() {
            return totalRenewals;
        }
        
        public Duration getRenewalInterval() {
            return renewalInterval;
        }
        
        public Duration getExtensionDuration() {
            return extensionDuration;
        }
        
        @Override
        public String toString() {
            return String.format("RenewalStatistics{activeTaskCount=%d, totalRenewals=%d, " +
                            "renewalInterval=%s, extensionDuration=%s}",
                    activeTaskCount, totalRenewals, renewalInterval, extensionDuration);
        }
    }
}
