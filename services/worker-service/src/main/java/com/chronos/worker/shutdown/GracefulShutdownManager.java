package com.chronos.worker.shutdown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages graceful shutdown of worker service.
 * 
 * Problem: Workers that shutdown immediately may leave tasks in inconsistent state.
 * Tasks that were being executed get abandoned, locks held, but work incomplete.
 * 
 * Solution: Graceful shutdown sequence:
 * 1. Stop accepting new tasks (set shutdown flag)
 * 2. Wait for in-flight tasks to complete (with timeout)
 * 3. Clean up resources and exit
 * 
 * Features:
 * - Track in-flight tasks by task ID
 * - Configurable shutdown timeout
 * - Force shutdown after timeout
 * - Thread-safe task tracking
 * - Shutdown statistics logging
 * 
 * Usage:
 * - Call registerTask() when starting task execution
 * - Call completeTask() when task finishes (success or failure)
 * - Spring automatically calls shutdown() on @PreDestroy
 */
@Component
public class GracefulShutdownManager {
    
    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownManager.class);
    
    // Track in-flight tasks: taskId -> start timestamp
    private final Map<String, Long> inFlightTasks = new ConcurrentHashMap<>();
    
    // Shutdown state
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicInteger totalTasksProcessed = new AtomicInteger(0);
    
    @Value("${chronos.worker.shutdown-timeout:60s}")
    private Duration shutdownTimeout;
    
    @Value("${worker.id}")
    private String workerId;
    
    /**
     * Check if shutdown has been requested.
     * 
     * @return true if shutdown in progress
     */
    public boolean isShutdownRequested() {
        return shutdownRequested.get();
    }
    
    /**
     * Register a task as in-flight.
     * Should be called at the start of task execution.
     * 
     * @param taskId task identifier
     * @return true if registered (shutdown not requested), false if shutdown in progress
     */
    public boolean registerTask(String taskId) {
        if (shutdownRequested.get()) {
            log.warn("Shutdown in progress, rejecting task: {}", taskId);
            return false;
        }
        
        inFlightTasks.put(taskId, System.currentTimeMillis());
        log.debug("Task registered: {}, inFlight={}", taskId, inFlightTasks.size());
        return true;
    }
    
    /**
     * Mark a task as complete.
     * Should be called when task finishes (success or failure).
     * 
     * @param taskId task identifier
     */
    public void completeTask(String taskId) {
        Long startTime = inFlightTasks.remove(taskId);
        
        if (startTime != null) {
            totalTasksProcessed.incrementAndGet();
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Task completed: {}, duration={}ms, remaining={}", 
                    taskId, duration, inFlightTasks.size());
        } else {
            log.warn("Task not found in registry: {}", taskId);
        }
    }
    
    /**
     * Get the number of in-flight tasks.
     * 
     * @return count of tasks currently being processed
     */
    public int getInFlightTaskCount() {
        return inFlightTasks.size();
    }
    
    /**
     * Get the total number of tasks processed.
     * 
     * @return total task count
     */
    public int getTotalTasksProcessed() {
        return totalTasksProcessed.get();
    }
    
    /**
     * Initiate graceful shutdown.
     * Waits for in-flight tasks to complete up to configured timeout.
     * 
     * Called automatically by Spring on application shutdown.
     */
    @PreDestroy
    public void shutdown() {
        if (!shutdownRequested.compareAndSet(false, true)) {
            log.debug("Shutdown already in progress");
            return;
        }
        
        log.info("=== Graceful Shutdown Initiated ===");
        log.info("Worker: {}", workerId);
        log.info("In-flight tasks: {}", inFlightTasks.size());
        log.info("Total tasks processed: {}", totalTasksProcessed.get());
        log.info("Shutdown timeout: {}", shutdownTimeout);
        
        if (inFlightTasks.isEmpty()) {
            log.info("No in-flight tasks, shutdown immediate");
            return;
        }
        
        // Wait for in-flight tasks to complete
        long timeoutMillis = shutdownTimeout.toMillis();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeoutMillis;
        
        log.info("Waiting for {} in-flight tasks to complete (timeout: {}ms)", 
                inFlightTasks.size(), timeoutMillis);
        
        while (!inFlightTasks.isEmpty() && System.currentTimeMillis() < endTime) {
            try {
                // Check every second
                Thread.sleep(1000);
                
                long elapsed = System.currentTimeMillis() - startTime;
                long remaining = endTime - System.currentTimeMillis();
                
                log.info("Shutdown progress: inFlight={}, elapsed={}s, remaining={}s",
                        inFlightTasks.size(), elapsed / 1000, remaining / 1000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Shutdown wait interrupted");
                break;
            }
        }
        
        // Check final state
        if (inFlightTasks.isEmpty()) {
            long shutdownDuration = System.currentTimeMillis() - startTime;
            log.info("=== Graceful Shutdown Complete ===");
            log.info("All tasks completed successfully");
            log.info("Shutdown duration: {}ms", shutdownDuration);
        } else {
            log.error("=== Forced Shutdown ===");
            log.error("Timeout reached with {} tasks still in-flight", inFlightTasks.size());
            log.error("Abandoned tasks: {}", inFlightTasks.keySet());
            
            // Log details of abandoned tasks
            for (Map.Entry<String, Long> entry : inFlightTasks.entrySet()) {
                long taskDuration = System.currentTimeMillis() - entry.getValue();
                log.error("Abandoned task: {}, running for {}s", 
                        entry.getKey(), taskDuration / 1000);
            }
        }
    }
    
    /**
     * Get shutdown statistics.
     * 
     * @return shutdown statistics
     */
    public ShutdownStatistics getStatistics() {
        return new ShutdownStatistics(
                shutdownRequested.get(),
                inFlightTasks.size(),
                totalTasksProcessed.get(),
                shutdownTimeout
        );
    }
    
    /**
     * Statistics about shutdown state.
     */
    public static class ShutdownStatistics {
        private final boolean shutdownRequested;
        private final int inFlightTasks;
        private final int totalTasksProcessed;
        private final Duration shutdownTimeout;
        
        public ShutdownStatistics(boolean shutdownRequested, int inFlightTasks,
                                 int totalTasksProcessed, Duration shutdownTimeout) {
            this.shutdownRequested = shutdownRequested;
            this.inFlightTasks = inFlightTasks;
            this.totalTasksProcessed = totalTasksProcessed;
            this.shutdownTimeout = shutdownTimeout;
        }
        
        public boolean isShutdownRequested() {
            return shutdownRequested;
        }
        
        public int getInFlightTasks() {
            return inFlightTasks;
        }
        
        public int getTotalTasksProcessed() {
            return totalTasksProcessed;
        }
        
        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }
        
        @Override
        public String toString() {
            return String.format("ShutdownStatistics{shutdownRequested=%s, inFlight=%d, " +
                            "totalProcessed=%d, timeout=%s}",
                    shutdownRequested, inFlightTasks, totalTasksProcessed, shutdownTimeout);
        }
    }
}
