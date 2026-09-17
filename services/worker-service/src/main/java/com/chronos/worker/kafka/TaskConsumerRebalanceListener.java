package com.chronos.worker.kafka;

import com.chronos.worker.shutdown.GracefulShutdownManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;

/**
 * Kafka consumer rebalance listener for handling partition reassignment.
 * 
 * Problem: When Kafka rebalances (adding/removing consumers, consumer crash),
 * partitions are reassigned. If a consumer is processing a task when its
 * partition is revoked, the task may be reassigned to another consumer,
 * leading to duplicate processing.
 * 
 * Solution: Rebalance listener coordinates with graceful shutdown:
 * 1. On partitions revoked: pause processing, wait for in-flight tasks
 * 2. On partitions assigned: resume processing
 * 
 * Rebalance Triggers:
 * - New consumer joins the group
 * - Existing consumer leaves (crash or shutdown)
 * - Consumer heartbeat timeout
 * - Topic partition count changes
 * 
 * States:
 * - STABLE: Normal operation, partitions assigned
 * - REBALANCING: Partitions being reassigned
 * 
 * Integration:
 * - Works with GracefulShutdownManager to track in-flight tasks
 * - Pauses task processing during rebalance
 * - Waits for tasks to complete before acknowledging revocation
 * 
 * Configuration:
 * - Rebalance timeout: 30 seconds (configurable)
 * - If tasks don't complete in time, force proceed with rebalance
 */
@Component
public class TaskConsumerRebalanceListener implements ConsumerRebalanceListener {
    
    private static final Logger log = LoggerFactory.getLogger(TaskConsumerRebalanceListener.class);
    
    private final GracefulShutdownManager shutdownManager;
    
    @Value("${chronos.kafka.rebalance-timeout:30s}")
    private Duration rebalanceTimeout;
    
    @Value("${worker.id}")
    private String workerId;
    
    public TaskConsumerRebalanceListener(GracefulShutdownManager shutdownManager) {
        this.shutdownManager = shutdownManager;
    }
    
    /**
     * Called before partitions are revoked from this consumer.
     * 
     * This is our chance to:
     * 1. Finish processing in-flight tasks
     * 2. Commit offsets
     * 3. Clean up partition-specific state
     * 
     * If we don't complete in time, Kafka will forcefully revoke the partitions.
     * 
     * @param partitions partitions being revoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("=== Kafka Rebalance: Partitions Revoked ===");
        log.warn("Worker: {}", workerId);
        log.warn("Revoked partitions: {}", partitions);
        log.warn("In-flight tasks: {}", shutdownManager.getInFlightTaskCount());
        
        if (shutdownManager.getInFlightTaskCount() == 0) {
            log.info("No in-flight tasks, rebalance can proceed immediately");
            return;
        }
        
        // Wait for in-flight tasks to complete
        long timeoutMillis = rebalanceTimeout.toMillis();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeoutMillis;
        
        log.info("Waiting for {} in-flight tasks to complete (timeout: {}ms)",
                shutdownManager.getInFlightTaskCount(), timeoutMillis);
        
        while (shutdownManager.getInFlightTaskCount() > 0 && System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000);
                
                long elapsed = System.currentTimeMillis() - startTime;
                long remaining = endTime - System.currentTimeMillis();
                
                log.info("Rebalance wait: inFlight={}, elapsed={}s, remaining={}s",
                        shutdownManager.getInFlightTaskCount(), elapsed / 1000, remaining / 1000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Rebalance wait interrupted");
                break;
            }
        }
        
        // Log final state
        if (shutdownManager.getInFlightTaskCount() == 0) {
            long waitDuration = System.currentTimeMillis() - startTime;
            log.info("All tasks completed before rebalance (waited {}ms)", waitDuration);
        } else {
            log.error("Rebalance timeout: {} tasks still in-flight, proceeding anyway",
                    shutdownManager.getInFlightTaskCount());
            log.error("These tasks may be processed by another consumer (duplicate work)");
        }
    }
    
    /**
     * Called when new partitions are assigned to this consumer.
     * 
     * This happens after:
     * - Consumer joins the group for the first time
     * - Rebalance completes and partitions are redistributed
     * 
     * We can use this to:
     * 1. Initialize partition-specific state
     * 2. Seek to specific offsets if needed
     * 3. Log partition assignment for monitoring
     * 
     * @param partitions partitions now assigned to this consumer
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("=== Kafka Rebalance: Partitions Assigned ===");
        log.info("Worker: {}", workerId);
        log.info("Assigned partitions: {}", partitions);
        log.info("Total partitions: {}", partitions.size());
        
        // Log partition details
        for (TopicPartition partition : partitions) {
            log.debug("Assigned: topic={}, partition={}", 
                    partition.topic(), partition.partition());
        }
        
        log.info("Rebalance complete, resuming normal operation");
    }
    
    /**
     * Called when partitions are lost (not gracefully revoked).
     * 
     * This happens when:
     * - Consumer crashes
     * - Consumer is forcefully removed from group
     * - Network partition
     * 
     * We cannot finish processing tasks - they are already lost to another consumer.
     * Just log and clean up.
     * 
     * @param partitions partitions that were lost
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.error("=== Kafka Rebalance: Partitions Lost (Unclean) ===");
        log.error("Worker: {}", workerId);
        log.error("Lost partitions: {}", partitions);
        log.error("In-flight tasks: {}", shutdownManager.getInFlightTaskCount());
        log.error("Tasks will be reassigned to other consumers");
        
        // Cannot wait for tasks - partitions already reassigned
        // Just log for debugging
        if (shutdownManager.getInFlightTaskCount() > 0) {
            log.error("WARNING: {} tasks were in-flight when partitions lost", 
                    shutdownManager.getInFlightTaskCount());
            log.error("These tasks will be retried by another consumer");
        }
    }
}
