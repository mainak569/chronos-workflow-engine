package com.chronos.worker.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Scheduler for periodic worker heartbeat transmission.
 * Sends heartbeats every 10 seconds to maintain worker availability status.
 */
@Service
public class WorkerHeartbeatScheduler {
    
    private static final Logger log = LoggerFactory.getLogger(WorkerHeartbeatScheduler.class);
    
    private final WorkerRegistry workerRegistry;
    
    @Value("${worker.id}")
    private String workerId;
    
    private volatile boolean heartbeatEnabled = false;
    
    public WorkerHeartbeatScheduler(WorkerRegistry workerRegistry) {
        this.workerRegistry = workerRegistry;
    }
    
    /**
     * Enable heartbeat transmission.
     * Called after worker registration is complete.
     */
    public void enableHeartbeat() {
        this.heartbeatEnabled = true;
        log.info("Heartbeat enabled for worker: {}", workerId);
    }
    
    /**
     * Disable heartbeat transmission.
     * Called during graceful shutdown.
     */
    public void disableHeartbeat() {
        this.heartbeatEnabled = false;
        log.info("Heartbeat disabled for worker: {}", workerId);
    }
    
    /**
     * Send heartbeat every 10 seconds.
     * Fixed rate ensures consistent heartbeat interval even if processing takes time.
     */
    @Scheduled(fixedRateString = "${worker.heartbeat.interval:10000}")
    public void sendHeartbeat() {
        if (!heartbeatEnabled) {
            return;
        }
        
        try {
            workerRegistry.updateHeartbeat(workerId);
            log.debug("Heartbeat sent for worker: {}", workerId);
        } catch (Exception e) {
            log.error("Failed to send heartbeat for worker: {}", workerId, e);
            // Don't disable heartbeat on transient errors - will retry on next schedule
        }
    }
}
