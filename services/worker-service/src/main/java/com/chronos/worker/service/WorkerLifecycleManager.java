package com.chronos.worker.service;

import com.chronos.worker.domain.WorkerMetadata;
import com.chronos.worker.domain.WorkerStatus;
import com.chronos.worker.event.WorkerEventPublisher;
import com.chronos.worker.event.WorkerRegisteredEvent;
import com.chronos.worker.event.WorkerUnavailableEvent;
import com.chronos.worker.shutdown.GracefulShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.UUID;

/**
 * Manages worker lifecycle: registration on startup and deregistration on shutdown.
 */
@Service
public class WorkerLifecycleManager {
    
    private static final Logger log = LoggerFactory.getLogger(WorkerLifecycleManager.class);
    
    private final WorkerRegistry workerRegistry;
    private final WorkerHeartbeatScheduler heartbeatScheduler;
    private final WorkerEventPublisher eventPublisher;
    private final GracefulShutdownManager shutdownManager;
    
    @Value("${worker.id:#{null}}")
    private String workerId;
    
    @Value("${worker.supported-task-types}")
    private List<String> supportedTaskTypes;
    
    private volatile boolean registered = false;
    
    public WorkerLifecycleManager(WorkerRegistry workerRegistry,
                                 WorkerHeartbeatScheduler heartbeatScheduler,
                                 WorkerEventPublisher eventPublisher,
                                 GracefulShutdownManager shutdownManager) {
        this.workerRegistry = workerRegistry;
        this.heartbeatScheduler = heartbeatScheduler;
        this.eventPublisher = eventPublisher;
        this.shutdownManager = shutdownManager;
    }
    
    /**
     * Register worker when application context is fully initialized.
     * Publishes WorkerRegistered event after successful registration.
     */
    @EventListener(ContextRefreshedEvent.class)
    public void onApplicationReady() {
        if (registered) {
            log.debug("Worker already registered, skipping duplicate registration");
            return;
        }
        
        try {
            // Generate worker ID if not configured
            if (workerId == null || workerId.isEmpty()) {
                workerId = "worker-" + UUID.randomUUID().toString();
                log.info("Generated worker ID: {}", workerId);
            }
            
            // Validate configuration
            if (supportedTaskTypes == null || supportedTaskTypes.isEmpty()) {
                throw new IllegalStateException("Worker must support at least one task type");
            }
            
            // Create worker metadata
            WorkerMetadata worker = WorkerMetadata.builder()
                    .workerId(workerId)
                    .status(WorkerStatus.REGISTERING)
                    .supportedTaskTypes(supportedTaskTypes)
                    .build();
            
            log.info("Registering worker: {} with task types: {}", workerId, supportedTaskTypes);
            
            // Register with registry (transitions to AVAILABLE)
            workerRegistry.registerWorker(worker);
            
            // Enable heartbeat
            heartbeatScheduler.enableHeartbeat();
            
            // Publish registration event
            WorkerRegisteredEvent registeredEvent = WorkerRegisteredEvent.builder()
                    .workerId(workerId)
                    .supportedTaskTypes(supportedTaskTypes)
                    .build();
            eventPublisher.publishWorkerRegistered(registeredEvent);
            
            registered = true;
            
            log.info("Worker registration complete: {}", workerId);
            
        } catch (Exception e) {
            log.error("Failed to register worker", e);
            throw new RuntimeException("Worker registration failed", e);
        }
    }
    
    /**
     * Gracefully deregister worker on shutdown.
     * Publishes WorkerUnavailable event before deregistration.
     * Waits for in-flight tasks via GracefulShutdownManager.
     */
    @PreDestroy
    public void onShutdown() {
        if (!registered) {
            log.debug("Worker not registered, skipping deregistration");
            return;
        }
        
        try {
            log.info("Gracefully shutting down worker: {}", workerId);
            
            // Get current task stats
            GracefulShutdownManager.ShutdownStatistics stats = shutdownManager.getStatistics();
            log.info("Shutdown stats: {}", stats);
            
            // Disable heartbeat
            heartbeatScheduler.disableHeartbeat();
            
            // Publish unavailable event
            WorkerUnavailableEvent unavailableEvent = WorkerUnavailableEvent.builder()
                    .workerId(workerId)
                    .reason("Graceful shutdown")
                    .build();
            eventPublisher.publishWorkerUnavailable(unavailableEvent);
            
            // GracefulShutdownManager will wait for in-flight tasks
            // (triggered by its own @PreDestroy, which runs after this due to Spring ordering)
            
            // Deregister from registry
            workerRegistry.deregisterWorker(workerId);
            
            registered = false;
            
            log.info("Worker shutdown complete: {}", workerId);
            
        } catch (Exception e) {
            log.error("Error during worker shutdown", e);
            // Continue shutdown even if deregistration fails
        }
    }
    
    /**
     * Check if worker is registered and active.
     */
    public boolean isRegistered() {
        return registered;
    }
    
    /**
     * Get the current worker ID.
     */
    public String getWorkerId() {
        return workerId;
    }
}
