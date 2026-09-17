package com.chronos.workflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Metrics for workflow operations.
 * Tracks workflow executions, failures, duration, and active workflows.
 */
@Component
public class WorkflowMetrics {
    
    private final MeterRegistry registry;
    
    public WorkflowMetrics(MeterRegistry registry) {
        this.registry = registry;
    }
    
    /**
     * Record workflow execution start.
     *
     * @param workflowId Workflow identifier
     * @param status Execution status (RUNNING, COMPLETED, FAILED)
     */
    public void recordWorkflowExecution(String workflowId, String status) {
        Counter.builder("chronos.workflow.executions")
                .description("Total workflow executions")
                .tag("workflow_id", workflowId)
                .tag("status", status)
                .register(registry)
                .increment();
    }
    
    /**
     * Record workflow duration.
     *
     * @param workflowId Workflow identifier
     * @param startTime Workflow start time
     * @param status Final status
     */
    public void recordWorkflowDuration(String workflowId, Instant startTime, String status) {
        Duration duration = Duration.between(startTime, Instant.now());
        
        Timer.builder("chronos.workflow.duration")
                .description("Workflow execution duration")
                .tag("workflow_id", workflowId)
                .tag("status", status)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry)
                .record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record workflow failure.
     *
     * @param workflowId Workflow identifier
     * @param errorType Type of error
     */
    public void recordWorkflowFailure(String workflowId, String errorType) {
        Counter.builder("chronos.workflow.failures")
                .description("Total workflow failures")
                .tag("workflow_id", workflowId)
                .tag("error_type", errorType)
                .register(registry)
                .increment();
    }
    
    /**
     * Increment active workflow count.
     *
     * @param workflowId Workflow identifier
     */
    public void incrementActiveWorkflows(String workflowId) {
        registry.gauge("chronos.workflow.active", 
                java.util.Collections.singletonList(io.micrometer.core.instrument.Tag.of("workflow_id", workflowId)),
                1);
    }
    
    /**
     * Decrement active workflow count.
     *
     * @param workflowId Workflow identifier
     */
    public void decrementActiveWorkflows(String workflowId) {
        registry.gauge("chronos.workflow.active", 
                java.util.Collections.singletonList(io.micrometer.core.instrument.Tag.of("workflow_id", workflowId)),
                0);
    }
    
    /**
     * Record workflow created.
     *
     * @param workflowId Workflow identifier
     * @param ownerId Owner user ID
     */
    public void recordWorkflowCreated(String workflowId, String ownerId) {
        Counter.builder("chronos.workflow.created")
                .description("Total workflows created")
                .tag("workflow_id", workflowId)
                .tag("owner_id", ownerId)
                .register(registry)
                .increment();
    }
    
    /**
     * Record workflow deleted.
     *
     * @param workflowId Workflow identifier
     */
    public void recordWorkflowDeleted(String workflowId) {
        Counter.builder("chronos.workflow.deleted")
                .description("Total workflows deleted")
                .tag("workflow_id", workflowId)
                .register(registry)
                .increment();
    }
}
