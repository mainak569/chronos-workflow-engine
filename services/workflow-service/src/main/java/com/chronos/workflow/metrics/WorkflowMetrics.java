package com.chronos.workflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Workflow API metrics (exposed at /actuator/prometheus).
 *
 * Labels are deliberately low-cardinality (no workflow or user IDs), so the number of
 * time series stays constant no matter how many workflows exist.
 * Execution outcomes (COMPLETED / FAILED) are recorded by the scheduler service, which
 * drives executions; this service records the ones it decides itself (CANCELLED).
 */
@Component
public class WorkflowMetrics {

    private final Counter workflowsCreated;
    private final Counter workflowsDeleted;
    private final Counter executionsStarted;
    private final Counter executionsCancelled;

    public WorkflowMetrics(MeterRegistry registry) {
        this.workflowsCreated = Counter.builder("chronos.workflows.created")
                .description("Workflow definitions created")
                .register(registry);
        this.workflowsDeleted = Counter.builder("chronos.workflows.deleted")
                .description("Workflow definitions deleted")
                .register(registry);
        this.executionsStarted = Counter.builder("chronos.workflow.executions.started")
                .description("Workflow executions started through the API")
                .register(registry);
        this.executionsCancelled = Counter.builder("chronos.workflow.executions")
                .description("Workflow executions that reached a final status")
                .tag("status", "CANCELLED")
                .register(registry);
    }

    public void recordWorkflowCreated() {
        workflowsCreated.increment();
    }

    public void recordWorkflowDeleted() {
        workflowsDeleted.increment();
    }

    public void recordExecutionStarted() {
        executionsStarted.increment();
    }

    public void recordExecutionCancelled() {
        executionsCancelled.increment();
    }
}
