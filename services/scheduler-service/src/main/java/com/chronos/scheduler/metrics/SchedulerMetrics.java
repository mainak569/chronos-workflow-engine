package com.chronos.scheduler.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Workflow and task execution metrics recorded by the scheduler, which drives every execution
 * (exposed at /actuator/prometheus).
 *
 * All labels are low-cardinality (status / outcome only), so the number of time series is fixed.
 */
@Component
public class SchedulerMetrics {

    private final MeterRegistry registry;

    private final Timer workflowDuration;
    private final Counter tasksDispatched;

    private final AtomicLong activeExecutions = new AtomicLong();
    private final AtomicLong outboxPending = new AtomicLong();
    private final AtomicLong leader = new AtomicLong();

    public SchedulerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.workflowDuration = Timer.builder("chronos.workflow.duration")
                .description("Duration of finished workflow executions (start to completion or failure)")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(100))
                .maximumExpectedValue(Duration.ofHours(1))
                .register(registry);
        this.tasksDispatched = Counter.builder("chronos.tasks.dispatched")
                .description("Task attempts dispatched to workers")
                .register(registry);

        Gauge.builder("chronos.workflow.active", activeExecutions, AtomicLong::get)
                .description("Workflow executions currently PENDING or RUNNING")
                .register(registry);
        Gauge.builder("chronos.outbox.pending", outboxPending, AtomicLong::get)
                .description("Outbox messages waiting to be published to Kafka")
                .register(registry);
        Gauge.builder("chronos.scheduler.leader", leader, AtomicLong::get)
                .description("1 if this scheduler instance is the elected leader, else 0")
                .register(registry);
    }

    /**
     * A workflow execution reached COMPLETED or FAILED.
     */
    public void recordExecutionFinished(String status, Long durationMs) {
        Counter.builder("chronos.workflow.executions")
                .description("Workflow executions that reached a final status")
                .tag("status", status)
                .register(registry)
                .increment();
        if (durationMs != null) {
            workflowDuration.record(Duration.ofMillis(durationMs));
        }
    }

    /**
     * A task attempt finished: completed, failed (permanently), retried (failed, another attempt
     * scheduled) or requeued (its worker was lost).
     */
    public void recordTaskOutcome(String outcome) {
        Counter.builder("chronos.tasks")
                .description("Task attempt outcomes as recorded by the scheduler")
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    public void recordTaskDispatched() {
        tasksDispatched.increment();
    }

    public void updateActiveExecutions(long count) {
        activeExecutions.set(count);
    }

    public void updateOutboxPending(long count) {
        outboxPending.set(count);
    }

    public void updateLeader(boolean isLeader) {
        leader.set(isLeader ? 1 : 0);
    }
}
