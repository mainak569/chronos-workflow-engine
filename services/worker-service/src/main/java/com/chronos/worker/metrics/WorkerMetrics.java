package com.chronos.worker.metrics;

import com.chronos.worker.shutdown.GracefulShutdownManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Task execution metrics of this worker (exposed at /actuator/prometheus).
 * Labels are low-cardinality: task_type comes from the configured set of supported types.
 */
@Component
public class WorkerMetrics {

    private final MeterRegistry registry;
    private final Counter deadLettered;

    public WorkerMetrics(MeterRegistry registry, GracefulShutdownManager shutdownManager) {
        this.registry = registry;
        this.deadLettered = Counter.builder("chronos.task.dlq")
                .description("Tasks sent to the dead letter queue after their final failed attempt")
                .register(registry);
        Gauge.builder("chronos.worker.tasks.inflight", shutdownManager, GracefulShutdownManager::getInFlightTaskCount)
                .description("Tasks currently executing on this worker")
                .register(registry);
    }

    /**
     * Record one task attempt executed by this worker.
     *
     * @param outcome "success" or "failure"
     */
    public void recordExecution(String taskType, String outcome, long durationMs) {
        Timer.builder("chronos.task.execution")
                .description("Task attempt execution time on workers")
                .tag("task_type", taskType != null ? taskType : "unknown")
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(30))
                .register(registry)
                .record(Duration.ofMillis(durationMs));
    }

    public void recordDeadLettered() {
        deadLettered.increment();
    }
}
