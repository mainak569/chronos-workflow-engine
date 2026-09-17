package com.chronos.workflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for task operations.
 * Tracks task executions, duration, retries, queue depth, and latency.
 */
@Component
public class TaskMetrics {
    
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> queueDepthGauges = new ConcurrentHashMap<>();
    
    public TaskMetrics(MeterRegistry registry) {
        this.registry = registry;
    }
    
    /**
     * Record task execution.
     *
     * @param taskType Type of task
     * @param status Execution status (STARTED, COMPLETED, FAILED)
     */
    public void recordTaskExecution(String taskType, String status) {
        Counter.builder("chronos.task.executions")
                .description("Total task executions")
                .tag("task_type", taskType)
                .tag("status", status)
                .register(registry)
                .increment();
    }
    
    /**
     * Record task duration.
     *
     * @param taskType Type of task
     * @param workerId Worker that executed the task
     * @param startTime Task start time
     */
    public void recordTaskDuration(String taskType, String workerId, Instant startTime) {
        Duration duration = Duration.between(startTime, Instant.now());
        
        Timer.builder("chronos.task.duration")
                .description("Task execution duration")
                .tag("task_type", taskType)
                .tag("worker_id", workerId)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry)
                .record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record task retry.
     *
     * @param taskType Type of task
     * @param retryReason Reason for retry
     */
    public void recordTaskRetry(String taskType, String retryReason) {
        Counter.builder("chronos.task.retries")
                .description("Total task retry attempts")
                .tag("task_type", taskType)
                .tag("retry_reason", retryReason)
                .register(registry)
                .increment();
    }
    
    /**
     * Update task queue depth.
     *
     * @param taskType Type of task
     * @param depth Current queue depth
     */
    public void updateQueueDepth(String taskType, long depth) {
        queueDepthGauges.computeIfAbsent(taskType, type -> {
            AtomicLong gauge = new AtomicLong(0);
            Gauge.builder("chronos.task.queue.depth", gauge, AtomicLong::get)
                    .description("Number of tasks waiting in queue")
                    .tag("task_type", type)
                    .register(registry);
            return gauge;
        }).set(depth);
    }
    
    /**
     * Record task latency (time from creation to execution start).
     *
     * @param taskType Type of task
     * @param createdAt Task creation time
     * @param startedAt Task execution start time
     */
    public void recordTaskLatency(String taskType, Instant createdAt, Instant startedAt) {
        Duration latency = Duration.between(createdAt, startedAt);
        
        Timer.builder("chronos.task.latency")
                .description("Time from task creation to execution start")
                .tag("task_type", taskType)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry)
                .record(latency.toMillis(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record task failure.
     *
     * @param taskType Type of task
     * @param errorType Type of error
     */
    public void recordTaskFailure(String taskType, String errorType) {
        Counter.builder("chronos.task.failures")
                .description("Total task failures")
                .tag("task_type", taskType)
                .tag("error_type", errorType)
                .register(registry)
                .increment();
    }
    
    /**
     * Record task sent to DLQ.
     *
     * @param taskType Type of task
     * @param reason Reason for DLQ
     */
    public void recordTaskDLQ(String taskType, String reason) {
        Counter.builder("chronos.task.dlq")
                .description("Tasks sent to dead letter queue")
                .tag("task_type", taskType)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }
}
