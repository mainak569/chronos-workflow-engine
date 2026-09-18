package com.chronos.scheduler.domain;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single execution instance of a task within a workflow execution.
 * Tracks task-level execution state, dependencies, and results.
 */
@Document(collection = "task_executions")
@CompoundIndexes({
    @CompoundIndex(name = "execution_task_idx", def = "{'executionId': 1, 'taskId': 1}", unique = true),
    @CompoundIndex(name = "execution_status_idx", def = "{'executionId': 1, 'status': 1}"),
    // Recovery queries of the scheduler: due retries, stale dispatches, tasks of a lost worker
    @CompoundIndex(name = "status_retry_idx", def = "{'status': 1, 'nextRetryAt': 1}"),
    @CompoundIndex(name = "status_dispatched_idx", def = "{'status': 1, 'dispatchedAt': 1}"),
    @CompoundIndex(name = "status_worker_idx", def = "{'status': 1, 'workerId': 1}")
})
public class TaskExecution {
    
    /**
     * Unique identifier for this task execution.
     */
    @Id
    private String id;
    
    /**
     * ID of the workflow execution this task belongs to.
     */
    @Indexed
    private String executionId;
    
    /**
     * ID of the workflow definition.
     */
    @Indexed
    private String workflowId;
    
    /**
     * ID of the task definition within the workflow.
     */
    private String taskId;
    
    /**
     * Name of the task (denormalized for convenience).
     */
    private String taskName;
    
    /**
     * Type of the task (e.g., IMAGE_RESIZE, DATA_PROCESSING).
     */
    private String taskType;
    
    /**
     * Current status of the task execution.
     */
    @Indexed
    private ExecutionStatus status;
    
    /**
     * List of task IDs that this task depends on.
     */
    private List<String> dependsOn;
    
    /**
     * Configuration/parameters for this task execution.
     */
    private Map<String, Object> configuration;
    
    /**
     * Input data for this task (resolved from dependencies and workflow input).
     */
    private Map<String, Object> input;
    
    /**
     * Output result when task completes successfully.
     */
    private Map<String, Object> output;
    
    /**
     * Error message if task fails.
     */
    private String errorMessage;
    
    /**
     * Error type/category if task fails.
     */
    private String errorType;
    
    /**
     * ID of the worker that executed this task.
     */
    private String workerId;
    
    /**
     * Current attempt number (starts at 1).
     */
    private int attemptNumber;
    
    /**
     * Maximum number of retry attempts allowed.
     */
    private int maxRetries;
    
    /**
     * Whether this task can be retried on failure.
     */
    private boolean retriable;
    
    /**
     * Timestamp when task execution was created.
     */
    @CreatedDate
    @Indexed
    private Instant createdAt;
    
    /**
     * Timestamp when task execution was last updated.
     */
    @LastModifiedDate
    private Instant updatedAt;
    
    /**
     * Timestamp when task started (transitioned to RUNNING).
     */
    private Instant startedAt;
    
    /**
     * Timestamp when task completed (reached terminal state).
     */
    private Instant completedAt;
    
    /**
     * Duration in milliseconds (only set after completion).
     */
    private Long durationMs;
    
    /**
     * Version for optimistic locking.
     */
    @Version
    private Long version;
    
    /**
     * Timestamp when a TaskReady event was issued for the current attempt.
     * Null means the current attempt has not been dispatched yet.
     */
    private Instant dispatchedAt;

    /**
     * Retry backoff settings copied from the task definition's retry configuration.
     * Null values fall back to the scheduler defaults.
     */
    private Long retryInitialDelayMs;
    private Double retryBackoffMultiplier;
    private Long retryMaxDelayMs;

    /**
     * Maximum execution time of one attempt in milliseconds (from the task definition).
     * Workers abort attempts that exceed it and report a retriable timeout failure.
     */
    private Long timeoutMs;

    /**
     * Timestamp of the last failed attempt.
     */
    private Instant lastAttemptAt;
    
    /**
     * Earliest time the next retry attempt may be dispatched (null if not waiting for a retry).
     */
    private Instant nextRetryAt;
    
    // Constructors
    public TaskExecution() {
    }
    
    // State transition methods with validation
    
    /**
     * Transition to RUNNING state.
     * Sets startedAt timestamp and worker information.
     */
    public void start(String workerId) {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.RUNNING);
        }
        this.status = ExecutionStatus.RUNNING;
        this.workerId = workerId;
        this.startedAt = Instant.now();
    }
    
    /**
     * Transition to COMPLETED state.
     * Sets completedAt and calculates duration.
     */
    public void complete(Map<String, Object> output) {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.COMPLETED);
        }
        this.status = ExecutionStatus.COMPLETED;
        this.output = output;
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    /**
     * Transition to FAILED state.
     * Sets error information, completedAt and calculates duration.
     */
    public void fail(String errorMessage, String errorType, boolean retriable) {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.FAILED);
        }
        this.status = ExecutionStatus.FAILED;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.retriable = retriable;
        this.lastAttemptAt = Instant.now();
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    /**
     * Transition to CANCELLED state.
     * Sets completedAt and calculates duration.
     */
    public void cancel() {
        if (this.status != null) {
            this.status.validateTransition(ExecutionStatus.CANCELLED);
        }
        this.status = ExecutionStatus.CANCELLED;
        this.completedAt = Instant.now();
        if (this.startedAt != null) {
            this.durationMs = this.completedAt.toEpochMilli() - this.startedAt.toEpochMilli();
        }
    }
    
    /**
     * Put a failed RUNNING attempt back to PENDING for another attempt.
     * Increments the attempt number and clears dispatch/worker state so the
     * task is dispatched again once {@code nextRetryAt} has passed.
     */
    public void scheduleRetry(String errorMessage, String errorType, Instant nextRetryAt) {
        if (this.status != ExecutionStatus.RUNNING) {
            throw new IllegalStateException("Only RUNNING tasks can be retried, current status: " + status);
        }
        if (!canRetry()) {
            throw new IllegalStateException("Task cannot be retried: attemptNumber=" + attemptNumber + ", maxRetries=" + maxRetries);
        }
        this.attemptNumber++;
        this.status = ExecutionStatus.PENDING;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.lastAttemptAt = Instant.now();
        this.nextRetryAt = nextRetryAt;
        this.dispatchedAt = null;
        this.workerId = null;
        this.startedAt = null;
    }
    
    /**
     * Return a RUNNING task whose worker disappeared to PENDING without consuming an attempt.
     */
    public void requeue() {
        if (this.status != ExecutionStatus.RUNNING) {
            throw new IllegalStateException("Only RUNNING tasks can be requeued, current status: " + status);
        }
        this.status = ExecutionStatus.PENDING;
        this.dispatchedAt = null;
        this.workerId = null;
        this.startedAt = null;
        this.nextRetryAt = null;
    }
    
    /**
     * Check whether this task can be dispatched now: PENDING, not yet dispatched for the
     * current attempt, past its retry backoff, and with all dependencies completed.
     */
    public boolean isDispatchable(Map<String, TaskExecution> taskExecutions, Instant now) {
        return status == ExecutionStatus.PENDING
                && dispatchedAt == null
                && (nextRetryAt == null || !now.isBefore(nextRetryAt))
                && areDependenciesSatisfied(taskExecutions);
    }
    
    /**
     * Check if all dependencies are satisfied.
     * Dependencies are satisfied when all dependent tasks are COMPLETED.
     */
    public boolean areDependenciesSatisfied(Map<String, TaskExecution> taskExecutions) {
        if (dependsOn == null || dependsOn.isEmpty()) {
            return true; // No dependencies means ready to execute
        }
        
        for (String dependencyId : dependsOn) {
            TaskExecution dependency = taskExecutions.get(dependencyId);
            if (dependency == null || dependency.getStatus() != ExecutionStatus.COMPLETED) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Check if this task can be retried.
     */
    public boolean canRetry() {
        return retriable && attemptNumber < maxRetries;
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public void setRetriable(boolean retriable) {
        this.retriable = retriable;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Instant getDispatchedAt() {
        return dispatchedAt;
    }

    public void setDispatchedAt(Instant dispatchedAt) {
        this.dispatchedAt = dispatchedAt;
    }

    public Long getRetryInitialDelayMs() {
        return retryInitialDelayMs;
    }

    public void setRetryInitialDelayMs(Long retryInitialDelayMs) {
        this.retryInitialDelayMs = retryInitialDelayMs;
    }

    public Double getRetryBackoffMultiplier() {
        return retryBackoffMultiplier;
    }

    public void setRetryBackoffMultiplier(Double retryBackoffMultiplier) {
        this.retryBackoffMultiplier = retryBackoffMultiplier;
    }

    public Long getRetryMaxDelayMs() {
        return retryMaxDelayMs;
    }

    public void setRetryMaxDelayMs(Long retryMaxDelayMs) {
        this.retryMaxDelayMs = retryMaxDelayMs;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    public void setLastAttemptAt(Instant lastAttemptAt) {
        this.lastAttemptAt = lastAttemptAt;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Instant nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }
    
    // Builder
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private String executionId;
        private String workflowId;
        private String taskId;
        private String taskName;
        private String taskType;
        private ExecutionStatus status = ExecutionStatus.PENDING;
        private List<String> dependsOn = new ArrayList<>();
        private Map<String, Object> configuration = new HashMap<>();
        private Map<String, Object> input = new HashMap<>();
        private Map<String, Object> output;
        private String errorMessage;
        private String errorType;
        private String workerId;
        private int attemptNumber = 1;
        private int maxRetries = 3;
        private boolean retriable = true;
        private Instant createdAt;
        private Instant updatedAt;
        private Instant startedAt;
        private Instant completedAt;
        private Long durationMs;
        private Long version;
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder executionId(String executionId) {
            this.executionId = executionId;
            return this;
        }
        
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }
        
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }
        
        public Builder taskName(String taskName) {
            this.taskName = taskName;
            return this;
        }
        
        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }
        
        public Builder status(ExecutionStatus status) {
            this.status = status;
            return this;
        }
        
        public Builder dependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn != null ? dependsOn : new ArrayList<>();
            return this;
        }
        
        public Builder configuration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }
        
        public Builder input(Map<String, Object> input) {
            this.input = input;
            return this;
        }
        
        public Builder output(Map<String, Object> output) {
            this.output = output;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder errorType(String errorType) {
            this.errorType = errorType;
            return this;
        }
        
        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }
        
        public Builder attemptNumber(int attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }
        
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder retriable(boolean retriable) {
            this.retriable = retriable;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }
        
        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }
        
        public Builder completedAt(Instant completedAt) {
            this.completedAt = completedAt;
            return this;
        }
        
        public Builder durationMs(Long durationMs) {
            this.durationMs = durationMs;
            return this;
        }
        
        public Builder version(Long version) {
            this.version = version;
            return this;
        }
        
        public TaskExecution build() {
            TaskExecution execution = new TaskExecution();
            execution.setId(id);
            execution.setExecutionId(executionId);
            execution.setWorkflowId(workflowId);
            execution.setTaskId(taskId);
            execution.setTaskName(taskName);
            execution.setTaskType(taskType);
            execution.setStatus(status);
            execution.setDependsOn(dependsOn);
            execution.setConfiguration(configuration);
            execution.setInput(input);
            execution.setOutput(output);
            execution.setErrorMessage(errorMessage);
            execution.setErrorType(errorType);
            execution.setWorkerId(workerId);
            execution.setAttemptNumber(attemptNumber);
            execution.setMaxRetries(maxRetries);
            execution.setRetriable(retriable);
            execution.setCreatedAt(createdAt);
            execution.setUpdatedAt(updatedAt);
            execution.setStartedAt(startedAt);
            execution.setCompletedAt(completedAt);
            execution.setDurationMs(durationMs);
            execution.setVersion(version);
            return execution;
        }
    }
}
