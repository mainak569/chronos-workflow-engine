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

/**
 * Persistent state for scheduled workflows.
 * 
 * Tracks when workflows were last executed and when they should execute next.
 * Uses optimistic locking to prevent duplicate scheduling by concurrent schedulers.
 * 
 * CRITICAL: All scheduling decisions must be based on this persistent state,
 * never on local JVM memory.
 */
@Document(collection = "scheduler_state")
@CompoundIndexes({
    @CompoundIndex(name = "enabled_next_scheduled_idx", 
                   def = "{'enabled': 1, 'nextScheduledTime': 1}"),
    @CompoundIndex(name = "workflow_enabled_idx",
                   def = "{'workflowId': 1, 'enabled': 1}")
})
public class SchedulerState {
    
    /**
     * Unique identifier (same as workflowId for 1:1 mapping).
     */
    @Id
    private String id;
    
    /**
     * Workflow identifier.
     */
    @Indexed(unique = true)
    private String workflowId;
    
    /**
     * Whether this workflow is enabled for scheduling.
     */
    @Indexed
    private boolean enabled;
    
    /**
     * Cron expression defining the schedule.
     * Examples: "0 * * * *" (hourly), "STAR/5 * * * *" (every 5 minutes)
     */
    private String schedule;
    
    /**
     * Timezone for schedule evaluation.
     */
    private String timezone;
    
    /**
     * Timestamp when workflow was last executed.
     */
    @Indexed
    private Instant lastExecutionTime;
    
    /**
     * Execution ID of the last execution.
     */
    private String lastExecutionId;
    
    /**
     * Status of the last execution.
     */
    private ExecutionStatus lastExecutionStatus;
    
    /**
     * Timestamp when workflow should execute next.
     * Scheduler queries for (enabled=true AND nextScheduledTime <= now).
     */
    @Indexed
    private Instant nextScheduledTime;
    
    /**
     * Total number of executions triggered.
     */
    private long executionCount;
    
    /**
     * Version for optimistic locking.
     * Prevents duplicate scheduling when multiple schedulers race.
     */
    @Version
    private Long version;
    
    /**
     * When this state was created.
     */
    @CreatedDate
    private Instant createdAt;
    
    /**
     * When this state was last updated.
     */
    @LastModifiedDate
    private Instant updatedAt;
    
    /**
     * ID of scheduler that last updated this state.
     */
    private String lastSchedulerId;
    
    // Constructors
    
    public SchedulerState() {
    }
    
    public SchedulerState(String workflowId, String schedule, String timezone) {
        this.id = workflowId; // Use workflowId as document ID
        this.workflowId = workflowId;
        this.schedule = schedule;
        this.timezone = timezone;
        this.enabled = true;
        this.executionCount = 0;
    }
    
    // Business Methods
    
    /**
     * Mark workflow as executed.
     * Updates last execution info, increments counter, calculates next schedule time.
     * 
     * @param executionId the execution ID
     * @param executionStatus the execution status
     * @param nextScheduledTime the next scheduled time
     * @param schedulerId the scheduler ID that triggered this
     */
    public void markExecuted(String executionId, ExecutionStatus executionStatus,
                           Instant nextScheduledTime, String schedulerId) {
        this.lastExecutionTime = Instant.now();
        this.lastExecutionId = executionId;
        this.lastExecutionStatus = executionStatus;
        this.nextScheduledTime = nextScheduledTime;
        this.lastSchedulerId = schedulerId;
        this.executionCount++;
    }
    
    /**
     * Check if workflow is ready to execute.
     * 
     * @param now current time
     * @return true if enabled and nextScheduledTime is past
     */
    public boolean isReadyToExecute(Instant now) {
        return enabled && 
               nextScheduledTime != null && 
               !nextScheduledTime.isAfter(now);
    }
    
    /**
     * Disable scheduling for this workflow.
     */
    public void disable() {
        this.enabled = false;
    }
    
    /**
     * Enable scheduling for this workflow.
     */
    public void enable() {
        this.enabled = true;
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getWorkflowId() {
        return workflowId;
    }
    
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public String getSchedule() {
        return schedule;
    }
    
    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }
    
    public String getTimezone() {
        return timezone;
    }
    
    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
    
    public Instant getLastExecutionTime() {
        return lastExecutionTime;
    }
    
    public void setLastExecutionTime(Instant lastExecutionTime) {
        this.lastExecutionTime = lastExecutionTime;
    }
    
    public String getLastExecutionId() {
        return lastExecutionId;
    }
    
    public void setLastExecutionId(String lastExecutionId) {
        this.lastExecutionId = lastExecutionId;
    }
    
    public ExecutionStatus getLastExecutionStatus() {
        return lastExecutionStatus;
    }
    
    public void setLastExecutionStatus(ExecutionStatus lastExecutionStatus) {
        this.lastExecutionStatus = lastExecutionStatus;
    }
    
    public Instant getNextScheduledTime() {
        return nextScheduledTime;
    }
    
    public void setNextScheduledTime(Instant nextScheduledTime) {
        this.nextScheduledTime = nextScheduledTime;
    }
    
    public long getExecutionCount() {
        return executionCount;
    }
    
    public void setExecutionCount(long executionCount) {
        this.executionCount = executionCount;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
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
    
    public String getLastSchedulerId() {
        return lastSchedulerId;
    }
    
    public void setLastSchedulerId(String lastSchedulerId) {
        this.lastSchedulerId = lastSchedulerId;
    }
    
    // Builder
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String workflowId;
        private String schedule;
        private String timezone = "UTC";
        private boolean enabled = true;
        private Instant nextScheduledTime;
        
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }
        
        public Builder schedule(String schedule) {
            this.schedule = schedule;
            return this;
        }
        
        public Builder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder nextScheduledTime(Instant nextScheduledTime) {
            this.nextScheduledTime = nextScheduledTime;
            return this;
        }
        
        public SchedulerState build() {
            SchedulerState state = new SchedulerState(workflowId, schedule, timezone);
            state.setEnabled(enabled);
            state.setNextScheduledTime(nextScheduledTime);
            return state;
        }
    }
    
    @Override
    public String toString() {
        return String.format("SchedulerState{workflowId='%s', enabled=%s, schedule='%s', " +
                        "lastExecutionTime=%s, nextScheduledTime=%s, executionCount=%d, version=%d}",
                workflowId, enabled, schedule, lastExecutionTime, nextScheduledTime,
                executionCount, version);
    }
}
