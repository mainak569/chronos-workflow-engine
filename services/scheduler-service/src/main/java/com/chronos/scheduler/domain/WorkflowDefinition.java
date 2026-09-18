package com.chronos.scheduler.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only view of a workflow definition owned by workflow-service ("workflows" collection).
 * Used to create executions for scheduled (cron) workflows. Never saved by this service.
 */
@Document(collection = "workflows")
public class WorkflowDefinition {

    @Id
    private String id;
    private String ownerId;
    private String name;
    private List<TaskSpec> tasks = new ArrayList<>();
    private String schedule;
    private String timezone;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TaskSpec> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskSpec> tasks) {
        this.tasks = tasks != null ? tasks : new ArrayList<>();
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

    /**
     * A task definition inside a workflow.
     */
    public static class TaskSpec {
        private String taskId;
        private String name;
        private String taskType;
        private Set<String> dependencies = new HashSet<>();
        private Map<String, Object> configuration = new HashMap<>();
        private RetrySpec retryConfig;
        private Long timeoutMs;

        public Long getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTaskType() {
            return taskType;
        }

        public void setTaskType(String taskType) {
            this.taskType = taskType;
        }

        public Set<String> getDependencies() {
            return dependencies;
        }

        public void setDependencies(Set<String> dependencies) {
            this.dependencies = dependencies != null ? dependencies : new HashSet<>();
        }

        public Map<String, Object> getConfiguration() {
            return configuration;
        }

        public void setConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration != null ? configuration : new HashMap<>();
        }

        public RetrySpec getRetryConfig() {
            return retryConfig;
        }

        public void setRetryConfig(RetrySpec retryConfig) {
            this.retryConfig = retryConfig;
        }
    }

    /**
     * Retry settings of a task definition.
     */
    public static class RetrySpec {
        private Integer maxAttempts;
        private Long initialDelayMs;
        private Double backoffMultiplier;
        private Long maxDelayMs;

        public Integer getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Long getInitialDelayMs() {
            return initialDelayMs;
        }

        public void setInitialDelayMs(Long initialDelayMs) {
            this.initialDelayMs = initialDelayMs;
        }

        public Double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(Double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }

        public Long getMaxDelayMs() {
            return maxDelayMs;
        }

        public void setMaxDelayMs(Long maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
        }
    }
}
