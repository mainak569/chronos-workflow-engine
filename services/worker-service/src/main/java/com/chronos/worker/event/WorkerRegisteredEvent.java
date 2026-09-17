package com.chronos.worker.event;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event published when a worker registers itself with the system.
 */
public class WorkerRegisteredEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workerId;
    private List<String> supportedTaskTypes;
    private String workerHostname;
    private Integer workerPort;

    // Constructors
    public WorkerRegisteredEvent() {
    }

    public WorkerRegisteredEvent(String eventId, String correlationId, Instant timestamp,
                                String workerId, List<String> supportedTaskTypes,
                                String workerHostname, Integer workerPort) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workerId = workerId;
        this.supportedTaskTypes = supportedTaskTypes;
        this.workerHostname = workerHostname;
        this.workerPort = workerPort;
    }

    // Getters and Setters
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public List<String> getSupportedTaskTypes() {
        return supportedTaskTypes;
    }

    public void setSupportedTaskTypes(List<String> supportedTaskTypes) {
        this.supportedTaskTypes = supportedTaskTypes;
    }

    public String getWorkerHostname() {
        return workerHostname;
    }

    public void setWorkerHostname(String workerHostname) {
        this.workerHostname = workerHostname;
    }

    public Integer getWorkerPort() {
        return workerPort;
    }

    public void setWorkerPort(Integer workerPort) {
        this.workerPort = workerPort;
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String eventId = UUID.randomUUID().toString();
        private String correlationId;
        private Instant timestamp = Instant.now();
        private String workerId;
        private List<String> supportedTaskTypes;
        private String workerHostname;
        private Integer workerPort;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder supportedTaskTypes(List<String> supportedTaskTypes) {
            this.supportedTaskTypes = supportedTaskTypes;
            return this;
        }

        public Builder workerHostname(String workerHostname) {
            this.workerHostname = workerHostname;
            return this;
        }

        public Builder workerPort(Integer workerPort) {
            this.workerPort = workerPort;
            return this;
        }

        public WorkerRegisteredEvent build() {
            return new WorkerRegisteredEvent(eventId, correlationId, timestamp,
                    workerId, supportedTaskTypes, workerHostname, workerPort);
        }
    }

    @Override
    public String toString() {
        return "WorkerRegisteredEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workerId='" + workerId + '\'' +
                ", supportedTaskTypes=" + supportedTaskTypes +
                ", workerHostname='" + workerHostname + '\'' +
                ", workerPort=" + workerPort +
                '}';
    }
}
