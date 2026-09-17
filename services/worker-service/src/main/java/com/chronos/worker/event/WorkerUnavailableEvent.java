package com.chronos.worker.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Event published when a worker becomes unavailable (shutdown, crash, or heartbeat timeout).
 */
public class WorkerUnavailableEvent {

    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String workerId;
    private String reason;

    // Constructors
    public WorkerUnavailableEvent() {
    }

    public WorkerUnavailableEvent(String eventId, String correlationId, Instant timestamp,
                                 String workerId, String reason) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.workerId = workerId;
        this.reason = reason;
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

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
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
        private String reason;

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

        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public WorkerUnavailableEvent build() {
            return new WorkerUnavailableEvent(eventId, correlationId, timestamp,
                    workerId, reason);
        }
    }

    @Override
    public String toString() {
        return "WorkerUnavailableEvent{" +
                "eventId='" + eventId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", workerId='" + workerId + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }
}
