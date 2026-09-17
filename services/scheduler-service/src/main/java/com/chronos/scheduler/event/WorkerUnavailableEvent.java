package com.chronos.scheduler.event;

import java.time.Instant;

/**
 * Event received when a worker becomes unavailable.
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
}
