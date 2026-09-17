package com.chronos.scheduler.event;

import java.time.Instant;
import java.util.List;

/**
 * Event received when a worker registers itself with the system.
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
}
