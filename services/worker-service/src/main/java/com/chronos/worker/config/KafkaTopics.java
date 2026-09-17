package com.chronos.worker.config;

/**
 * Kafka topic names for the Chronos workflow orchestration system.
 * Centralized topic definitions for consistency across services.
 */
public class KafkaTopics {

    // Workflow lifecycle events
    public static final String WORKFLOW_CREATED = "chronos.workflow.created";
    
    // Task lifecycle events
    public static final String TASK_READY = "chronos.task.ready";
    public static final String TASK_STARTED = "chronos.task.started";
    public static final String TASK_COMPLETED = "chronos.task.completed";
    public static final String TASK_FAILED = "chronos.task.failed";
    public static final String TASK_FAILED_PERMANENTLY = "chronos.task.failed.permanently";
    
    // Worker lifecycle events
    public static final String WORKER_REGISTERED = "chronos.worker.registered";
    public static final String WORKER_UNAVAILABLE = "chronos.worker.unavailable";
    
    // Dead letter topics for error handling
    public static final String DLT_WORKFLOW_CREATED = "chronos.workflow.created.dlt";
    public static final String DLT_TASK_READY = "chronos.task.ready.dlt";
    public static final String DLT_TASK_STATUS = "chronos.task.status.dlt";
    public static final String DLT_WORKER_STATUS = "chronos.worker.status.dlt";

    private KafkaTopics() {
        // Utility class
    }
}
