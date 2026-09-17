package com.chronos.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Scheduler Service Application
 * 
 * Responsibilities:
 * - Listen to Kafka events (WorkflowStarted, TaskCompleted, TaskFailed)
 * - Compute task dependencies
 * - Determine which tasks are in PENDING state with satisfied dependencies
 * - Transition tasks from PENDING to READY
 * - Publish TaskReady events to Kafka
 * - Coordinate across multiple scheduler instances using Redis leader election
 * - Handle scheduler restart gracefully (recover in-progress workflows)
 * - Detect failed workers via heartbeat expiration
 * - Recover tasks from failed workers
 */
@SpringBootApplication
@EnableMongoAuditing
@EnableScheduling
public class SchedulerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerServiceApplication.class, args);
    }
}
