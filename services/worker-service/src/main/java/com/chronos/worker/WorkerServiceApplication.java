package com.chronos.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Worker Service Application
 * 
 * Responsibilities:
 * - Register worker on startup with unique worker ID
 * - Publish heartbeats to Redis every 10 seconds
 * - Consume TaskReady events from Kafka
 * - Attempt to claim task using Redis distributed lock
 * - Execute task logic (simulate: resize image, process data, etc.)
 * - Report task status: TaskStarted, TaskCompleted, TaskFailed
 * - Handle graceful shutdown
 * - Support horizontal scaling (multiple instances)
 */
@SpringBootApplication
@EnableMongoAuditing
@EnableScheduling
public class WorkerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerServiceApplication.class, args);
    }
}
