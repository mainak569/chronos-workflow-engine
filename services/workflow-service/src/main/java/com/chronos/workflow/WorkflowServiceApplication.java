package com.chronos.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

/**
 * Workflow Service Application
 * 
 * Responsibilities:
 * - Create and validate workflow definitions
 * - Store workflow schemas in MongoDB
 * - Validate workflow DAG (detect cycles)
 * - Start workflow executions
 * - Create execution records
 * - Determine initial runnable tasks
 * - Publish WorkflowStarted events to Kafka
 * - Query workflow and execution status
 * - Update execution state on completion/failure
 */
@SpringBootApplication
@EnableMongoAuditing
public class WorkflowServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkflowServiceApplication.class, args);
    }
}
