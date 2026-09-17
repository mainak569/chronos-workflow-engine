package com.chronos.workflow.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * MongoDB configuration.
 * Enables auditing for automatic createdAt/updatedAt timestamps.
 */
@Configuration
@EnableMongoAuditing
@EnableMongoRepositories(basePackages = "com.chronos.workflow.repository")
public class MongoConfig {
}
