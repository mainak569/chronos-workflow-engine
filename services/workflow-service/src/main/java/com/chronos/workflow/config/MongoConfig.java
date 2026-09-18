package com.chronos.workflow.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * MongoDB configuration.
 * Auditing (createdAt/updatedAt) is enabled on WorkflowServiceApplication so that
 * test slices such as @DataMongoTest pick it up as well.
 */
@Configuration
@EnableMongoRepositories(basePackages = "com.chronos.workflow.repository")
public class MongoConfig {
}
