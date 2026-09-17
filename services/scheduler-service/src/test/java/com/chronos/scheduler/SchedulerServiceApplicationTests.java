package com.chronos.scheduler;

import org.junit.jupiter.api.Test;

/**
 * Basic smoke test for Scheduler Service
 * 
 * Note: Full context tests are disabled until infrastructure dependencies
 * (MongoDB, Kafka, Redis) are properly configured for testing with Testcontainers.
 */
class SchedulerServiceApplicationTests {

    @Test
    void contextLoads() {
        // Smoke test - verifies the test class loads
        // Full Spring context tests will be added in the next phase
    }
}
