package com.chronos.worker;

import org.junit.jupiter.api.Test;

/**
 * Basic smoke test for Worker Service
 * 
 * Note: Full context tests are disabled until infrastructure dependencies
 * (MongoDB, Kafka, Redis) are properly configured for testing with Testcontainers.
 */
class WorkerServiceApplicationTests {

    @Test
    void contextLoads() {
        // Smoke test - verifies the test class loads
        // Full Spring context tests will be added in the next phase
    }
}
