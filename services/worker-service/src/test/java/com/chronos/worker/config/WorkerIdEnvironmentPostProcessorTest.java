package com.chronos.worker.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.RandomValuePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerIdEnvironmentPostProcessorTest {

    private final WorkerIdEnvironmentPostProcessor processor = new WorkerIdEnvironmentPostProcessor();

    @Test
    void randomDefaultIsResolvedOnlyOnce() {
        StandardEnvironment environment = environmentWith(Map.of("worker.id", "worker-${random.uuid}"));
        RandomValuePropertySource.addToEnvironment(environment);
        // Without pinning, every lookup returns a different ID
        assertThat(environment.getProperty("worker.id")).isNotEqualTo(environment.getProperty("worker.id"));

        processor.postProcessEnvironment(environment, null);

        String workerId = environment.getProperty("worker.id");
        assertThat(workerId).startsWith("worker-");
        assertThat(environment.getProperty("worker.id")).isEqualTo(workerId);
    }

    @Test
    void blankIdFallsBackToHostname() {
        // e.g. WORKER_ID defined but empty in the container environment
        StandardEnvironment environment = environmentWith(Map.of("worker.id", "", "HOSTNAME", "container-1"));

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("worker.id")).isEqualTo("worker-container-1");
    }

    @Test
    void blankIdWithoutHostnameFallsBackToRandomId() {
        StandardEnvironment environment = environmentWith(Map.of("worker.id", " "));

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("worker.id")).startsWith("worker-").hasSizeGreaterThan(10);
    }

    private static StandardEnvironment environmentWith(Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test", new HashMap<>(properties)));
        return environment;
    }
}
