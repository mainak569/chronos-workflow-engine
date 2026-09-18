package com.chronos.worker.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;
import java.util.UUID;

/**
 * Resolves {@code worker.id} once at startup and makes sure it is never blank.
 *
 * The configured default contains {@code ${random.uuid}}, which Spring re-evaluates on every
 * resolution; without pinning it, each component injecting {@code worker.id} would get a
 * different ID (registration, heartbeats and task claims would not match).
 *
 * A blank value (e.g. WORKER_ID defined but empty in the environment) falls back to the
 * host name, then to a random ID, so a worker always has one stable, unique identity.
 */
public class WorkerIdEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    static final String PROPERTY = "worker.id";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String workerId = environment.getProperty(PROPERTY);
        if (workerId == null || workerId.isBlank()) {
            String hostname = environment.getProperty("HOSTNAME");
            workerId = "worker-" + (hostname != null && !hostname.isBlank()
                    ? hostname : UUID.randomUUID().toString());
        }
        environment.getPropertySources().addFirst(new MapPropertySource("resolvedWorkerId", Map.of(PROPERTY, workerId)));
    }

    @Override
    public int getOrder() {
        // After application.yml and profile-specific config have been loaded
        return Ordered.LOWEST_PRECEDENCE;
    }
}
