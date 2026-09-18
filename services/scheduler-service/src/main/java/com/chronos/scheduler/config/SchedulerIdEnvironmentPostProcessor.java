package com.chronos.scheduler.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;
import java.util.UUID;

/**
 * Resolves {@code scheduler.id} once at startup and makes sure it is never blank.
 *
 * The default value contains {@code ${random.uuid}}, which Spring re-evaluates on every
 * resolution; without pinning it, components injecting {@code scheduler.id} would disagree
 * about this instance's identity.
 */
public class SchedulerIdEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    static final String PROPERTY = "scheduler.id";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String schedulerId = environment.getProperty(PROPERTY);
        if (schedulerId == null || schedulerId.isBlank()) {
            String hostname = environment.getProperty("HOSTNAME");
            schedulerId = "scheduler-" + (hostname != null && !hostname.isBlank()
                    ? hostname : UUID.randomUUID().toString());
        }
        environment.getPropertySources().addFirst(new MapPropertySource("resolvedSchedulerId", Map.of(PROPERTY, schedulerId)));
    }

    @Override
    public int getOrder() {
        // After application.yml and profile-specific config have been loaded
        return Ordered.LOWEST_PRECEDENCE;
    }
}
