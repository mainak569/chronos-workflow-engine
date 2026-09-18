package com.chronos.scheduler.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.RandomValuePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerIdEnvironmentPostProcessorTest {

    private final SchedulerIdEnvironmentPostProcessor processor = new SchedulerIdEnvironmentPostProcessor();

    @Test
    void randomDefaultIsResolvedOnlyOnce() {
        StandardEnvironment environment = environmentWith(Map.of("scheduler.id", "scheduler-${random.uuid}"));
        RandomValuePropertySource.addToEnvironment(environment);

        processor.postProcessEnvironment(environment, null);

        String schedulerId = environment.getProperty("scheduler.id");
        assertThat(schedulerId).startsWith("scheduler-");
        assertThat(environment.getProperty("scheduler.id")).isEqualTo(schedulerId);
    }

    @Test
    void blankIdFallsBackToHostname() {
        StandardEnvironment environment = environmentWith(Map.of("scheduler.id", "", "HOSTNAME", "scheduler-box"));

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("scheduler.id")).isEqualTo("scheduler-scheduler-box");
    }

    private static StandardEnvironment environmentWith(Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("test", new HashMap<>(properties)));
        return environment;
    }
}
