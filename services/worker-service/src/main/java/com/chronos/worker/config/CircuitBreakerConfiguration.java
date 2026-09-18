package com.chronos.worker.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Circuit breaker configuration for worker service MongoDB operations.
 * 
 * Same configuration as scheduler service to ensure consistent
 * failure handling across all Chronos services.
 */
@Configuration
public class CircuitBreakerConfiguration {
    
    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerConfiguration.class);
    
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(MeterRegistry meterRegistry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(10)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(
                        org.springframework.dao.DataAccessResourceFailureException.class,
                        org.springframework.dao.TransientDataAccessException.class,
                        com.mongodb.MongoException.class
                )
                .ignoreExceptions(
                        IllegalArgumentException.class,
                        IllegalStateException.class
                )
                .build();
        
        RegistryEventConsumer<CircuitBreaker> stateChangeLogger = new RegistryEventConsumer<>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> event) {
                CircuitBreaker cb = event.getAddedEntry();
                cb.getEventPublisher()
                        .onStateTransition(e -> log.warn("Circuit breaker '{}' state changed: {} -> {}",
                                cb.getName(), e.getStateTransition().getFromState(),
                                e.getStateTransition().getToState()))
                        .onFailureRateExceeded(e -> log.error("Circuit breaker '{}' failure rate exceeded: {}%",
                                cb.getName(), e.getFailureRate()))
                        .onError(e -> log.debug("Circuit breaker '{}' recorded error: {}",
                                cb.getName(), e.getThrowable().getClass().getSimpleName()));
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
            }
        };

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config, stateChangeLogger);

        // Export state, call and failure-rate metrics (resilience4j_circuitbreaker_*) to Prometheus;
        // this registry replaces the auto-configured one, so the binding has to be done here
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        
        log.info("Circuit breaker registry initialized");
        
        return registry;
    }
}
