package com.chronos.scheduler.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Circuit breaker configuration for MongoDB operations.
 * 
 * Problem: When MongoDB is unavailable or experiencing issues, services
 * continue making requests that fail, wasting resources and delaying failure.
 * 
 * Solution: Circuit breaker pattern monitors failures and "opens" the circuit
 * when failure rate exceeds threshold. While open, requests fail fast without
 * attempting database calls.
 * 
 * States:
 * - CLOSED: Normal operation, all requests pass through
 * - OPEN: Failure threshold exceeded, all requests fail immediately
 * - HALF_OPEN: Testing if service recovered, limited requests pass through
 * 
 * Configuration:
 * - Failure rate threshold: 50% (open circuit if >50% of calls fail)
 * - Wait duration in open state: 30 seconds
 * - Sliding window: 100 calls for calculating failure rate
 * - Minimum number of calls: 10 (need at least 10 calls before opening)
 * - Permitted calls in half-open: 5 (test with 5 calls)
 * 
 * Metrics:
 * - Circuit breaker state changes logged
 * - Failure rate tracked
 * - Integration with Micrometer/Prometheus
 */
@Configuration
public class CircuitBreakerConfiguration {
    
    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerConfiguration.class);
    
    /**
     * Create circuit breaker registry with custom configuration.
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                // Open circuit if 50% of calls fail
                .failureRateThreshold(50)
                
                // Wait 30 seconds before attempting half-open
                .waitDurationInOpenState(Duration.ofSeconds(30))
                
                // Allow 5 calls in half-open state to test recovery
                .permittedNumberOfCallsInHalfOpenState(5)
                
                // Use sliding window of 100 calls for failure calculation
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                
                // Need at least 10 calls before calculating failure rate
                .minimumNumberOfCalls(10)
                
                // Automatically transition from open to half-open
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                
                // Record these exceptions as failures
                .recordExceptions(
                        org.springframework.dao.DataAccessResourceFailureException.class,
                        org.springframework.dao.TransientDataAccessException.class,
                        com.mongodb.MongoException.class
                )
                
                // Don't record these as failures (they're client errors)
                .ignoreExceptions(
                        IllegalArgumentException.class,
                        IllegalStateException.class
                )
                
                .build();
        
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        
        // Register event consumer to log state changes
        registry.getEventPublisher().onEntryAdded(new RegistryEventConsumer<CircuitBreaker>() {
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
                // No action needed
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
                // No action needed
            }
        });
        
        log.info("Circuit breaker registry initialized with config: failureThreshold={}%, " +
                        "waitDuration={}s, slidingWindow={}, minCalls={}",
                config.getFailureRateThreshold(),
                config.getWaitDurationInOpenState().getSeconds(),
                config.getSlidingWindowSize(),
                config.getMinimumNumberOfCalls());
        
        return registry;
    }
}
