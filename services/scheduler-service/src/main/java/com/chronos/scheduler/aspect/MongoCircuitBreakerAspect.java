package com.chronos.scheduler.aspect;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Aspect that applies circuit breaker to all MongoDB repository operations.
 * 
 * This automatically wraps all repository method calls with circuit breaker logic,
 * preventing cascading failures when MongoDB is unavailable.
 * 
 * How it works:
 * 1. Intercepts all method calls on classes in repository package
 * 2. Wraps the call with circuit breaker
 * 3. If circuit is OPEN, fails fast with CallNotPermittedException
 * 4. If circuit is CLOSED/HALF_OPEN, executes the call
 * 5. Records success/failure for circuit breaker state management
 * 
 * Benefits:
 * - No code changes needed in repositories or services
 * - Consistent circuit breaker behavior across all database calls
 * - Fail-fast when MongoDB is down
 * - Automatic recovery when MongoDB comes back online
 */
@Aspect
@Component
public class MongoCircuitBreakerAspect {
    
    private static final Logger log = LoggerFactory.getLogger(MongoCircuitBreakerAspect.class);
    
    private final CircuitBreaker circuitBreaker;
    
    public MongoCircuitBreakerAspect(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("mongodb");
        log.info("MongoDB circuit breaker aspect initialized");
    }
    
    /**
     * Intercept all repository method calls and apply circuit breaker.
     * 
     * Pointcut: execution(* com.chronos.scheduler.repository..*(..))
     * Matches: All methods in any class in repository package or subpackages
     */
    @Around("execution(* com.chronos.scheduler.repository..*(..))")
    public Object aroundRepositoryMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = joinPoint.getSignature().toShortString();
        
        // Wrap repository call with circuit breaker
        Supplier<Object> supplier = () -> {
            try {
                return joinPoint.proceed();
            } catch (Throwable e) {
                // Convert checked exceptions to runtime for circuit breaker
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException("Repository operation failed", e);
            }
        };
        
        try {
            Object result = circuitBreaker.executeSupplier(supplier);
            log.trace("Repository operation succeeded: {}", methodName);
            return result;
            
        } catch (CallNotPermittedException e) {
            // Circuit is OPEN - fail fast
            log.error("Circuit breaker OPEN - MongoDB unavailable: {}", methodName);
            throw new DataAccessResourceFailureException(
                    "MongoDB circuit breaker is OPEN - service temporarily unavailable", e);
            
        } catch (Exception e) {
            log.error("Repository operation failed: {}", methodName, e);
            throw e;
        }
    }
    
    /**
     * Get current circuit breaker state for monitoring.
     * 
     * @return circuit breaker state (CLOSED, OPEN, HALF_OPEN)
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
    
    /**
     * Get circuit breaker metrics for monitoring.
     * 
     * @return circuit breaker metrics
     */
    public CircuitBreaker.Metrics getMetrics() {
        return circuitBreaker.getMetrics();
    }
}
