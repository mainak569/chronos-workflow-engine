package com.chronos.workflow.aspect;

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
    
    @Around("execution(* com.chronos.workflow.repository..*(..))")
    public Object aroundRepositoryMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = joinPoint.getSignature().toShortString();
        
        Supplier<Object> supplier = () -> {
            try {
                return joinPoint.proceed();
            } catch (Throwable e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException("Repository operation failed", e);
            }
        };
        
        try {
            return circuitBreaker.executeSupplier(supplier);
        } catch (CallNotPermittedException e) {
            log.error("Circuit breaker OPEN - MongoDB unavailable: {}", methodName);
            throw new DataAccessResourceFailureException(
                    "MongoDB circuit breaker is OPEN - service temporarily unavailable", e);
        }
    }
}
