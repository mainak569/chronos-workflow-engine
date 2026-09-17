package com.chronos.worker.retry;

import java.time.Duration;

/**
 * Retry policy with exponential backoff.
 * 
 * Formula: delay = baseDelay * (backoffMultiplier ^ attemptNumber)
 * Capped at maxDelay.
 * 
 * Example with baseDelay=5s, backoffMultiplier=6, maxDelay=5min:
 * - Attempt 1: 5s
 * - Attempt 2: 30s (5 * 6^1)
 * - Attempt 3: 3min (5 * 6^2 = 180s)
 * - Attempt 4+: 5min (capped)
 */
public class RetryPolicy {
    
    /**
     * Maximum number of retry attempts.
     * Total attempts = maxAttempts + 1 (initial attempt).
     */
    private final int maxAttempts;
    
    /**
     * Base delay before first retry.
     */
    private final Duration baseDelay;
    
    /**
     * Backoff multiplier for exponential backoff.
     */
    private final double backoffMultiplier;
    
    /**
     * Maximum delay between retries.
     */
    private final Duration maxDelay;
    
    /**
     * Whether to retry on all failures or only retriable failures.
     */
    private final boolean retryOnAllFailures;
    
    // Default policy constants
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final Duration DEFAULT_BASE_DELAY = Duration.ofSeconds(5);
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 6.0;
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(5);
    
    private RetryPolicy(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.baseDelay = builder.baseDelay;
        this.backoffMultiplier = builder.backoffMultiplier;
        this.maxDelay = builder.maxDelay;
        this.retryOnAllFailures = builder.retryOnAllFailures;
    }
    
    /**
     * Calculate delay before next retry attempt.
     * 
     * @param attemptNumber the current attempt number (1-based)
     * @return the delay duration
     */
    public Duration calculateDelay(int attemptNumber) {
        if (attemptNumber <= 0) {
            return Duration.ZERO;
        }
        
        // Exponential backoff: baseDelay * (multiplier ^ (attemptNumber - 1))
        long delaySeconds = (long) (baseDelay.getSeconds() * Math.pow(backoffMultiplier, attemptNumber - 1));
        Duration delay = Duration.ofSeconds(delaySeconds);
        
        // Cap at maxDelay
        if (delay.compareTo(maxDelay) > 0) {
            delay = maxDelay;
        }
        
        return delay;
    }
    
    /**
     * Check if retry is allowed for the given attempt number.
     * 
     * @param attemptNumber the current attempt number (1-based)
     * @return true if retry is allowed
     */
    public boolean canRetry(int attemptNumber) {
        return attemptNumber < maxAttempts;
    }
    
    /**
     * Check if the failure should be retried based on error type.
     * 
     * @param errorType the error type/category
     * @param retriable whether the error is marked as retriable
     * @return true if should retry
     */
    public boolean shouldRetry(String errorType, boolean retriable) {
        if (retryOnAllFailures) {
            return true;
        }
        
        // Only retry if explicitly marked as retriable
        return retriable;
    }
    
    // Getters
    
    public int getMaxAttempts() {
        return maxAttempts;
    }
    
    public Duration getBaseDelay() {
        return baseDelay;
    }
    
    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }
    
    public Duration getMaxDelay() {
        return maxDelay;
    }
    
    public boolean isRetryOnAllFailures() {
        return retryOnAllFailures;
    }
    
    /**
     * Create a default retry policy.
     */
    public static RetryPolicy defaultPolicy() {
        return builder().build();
    }
    
    /**
     * Create a builder for custom retry policy.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        private Duration baseDelay = DEFAULT_BASE_DELAY;
        private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
        private Duration maxDelay = DEFAULT_MAX_DELAY;
        private boolean retryOnAllFailures = false;
        
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }
        
        public Builder baseDelay(Duration baseDelay) {
            this.baseDelay = baseDelay;
            return this;
        }
        
        public Builder backoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }
        
        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }
        
        public Builder retryOnAllFailures(boolean retryOnAllFailures) {
            this.retryOnAllFailures = retryOnAllFailures;
            return this;
        }
        
        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }
    
    @Override
    public String toString() {
        return "RetryPolicy{" +
                "maxAttempts=" + maxAttempts +
                ", baseDelay=" + baseDelay +
                ", backoffMultiplier=" + backoffMultiplier +
                ", maxDelay=" + maxDelay +
                ", retryOnAllFailures=" + retryOnAllFailures +
                '}';
    }
}
