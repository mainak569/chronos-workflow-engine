package com.chronos.workflow.domain;

import jakarta.validation.constraints.Min;

/**
 * Retry configuration for task execution.
 * Defines how many times to retry and with what backoff strategy.
 */
public class RetryConfiguration {

    /**
     * Maximum number of retry attempts.
     * After this many failures, task moves to DEAD status.
     */
    @Min(value = 0, message = "Max attempts must be non-negative")
    private Integer maxAttempts = 3;

    /**
     * Initial delay before first retry (in milliseconds).
     */
    @Min(value = 0, message = "Initial delay must be non-negative")
    private Long initialDelayMs = 5000L; // 5 seconds

    /**
     * Multiplier for exponential backoff.
     * Delay for retry N = initialDelayMs * (backoffMultiplier ^ (N-1))
     */
    @Min(value = 1, message = "Backoff multiplier must be at least 1")
    private Double backoffMultiplier = 2.0;

    /**
     * Maximum delay between retries (in milliseconds).
     * Caps the exponential growth.
     */
    @Min(value = 0, message = "Max delay must be non-negative")
    private Long maxDelayMs = 300000L; // 5 minutes
    
    // Constructors
    public RetryConfiguration() {
    }
    
    public RetryConfiguration(Integer maxAttempts, Long initialDelayMs, Double backoffMultiplier, Long maxDelayMs) {
        this.maxAttempts = maxAttempts != null ? maxAttempts : 3;
        this.initialDelayMs = initialDelayMs != null ? initialDelayMs : 5000L;
        this.backoffMultiplier = backoffMultiplier != null ? backoffMultiplier : 2.0;
        this.maxDelayMs = maxDelayMs != null ? maxDelayMs : 300000L;
    }
    
    // Getters and Setters
    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(Long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public Double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(Double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Long getMaxDelayMs() {
        return maxDelayMs;
    }

    public void setMaxDelayMs(Long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }
    
    // Builder
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Integer maxAttempts = 3;
        private Long initialDelayMs = 5000L;
        private Double backoffMultiplier = 2.0;
        private Long maxDelayMs = 300000L;
        
        public Builder maxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }
        
        public Builder initialDelayMs(Long initialDelayMs) {
            this.initialDelayMs = initialDelayMs;
            return this;
        }
        
        public Builder backoffMultiplier(Double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }
        
        public Builder maxDelayMs(Long maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
            return this;
        }
        
        public RetryConfiguration build() {
            return new RetryConfiguration(maxAttempts, initialDelayMs, backoffMultiplier, maxDelayMs);
        }
    }

    /**
     * Calculate the delay for a specific retry attempt.
     *
     * @param attempt The retry attempt number (1-based)
     * @return Delay in milliseconds
     */
    public long calculateDelay(int attempt) {
        if (attempt <= 0) {
            return 0L;
        }

        long delay = (long) (initialDelayMs * Math.pow(backoffMultiplier, attempt - 1));
        return Math.min(delay, maxDelayMs);
    }
}
