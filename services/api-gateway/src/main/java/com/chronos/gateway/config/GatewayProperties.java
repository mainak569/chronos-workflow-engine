package com.chronos.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Gateway configuration.
 *
 * @param workflowServiceUrl base URL of workflow-service (e.g. http://workflow-service:8081)
 * @param connectTimeout     timeout for connecting to the backend
 * @param readTimeout        timeout for the backend response
 * @param rateLimit          per-client request limit
 */
@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(
        String workflowServiceUrl,
        Duration connectTimeout,
        Duration readTimeout,
        RateLimit rateLimit) {

    public GatewayProperties {
        if (workflowServiceUrl == null || workflowServiceUrl.isBlank()) {
            workflowServiceUrl = "http://localhost:8081";
        }
        workflowServiceUrl = workflowServiceUrl.replaceAll("/+$", "");
        connectTimeout = connectTimeout != null ? connectTimeout : Duration.ofSeconds(5);
        readTimeout = readTimeout != null ? readTimeout : Duration.ofSeconds(30);
        rateLimit = rateLimit != null ? rateLimit : new RateLimit(true, 120);
    }

    /**
     * @param enabled           whether rate limiting is applied
     * @param requestsPerMinute allowed requests per client (user ID, or IP for anonymous calls) per minute
     */
    public record RateLimit(boolean enabled, int requestsPerMinute) {
    }
}
