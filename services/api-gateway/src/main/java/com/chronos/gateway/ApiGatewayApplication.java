package com.chronos.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * API Gateway Application
 * 
 * Responsibilities:
 * - Authentication (JWT validation before requests reach backend services)
 * - Request routing to backend services
 * - API-level rate limiting
 * - Correlation ID injection
 * - Consistent error response formatting
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
