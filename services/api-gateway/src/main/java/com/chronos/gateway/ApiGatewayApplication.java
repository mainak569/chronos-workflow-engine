package com.chronos.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * API Gateway Application
 * 
 * Responsibilities:
 * - Authentication and authorization
 * - Request routing to backend services
 * - API-level rate limiting
 * - Correlation ID injection
 * - Consistent error response formatting
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
