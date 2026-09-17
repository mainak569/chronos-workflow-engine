package com.chronos.workflow.observability;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Filter to handle correlation ID for distributed tracing.
 * 
 * Behavior:
 * - Extracts correlation ID from X-Correlation-ID header
 * - Generates new UUID if header is missing
 * - Stores in MDC (Mapped Diagnostic Context) for logging
 * - Adds to response header for client tracking
 * - Clears MDC after request completion
 */
@Component
@Order(1) // Execute before other filters
public class CorrelationIdFilter implements Filter {
    
    private static final Logger log = LoggerFactory.getLogger(CorrelationIdFilter.class);
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String MDC_KEY = "correlation_id";
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        try {
            // Extract or generate correlation ID
            String correlationId = httpRequest.getHeader(CORRELATION_ID_HEADER);
            
            if (correlationId == null || correlationId.trim().isEmpty()) {
                correlationId = UUID.randomUUID().toString();
                log.debug("Generated new correlation ID: {}", correlationId);
            } else {
                log.debug("Using existing correlation ID: {}", correlationId);
            }
            
            // Store in MDC for automatic inclusion in logs
            MDC.put(MDC_KEY, correlationId);
            
            // Add to response header
            httpResponse.setHeader(CORRELATION_ID_HEADER, correlationId);
            
            // Continue filter chain
            chain.doFilter(request, response);
            
        } finally {
            // Always clear MDC to prevent leaking to other requests
            MDC.clear();
        }
    }
    
    /**
     * Get current correlation ID from MDC.
     *
     * @return Current correlation ID, or null if not set
     */
    public static String getCurrentCorrelationId() {
        return MDC.get(MDC_KEY);
    }
}
