package com.chronos.workflow.observability;

import org.slf4j.MDC;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * HTTP client interceptor to propagate correlation ID to downstream services.
 * 
 * Adds X-Correlation-ID header to outgoing HTTP requests with value from MDC.
 */
@Component
public class CorrelationIdInterceptor implements ClientHttpRequestInterceptor {
    
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String MDC_KEY = "correlation_id";
    
    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body,
                                       ClientHttpRequestExecution execution) throws IOException {
        
        // Get correlation ID from MDC
        String correlationId = MDC.get(MDC_KEY);
        
        // Add to request header if present
        if (correlationId != null && !correlationId.isEmpty()) {
            request.getHeaders().set(CORRELATION_ID_HEADER, correlationId);
        }
        
        return execution.execute(request, body);
    }
}
