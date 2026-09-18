package com.chronos.gateway.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes gateway errors in the same JSON shape as the backend services' error responses.
 */
public final class ErrorResponses {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ErrorResponses() {
    }

    public static Map<String, Object> body(HttpStatus status, String error, String message,
                                           HttpServletRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", error);
        body.put("message", message);
        body.put("path", request.getRequestURI());
        body.put("correlationId", request.getAttribute(CorrelationIds.ATTRIBUTE));
        return body;
    }

    public static void write(HttpServletResponse response, HttpStatus status, String error, String message,
                             HttpServletRequest request) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        MAPPER.writeValue(response.getOutputStream(), body(status, error, message, request));
    }

    /**
     * Correlation ID request attribute/header names.
     */
    public static final class CorrelationIds {
        public static final String HEADER = "X-Correlation-ID";
        public static final String ATTRIBUTE = "correlationId";

        private CorrelationIds() {
        }
    }
}
