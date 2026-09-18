package com.chronos.gateway.filter;

import com.chronos.gateway.config.GatewayProperties;
import com.chronos.gateway.error.ErrorResponses;
import com.chronos.gateway.security.JwtValidator;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Cross-cutting gateway concerns for API requests:
 * 1. Correlation ID: reuse the caller's X-Correlation-ID or generate one, echo it in the response
 * 2. Authentication: every /api/** call except /api/v1/auth/** needs a valid Bearer JWT
 * 3. Rate limiting: per authenticated user, or per client IP for anonymous calls
 */
@Component
public class GatewayFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(GatewayFilter.class);

    public static final String USER_ID_ATTRIBUTE = "authenticatedUserId";

    private static final String PUBLIC_API_PREFIX = "/api/v1/auth/";

    private final JwtValidator jwtValidator;
    private final RateLimiter rateLimiter;
    private final boolean rateLimitEnabled;

    public GatewayFilter(JwtValidator jwtValidator, GatewayProperties properties) {
        this.jwtValidator = jwtValidator;
        this.rateLimiter = new RateLimiter(properties.rateLimit().requestsPerMinute());
        this.rateLimitEnabled = properties.rateLimit().enabled();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = Optional.ofNullable(request.getHeader(ErrorResponses.CorrelationIds.HEADER))
                .filter(id -> !id.isBlank() && id.length() <= 128)
                .orElseGet(() -> UUID.randomUUID().toString());
        request.setAttribute(ErrorResponses.CorrelationIds.ATTRIBUTE, correlationId);
        response.setHeader(ErrorResponses.CorrelationIds.HEADER, correlationId);
        MDC.put("correlation_id", correlationId);

        try {
            String userId = null;
            if (!request.getRequestURI().startsWith(PUBLIC_API_PREFIX)) {
                String header = request.getHeader("Authorization");
                if (header == null || !header.startsWith("Bearer ")) {
                    ErrorResponses.write(response, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED",
                            "Missing Bearer token", request);
                    return;
                }
                Optional<String> subject = jwtValidator.validate(header.substring(7));
                if (subject.isEmpty()) {
                    ErrorResponses.write(response, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED",
                            "Invalid or expired token", request);
                    return;
                }
                userId = subject.get();
                request.setAttribute(USER_ID_ATTRIBUTE, userId);
            }

            if (rateLimitEnabled) {
                String clientKey = userId != null ? "user:" + userId : "ip:" + request.getRemoteAddr();
                long now = System.currentTimeMillis();
                if (!rateLimiter.tryAcquire(clientKey, now)) {
                    log.warn("Rate limit exceeded: client={}, path={}", clientKey, request.getRequestURI());
                    response.setHeader("Retry-After", String.valueOf(rateLimiter.retryAfterSeconds(now)));
                    ErrorResponses.write(response, HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED",
                            "Too many requests, please retry later", request);
                    return;
                }
            }

            chain.doFilter(request, response);
        } finally {
            MDC.remove("correlation_id");
        }
    }
}
