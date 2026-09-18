package com.chronos.scheduler.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Protects the scheduler's operational REST API (/api/**) with a shared admin token.
 *
 * Requests must send the token in the X-Admin-Token header. When no token is
 * configured (chronos.admin.token / CHRONOS_ADMIN_TOKEN), the admin API is disabled.
 */
@Configuration
public class AdminApiSecurityConfig implements WebMvcConfigurer {

    static final String ADMIN_TOKEN_HEADER = "X-Admin-Token";

    private final String adminToken;

    public AdminApiSecurityConfig(@Value("${chronos.admin.token:}") String adminToken) {
        this.adminToken = adminToken;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new AdminTokenInterceptor()).addPathPatterns("/api/**");
    }

    private class AdminTokenInterceptor implements HandlerInterceptor {

        @Override
        public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
                throws Exception {
            if (adminToken == null || adminToken.isBlank()) {
                return reject(response, HttpStatus.FORBIDDEN, "Admin API is disabled (no admin token configured)");
            }
            String provided = request.getHeader(ADMIN_TOKEN_HEADER);
            if (provided == null || !MessageDigest.isEqual(
                    provided.getBytes(StandardCharsets.UTF_8), adminToken.getBytes(StandardCharsets.UTF_8))) {
                return reject(response, HttpStatus.UNAUTHORIZED, "Missing or invalid " + ADMIN_TOKEN_HEADER + " header");
            }
            return true;
        }

        private boolean reject(HttpServletResponse response, HttpStatus status, String message) throws Exception {
            response.setStatus(status.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().write("{\"status\":\"error\",\"message\":\"" + message + "\"}");
            return false;
        }
    }
}
