package com.chronos.workflow.config;

import com.chronos.workflow.security.JwtAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Instant;

/**
 * Spring Security configuration for JWT-based authentication.
 * 
 * Configuration:
 * - Stateless session (no server-side session)
 * - JWT authentication filter before Spring's authentication filter
 * - Public endpoints: /auth/** , /actuator/health, /actuator/info, /actuator/prometheus, /livez, /readyz
 * - Missing/invalid token: 401, authenticated but not allowed: 403 (JSON bodies)
 * - All other endpoints require authentication
 * - CSRF disabled (not needed for JWT)
 * - BCrypt password encoder with strength 10
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {
    
    private final JwtAuthenticationFilter jwtAuthenticationFilter;
    
    public SecurityConfig(JwtAuthenticationFilter jwtAuthenticationFilter) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
    }
    
    /**
     * Configure HTTP security.
     * 
     * Security rules:
     * - /auth/** endpoints are public (registration, login)
     * - /actuator/health is public
     * - All other endpoints require authentication
     * - Sessions are stateless (JWT only)
     * - CSRF protection disabled (JWT doesn't need it)
     */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                // Disable CSRF (not needed for stateless JWT authentication)
                .csrf(csrf -> csrf.disable())
                
                // Stateless session management (no server-side sessions)
                .sessionManagement(session -> 
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                )
                
                // Authorization rules
                .authorizeHttpRequests(auth -> auth
                        // Public endpoints
                        .requestMatchers("/api/v1/auth/**").permitAll()
                        .requestMatchers("/actuator/health", "/actuator/health/**",
                                "/actuator/info", "/actuator/prometheus", "/livez", "/readyz").permitAll()
                        .requestMatchers("/swagger-ui/**", "/v3/api-docs/**").permitAll()
                        
                        // All other endpoints require authentication
                        .anyRequest().authenticated()
                )
                
                // Add JWT filter before Spring Security's authentication filter
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint((request, response, e) ->
                                writeError(request, response, HttpStatus.UNAUTHORIZED, "Authentication required"))
                        .accessDeniedHandler((request, response, e) ->
                                writeError(request, response, HttpStatus.FORBIDDEN, "Access denied"))
                )
                
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        
        return http.build();
    }
    
    /**
     * Password encoder bean.
     * 
     * Uses BCrypt with strength 10 (2^10 = 1024 iterations).
     * This provides good security while maintaining reasonable performance.
     * 
     * BCrypt automatically:
     * - Generates unique salt per password
     * - Stores salt in the hash
     * - Provides timing-attack resistance
     */
    private static void writeError(HttpServletRequest request, HttpServletResponse response,
                                   HttpStatus status, String message) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(String.format(
                "{\"timestamp\":\"%s\",\"status\":%d,\"error\":\"%s\",\"message\":\"%s\",\"path\":\"%s\"}",
                Instant.now(), status.value(), status.name(), message, request.getRequestURI().replace("\"", "")));
    }
    
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }
}
