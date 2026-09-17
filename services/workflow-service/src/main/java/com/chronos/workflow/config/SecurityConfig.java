package com.chronos.workflow.config;

import com.chronos.workflow.security.JwtAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Spring Security configuration for JWT-based authentication.
 * 
 * Configuration:
 * - Stateless session (no server-side session)
 * - JWT authentication filter before Spring's authentication filter
 * - Public endpoints: /auth/** , /actuator/health
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
                        .requestMatchers("/actuator/health").permitAll()
                        .requestMatchers("/swagger-ui/**", "/v3/api-docs/**").permitAll()
                        
                        // All other endpoints require authentication
                        .anyRequest().authenticated()
                )
                
                // Add JWT filter before Spring Security's authentication filter
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
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }
}
