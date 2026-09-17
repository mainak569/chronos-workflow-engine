package com.chronos.workflow.security;

import com.chronos.workflow.domain.User;
import com.chronos.workflow.repository.UserRepository;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;

/**
 * JWT Authentication Filter.
 * 
 * Processes JWT tokens from Authorization header and sets Spring Security context.
 * 
 * Flow:
 * 1. Extract JWT from Authorization: Bearer <token>
 * 2. Validate JWT signature and expiration
 * 3. Extract user ID from token
 * 4. Load user from database
 * 5. Create Authentication object
 * 6. Set in SecurityContext for this request
 * 
 * This filter runs once per request before any controller processing.
 */
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    
    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationFilter.class);
    
    @Autowired
    private JwtTokenProvider tokenProvider;
    
    @Autowired
    private UserRepository userRepository;
    
    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                   HttpServletResponse response,
                                   FilterChain filterChain) throws ServletException, IOException {
        try {
            // Extract JWT from request
            String jwt = getJwtFromRequest(request);
            
            if (StringUtils.hasText(jwt) && tokenProvider.validateToken(jwt)) {
                // Get user ID from token
                String userId = tokenProvider.getUserIdFromToken(jwt);
                
                // Load user from database
                User user = userRepository.findById(userId).orElse(null);
                
                if (user != null && user.isEnabled()) {
                    // Create UserDetails
                    UserDetails userDetails = org.springframework.security.core.userdetails.User
                            .withUsername(userId)
                            .password(user.getPassword())
                            .authorities(new ArrayList<>())  // No roles for now
                            .accountExpired(false)
                            .accountLocked(false)
                            .credentialsExpired(false)
                            .disabled(!user.isEnabled())
                            .build();
                    
                    // Create authentication
                    UsernamePasswordAuthenticationToken authentication =
                            new UsernamePasswordAuthenticationToken(
                                    userDetails,
                                    null,
                                    userDetails.getAuthorities()
                            );
                    
                    authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                    
                    // Set authentication in SecurityContext
                    SecurityContextHolder.getContext().setAuthentication(authentication);
                    
                    log.debug("Set authentication for user: userId={}", userId);
                } else {
                    log.warn("User not found or disabled: userId={}", userId);
                }
            }
        } catch (Exception e) {
            log.error("Cannot set user authentication", e);
        }
        
        filterChain.doFilter(request, response);
    }
    
    /**
     * Extract JWT token from Authorization header.
     * 
     * Expected format: Authorization: Bearer <token>
     * 
     * @param request HTTP request
     * @return JWT token or null if not present
     */
    private String getJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);
        }
        
        return null;
    }
}
