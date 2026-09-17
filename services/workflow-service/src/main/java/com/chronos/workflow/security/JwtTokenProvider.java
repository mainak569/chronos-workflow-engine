package com.chronos.workflow.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * JWT token provider for generating and validating JWT tokens.
 * 
 * Security:
 * - Uses HS256 (HMAC with SHA-256)
 * - Secret key from environment variable
 * - Tokens expire after configured time (default 24 hours)
 * - No sensitive data in JWT payload
 */
@Component
public class JwtTokenProvider {
    
    private static final Logger log = LoggerFactory.getLogger(JwtTokenProvider.class);
    
    private final String jwtSecret;
    private final long jwtExpirationMs;
    
    /**
     * Constructor for Spring injection with @Value annotations.
     */
    public JwtTokenProvider(
            @Value("${chronos.security.jwt.secret}") String jwtSecret,
            @Value("${chronos.security.jwt.expiration:86400000}") long jwtExpirationMs) {
        this.jwtSecret = jwtSecret;
        this.jwtExpirationMs = jwtExpirationMs;
    }
    
    /**
     * Generate JWT token for authenticated user.
     * 
     * Token contains:
     * - sub (subject): user ID
     * - email: user's email
     * - iat (issued at): token creation time
     * - exp (expiration): token expiration time
     * 
     * @param userId user identifier
     * @param email user email
     * @return JWT token string
     */
    public String generateToken(String userId, String email) {
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + jwtExpirationMs);
        
        SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
        
        String token = Jwts.builder()
                .subject(userId)
                .claim("email", email)
                .issuedAt(now)
                .expiration(expiryDate)
                .signWith(key)
                .compact();
        
        log.debug("Generated JWT token for user: userId={}, expiresIn={}ms", userId, jwtExpirationMs);
        
        return token;
    }
    
    /**
     * Extract user ID from JWT token.
     * 
     * @param token JWT token
     * @return user ID from token's subject claim
     */
    public String getUserIdFromToken(String token) {
        try {
            SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
            
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();
            
            return claims.getSubject();
            
        } catch (JwtException e) {
            log.error("Failed to extract user ID from token", e);
            throw new RuntimeException("Invalid JWT token", e);
        }
    }
    
    /**
     * Extract email from JWT token.
     * 
     * @param token JWT token
     * @return email from token claims
     */
    public String getEmailFromToken(String token) {
        try {
            SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
            
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();
            
            return claims.get("email", String.class);
            
        } catch (JwtException e) {
            log.error("Failed to extract email from token", e);
            throw new RuntimeException("Invalid JWT token", e);
        }
    }
    
    /**
     * Validate JWT token.
     * 
     * Checks:
     * - Signature is valid
     * - Token is not expired
     * - Token structure is correct
     * 
     * @param token JWT token to validate
     * @return true if token is valid, false otherwise
     */
    public boolean validateToken(String token) {
        try {
            SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
            
            Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token);
            
            return true;
            
        } catch (ExpiredJwtException e) {
            log.warn("JWT token expired: {}", e.getMessage());
            return false;
        } catch (UnsupportedJwtException e) {
            log.error("Unsupported JWT token: {}", e.getMessage());
            return false;
        } catch (MalformedJwtException e) {
            log.error("Malformed JWT token: {}", e.getMessage());
            return false;
        } catch (io.jsonwebtoken.security.SignatureException e) {
            log.error("Invalid JWT signature: {}", e.getMessage());
            return false;
        } catch (IllegalArgumentException e) {
            log.error("JWT token compact of handler are invalid: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Get JWT expiration time in milliseconds.
     * 
     * @return expiration time in ms
     */
    public long getExpirationMs() {
        return jwtExpirationMs;
    }
}
