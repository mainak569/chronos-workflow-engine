package com.chronos.workflow.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for JwtTokenProvider.
 * Tests token generation, validation, and claims extraction.
 */
class JwtTokenProviderTest {

    private JwtTokenProvider jwtTokenProvider;
    private String jwtSecret;
    private long jwtExpiration;

    @BeforeEach
    void setUp() {
        jwtSecret = "test-secret-key-for-jwt-signing-must-be-at-least-256-bits-long-for-hs256";
        jwtExpiration = 3600000; // 1 hour
        jwtTokenProvider = new JwtTokenProvider(jwtSecret, jwtExpiration);
    }

    @Test
    @DisplayName("Should generate valid JWT token")
    void shouldGenerateValidToken() {
        // Given
        String userId = "user-123";
        String email = "john@example.com";

        // When
        String token = jwtTokenProvider.generateToken(userId, email);

        // Then
        assertThat(token).isNotNull();
        assertThat(token).isNotEmpty();
        assertThat(token.split("\\.")).hasSize(3); // JWT has 3 parts: header.payload.signature

        // Verify token can be parsed
        SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
        Claims claims = Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();

        assertThat(claims.getSubject()).isEqualTo(userId);
        assertThat(claims.get("email", String.class)).isEqualTo(email);
        assertThat(claims.getIssuedAt()).isNotNull();
        assertThat(claims.getExpiration()).isNotNull();
    }

    @Test
    @DisplayName("Should validate valid JWT token")
    void shouldValidateValidToken() {
        // Given
        String token = jwtTokenProvider.generateToken("user-123", "john@example.com");

        // When
        boolean isValid = jwtTokenProvider.validateToken(token);

        // Then
        assertThat(isValid).isTrue();
    }

    @Test
    @DisplayName("Should reject invalid JWT token")
    void shouldRejectInvalidToken() {
        // Given
        String invalidToken = "invalid.jwt.token";

        // When
        boolean isValid = jwtTokenProvider.validateToken(invalidToken);

        // Then
        assertThat(isValid).isFalse();
    }

    @Test
    @DisplayName("Should reject token signed with different secret")
    void shouldRejectTokenWithDifferentSecret() {
        // Given - generate token with different secret
        String differentSecret = "different-secret-key-for-jwt-signing-must-be-at-least-256-bits-long";
        SecretKey key = Keys.hmacShaKeyFor(differentSecret.getBytes(StandardCharsets.UTF_8));
        
        String token = Jwts.builder()
                .subject("user-123")
                .claim("email", "john@example.com")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + jwtExpiration))
                .signWith(key)
                .compact();

        // When
        boolean isValid = jwtTokenProvider.validateToken(token);

        // Then
        assertThat(isValid).isFalse();
    }

    @Test
    @DisplayName("Should reject expired JWT token")
    void shouldRejectExpiredToken() {
        // Given - create token with past expiration
        SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
        String expiredToken = Jwts.builder()
                .subject("user-123")
                .claim("email", "john@example.com")
                .issuedAt(new Date(System.currentTimeMillis() - 7200000)) // 2 hours ago
                .expiration(new Date(System.currentTimeMillis() - 3600000)) // 1 hour ago
                .signWith(key)
                .compact();

        // When
        boolean isValid = jwtTokenProvider.validateToken(expiredToken);

        // Then
        assertThat(isValid).isFalse();
    }

    @Test
    @DisplayName("Should extract user ID from token")
    void shouldExtractUserIdFromToken() {
        // Given
        String userId = "user-123";
        String token = jwtTokenProvider.generateToken(userId, "john@example.com");

        // When
        String extractedUserId = jwtTokenProvider.getUserIdFromToken(token);

        // Then
        assertThat(extractedUserId).isEqualTo(userId);
    }

    @Test
    @DisplayName("Should extract email from token")
    void shouldExtractEmailFromToken() {
        // Given
        String email = "john@example.com";
        String token = jwtTokenProvider.generateToken("user-123", email);

        // When
        String extractedEmail = jwtTokenProvider.getEmailFromToken(token);

        // Then
        assertThat(extractedEmail).isEqualTo(email);
    }

    @Test
    @DisplayName("Should set correct expiration time")
    void shouldSetCorrectExpirationTime() {
        // Given
        long beforeGeneration = System.currentTimeMillis();
        String token = jwtTokenProvider.generateToken("user-123", "john@example.com");
        long afterGeneration = System.currentTimeMillis();

        // When - parse token to get expiration
        SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
        Claims claims = Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();

        long expirationTime = claims.getExpiration().getTime();
        long issuedAtTime = claims.getIssuedAt().getTime();

        // Then
        assertThat(expirationTime - issuedAtTime).isEqualTo(jwtExpiration);
        assertThat(expirationTime).isBetween(
                beforeGeneration + jwtExpiration - 1000, // Allow 1 second margin
                afterGeneration + jwtExpiration + 1000
        );
    }

    @Test
    @DisplayName("Should generate different tokens for different users")
    void shouldGenerateDifferentTokensForDifferentUsers() {
        // Given
        String token1 = jwtTokenProvider.generateToken("user-1", "user1@example.com");
        String token2 = jwtTokenProvider.generateToken("user-2", "user2@example.com");

        // Then
        assertThat(token1).isNotEqualTo(token2);

        String userId1 = jwtTokenProvider.getUserIdFromToken(token1);
        String userId2 = jwtTokenProvider.getUserIdFromToken(token2);

        assertThat(userId1).isEqualTo("user-1");
        assertThat(userId2).isEqualTo("user-2");
    }

    @Test
    @DisplayName("Should generate different tokens on multiple calls")
    void shouldGenerateDifferentTokensOnMultipleCalls() {
        // Given - generate two tokens for same user at different times
        String token1 = jwtTokenProvider.generateToken("user-123", "john@example.com");
        
        try {
            Thread.sleep(1000); // 1 second delay to ensure different issued-at time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        String token2 = jwtTokenProvider.generateToken("user-123", "john@example.com");

        // Then
        assertThat(token1).isNotEqualTo(token2); // Different due to different iat (issued-at)
    }

    @Test
    @DisplayName("Should handle null token gracefully")
    void shouldHandleNullToken() {
        // When / Then
        assertThat(jwtTokenProvider.validateToken(null)).isFalse();
        
        assertThatThrownBy(() -> jwtTokenProvider.getUserIdFromToken(null))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Should handle empty token gracefully")
    void shouldHandleEmptyToken() {
        // When / Then
        assertThat(jwtTokenProvider.validateToken("")).isFalse();
        
        assertThatThrownBy(() -> jwtTokenProvider.getUserIdFromToken(""))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Should reject malformed token")
    void shouldRejectMalformedToken() {
        // Given
        String malformedToken = "this.is.malformed";

        // When / Then
        assertThat(jwtTokenProvider.validateToken(malformedToken)).isFalse();
    }

    @Test
    @DisplayName("Should use HS512 algorithm")
    void shouldUseHS512Algorithm() {
        // Given
        String token = jwtTokenProvider.generateToken("user-123", "john@example.com");

        // When - decode header (base64)
        String header = token.split("\\.")[0];
        String decodedHeader = new String(java.util.Base64.getUrlDecoder().decode(header));

        // Then - JJWT 0.12.x uses HS512 by default for HMAC
        assertThat(decodedHeader).contains("\"alg\":\"HS512\"");
    }
}
