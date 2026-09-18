package com.chronos.gateway.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class JwtValidatorTest {

    private static final String SECRET = "test-secret-key-for-jwt-signing-must-be-at-least-256-bits-long";

    private final JwtValidator validator = new JwtValidator(SECRET);

    @Test
    void acceptsTokenSignedWithTheSharedSecret() {
        assertThat(validator.validate(token(SECRET, "user-1", 60_000))).hasValue("user-1");
    }

    @Test
    void rejectsTokenSignedWithAnotherSecret() {
        String forged = token("another-secret-key-that-is-also-long-enough-for-hmac-sha", "user-1", 60_000);

        assertThat(validator.validate(forged)).isEmpty();
    }

    @Test
    void rejectsExpiredToken() {
        assertThat(validator.validate(token(SECRET, "user-1", -1_000))).isEmpty();
    }

    @Test
    void rejectsGarbage() {
        assertThat(validator.validate("not-a-jwt")).isEmpty();
    }

    private static String token(String secret, String subject, long validForMs) {
        return Jwts.builder()
                .subject(subject)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + validForMs))
                .signWith(Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8)))
                .compact();
    }
}
