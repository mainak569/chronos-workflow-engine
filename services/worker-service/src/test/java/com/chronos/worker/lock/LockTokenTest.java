package com.chronos.worker.lock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("LockToken Tests")
class LockTokenTest {
    
    @Test
    @DisplayName("Should create unique lock token with workerId and UUID")
    void shouldCreateUniqueLockToken() {
        // Given
        String workerId = "worker-123";
        
        // When
        LockToken token = LockToken.create(workerId);
        
        // Then
        assertThat(token).isNotNull();
        assertThat(token.getWorkerId()).isEqualTo(workerId);
        assertThat(token.getUuid()).isNotNull();
        assertThat(token.getToken()).startsWith(workerId + ":");
        assertThat(token.getToken()).contains(":");
    }
    
    @Test
    @DisplayName("Should create different UUIDs for same worker")
    void shouldCreateDifferentUuidsForSameWorker() {
        // Given
        String workerId = "worker-123";
        
        // When
        LockToken token1 = LockToken.create(workerId);
        LockToken token2 = LockToken.create(workerId);
        
        // Then
        assertThat(token1.getWorkerId()).isEqualTo(token2.getWorkerId());
        assertThat(token1.getUuid()).isNotEqualTo(token2.getUuid());
        assertThat(token1.getToken()).isNotEqualTo(token2.getToken());
    }
    
    @Test
    @DisplayName("Should parse token string correctly")
    void shouldParseTokenString() {
        // Given
        String tokenString = "worker-123:550e8400-e29b-41d4-a716-446655440000";
        
        // When
        LockToken token = LockToken.parse(tokenString);
        
        // Then
        assertThat(token).isNotNull();
        assertThat(token.getWorkerId()).isEqualTo("worker-123");
        assertThat(token.getUuid()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(token.getToken()).isEqualTo(tokenString);
    }
    
    @Test
    @DisplayName("Should handle workerId with colons in parsing")
    void shouldHandleWorkerIdWithColons() {
        // Given
        String tokenString = "host:port:worker-123:550e8400-e29b-41d4-a716-446655440000";
        
        // When
        LockToken token = LockToken.parse(tokenString);
        
        // Then
        assertThat(token).isNotNull();
        assertThat(token.getWorkerId()).isEqualTo("host:port:worker-123");
        assertThat(token.getUuid()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }
    
    @Test
    @DisplayName("Should return null for invalid token format")
    void shouldReturnNullForInvalidFormat() {
        // When/Then
        assertThat(LockToken.parse(null)).isNull();
        assertThat(LockToken.parse("")).isNull();
        assertThat(LockToken.parse("no-separator")).isNull();
    }
    
    @Test
    @DisplayName("Should check if token belongs to worker")
    void shouldCheckIfTokenBelongsToWorker() {
        // Given
        LockToken token = LockToken.create("worker-123");
        
        // When/Then
        assertThat(token.belongsTo("worker-123")).isTrue();
        assertThat(token.belongsTo("worker-456")).isFalse();
    }
    
    @Test
    @DisplayName("Should implement equals and hashCode correctly")
    void shouldImplementEqualsAndHashCode() {
        // Given
        LockToken token1 = LockToken.create("worker-1");
        LockToken token2 = LockToken.create("worker-1");
        LockToken token3 = LockToken.parse(token1.getToken());
        
        // Then
        assertThat(token1).isNotEqualTo(token2); // Different UUIDs
        assertThat(token1).isEqualTo(token3);     // Same token string
        assertThat(token1.hashCode()).isEqualTo(token3.hashCode());
    }
    
    @Test
    @DisplayName("Should produce readable toString")
    void shouldProduceReadableToString() {
        // Given
        LockToken token = LockToken.create("worker-123");
        
        // When
        String toString = token.toString();
        
        // Then
        assertThat(toString).isEqualTo(token.getToken());
        assertThat(toString).startsWith("worker-123:");
    }
    
    @Test
    @DisplayName("Should throw NPE for null workerId")
    void shouldThrowNpeForNullWorkerId() {
        // When/Then
        assertThatThrownBy(() -> LockToken.create(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("workerId");
    }
}
