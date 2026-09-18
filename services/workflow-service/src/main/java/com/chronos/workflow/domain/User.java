package com.chronos.workflow.domain;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * User entity for authentication and authorization.
 * 
 * Security:
 * - Password stored as BCrypt hash (never plaintext)
 * - Email used as unique identifier for login
 * - Username for display purposes
 * - Enabled flag for account activation/deactivation
 */
@Document(collection = "users")
public class User {
    
    /**
     * Unique user identifier.
     */
    @Id
    private String id;
    
    /**
     * User's email address (unique, used for login).
     */
    // Name matches MongoDB's default ("email_1") so it does not clash with indexes
    // created outside the application (e.g. by an older init-mongo.js)
    @Indexed(name = "email_1", unique = true)
    private String email;
    
    /**
     * Username for display.
     */
    private String username;
    
    /**
     * BCrypt hashed password.
     * NEVER store plaintext passwords.
     * Hash format: $2a$10$xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
     */
    private String password;
    
    /**
     * Whether the account is enabled.
     * Can be used for email verification or account suspension.
     */
    private boolean enabled;
    
    /**
     * Account creation timestamp.
     */
    @CreatedDate
    private Instant createdAt;
    
    /**
     * Last modification timestamp.
     */
    @LastModifiedDate
    private Instant updatedAt;
    
    /**
     * Last login timestamp.
     */
    private Instant lastLoginAt;
    
    // Constructors
    
    public User() {
        this.enabled = true;
    }
    
    public User(String email, String username, String password) {
        this.email = email;
        this.username = username;
        this.password = password;
        this.enabled = true;
    }
    
    // Business Methods
    
    /**
     * Mark user as logged in.
     */
    public void updateLastLogin() {
        this.lastLoginAt = Instant.now();
    }
    
    /**
     * Disable user account.
     */
    public void disable() {
        this.enabled = false;
    }
    
    /**
     * Enable user account.
     */
    public void enable() {
        this.enabled = true;
    }
    
    // Getters and Setters
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
    
    public Instant getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public Instant getLastLoginAt() {
        return lastLoginAt;
    }
    
    public void setLastLoginAt(Instant lastLoginAt) {
        this.lastLoginAt = lastLoginAt;
    }
    
    @Override
    public String toString() {
        return String.format("User{id='%s', email='%s', username='%s', enabled=%s}",
                id, email, username, enabled);
    }
    
    // Builder
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private String email;
        private String username;
        private String password;
        private boolean enabled = true;
        private Instant createdAt;
        private Instant updatedAt;
        private Instant lastLoginAt;
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder email(String email) {
            this.email = email;
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }
        
        public Builder lastLoginAt(Instant lastLoginAt) {
            this.lastLoginAt = lastLoginAt;
            return this;
        }
        
        public User build() {
            User user = new User();
            user.setId(id);
            user.setEmail(email);
            user.setUsername(username);
            user.setPassword(password);
            user.setEnabled(enabled);
            user.setCreatedAt(createdAt);
            user.setUpdatedAt(updatedAt);
            user.setLastLoginAt(lastLoginAt);
            return user;
        }
    }
}
