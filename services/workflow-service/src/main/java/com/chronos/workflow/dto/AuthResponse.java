package com.chronos.workflow.dto;

/**
 * Response DTO for authentication (login/register).
 */
public class AuthResponse {
    
    private String token;
    private String tokenType = "Bearer";
    private String userId;
    private String email;
    private String username;
    private long expiresIn;  // milliseconds
    
    // Constructors
    
    public AuthResponse() {
    }
    
    public AuthResponse(String token, String userId, String email, String username, long expiresIn) {
        this.token = token;
        this.userId = userId;
        this.email = email;
        this.username = username;
        this.expiresIn = expiresIn;
    }
    
    // Getters and Setters
    
    public String getToken() {
        return token;
    }
    
    public void setToken(String token) {
        this.token = token;
    }
    
    public String getTokenType() {
        return tokenType;
    }
    
    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
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
    
    public long getExpiresIn() {
        return expiresIn;
    }
    
    public void setExpiresIn(long expiresIn) {
        this.expiresIn = expiresIn;
    }
}
