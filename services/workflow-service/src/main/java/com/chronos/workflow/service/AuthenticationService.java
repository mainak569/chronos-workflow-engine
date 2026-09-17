package com.chronos.workflow.service;

import com.chronos.workflow.domain.User;
import com.chronos.workflow.dto.AuthResponse;
import com.chronos.workflow.dto.LoginRequest;
import com.chronos.workflow.dto.RegisterRequest;
import com.chronos.workflow.exception.AuthenticationException;
import com.chronos.workflow.exception.DuplicateResourceException;
import com.chronos.workflow.repository.UserRepository;
import com.chronos.workflow.security.JwtTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service for user authentication (registration and login).
 * 
 * Security:
 * - Passwords hashed with BCrypt before storage
 * - Email uniqueness enforced
 * - Failed login attempts logged
 * - JWT tokens generated for successful authentication
 */
@Service
public class AuthenticationService {
    
    private static final Logger log = LoggerFactory.getLogger(AuthenticationService.class);
    
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;
    
    public AuthenticationService(
            UserRepository userRepository,
            PasswordEncoder passwordEncoder,
            JwtTokenProvider jwtTokenProvider) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtTokenProvider = jwtTokenProvider;
    }
    
    /**
     * Register a new user.
     * 
     * Steps:
     * 1. Check email uniqueness
     * 2. Hash password with BCrypt
     * 3. Save user to database
     * 4. Generate JWT token
     * 5. Return authentication response
     * 
     * @param request registration request
     * @return authentication response with JWT token
     * @throws DuplicateResourceException if email already registered
     */
    @Transactional
    public AuthResponse register(RegisterRequest request) {
        log.info("Registering new user: email={}, username={}", request.getEmail(), request.getUsername());
        
        // Check if email already exists
        if (userRepository.existsByEmail(request.getEmail())) {
            log.warn("Registration failed: email already exists: {}", request.getEmail());
            throw new DuplicateResourceException("Email already registered: " + request.getEmail());
        }
        
        // Create new user
        User user = new User();
        user.setEmail(request.getEmail());
        user.setUsername(request.getUsername());
        
        // Hash password with BCrypt
        String hashedPassword = passwordEncoder.encode(request.getPassword());
        user.setPassword(hashedPassword);
        
        user.setEnabled(true);
        
        // Save to database
        user = userRepository.save(user);
        
        log.info("User registered successfully: userId={}, email={}", user.getId(), user.getEmail());
        
        // Generate JWT token
        String token = jwtTokenProvider.generateToken(user.getId(), user.getEmail());
        
        // Return authentication response
        return new AuthResponse(
                token,
                user.getId(),
                user.getEmail(),
                user.getUsername(),
                jwtTokenProvider.getExpirationMs()
        );
    }
    
    /**
     * Authenticate user and generate JWT token.
     * 
     * Steps:
     * 1. Find user by email
     * 2. Verify password with BCrypt
     * 3. Update last login time
     * 4. Generate JWT token
     * 5. Return authentication response
     * 
     * @param request login request
     * @return authentication response with JWT token
     * @throws AuthenticationException if credentials are invalid
     */
    @Transactional
    public AuthResponse login(LoginRequest request) {
        log.info("Login attempt: email={}", request.getEmail());
        
        // Find user by email
        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> {
                    log.warn("Login failed: user not found: {}", request.getEmail());
                    return new AuthenticationException("Invalid email or password");
                });
        
        // Check if account is enabled
        if (!user.isEnabled()) {
            log.warn("Login failed: account disabled: {}", request.getEmail());
            throw new AuthenticationException("Account is disabled");
        }
        
        // Verify password with BCrypt (timing-attack resistant)
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            log.warn("Login failed: invalid password: {}", request.getEmail());
            throw new AuthenticationException("Invalid email or password");
        }
        
        // Update last login time
        user.updateLastLogin();
        userRepository.save(user);
        
        log.info("Login successful: userId={}, email={}", user.getId(), user.getEmail());
        
        // Generate JWT token
        String token = jwtTokenProvider.generateToken(user.getId(), user.getEmail());
        
        // Return authentication response
        return new AuthResponse(
                token,
                user.getId(),
                user.getEmail(),
                user.getUsername(),
                jwtTokenProvider.getExpirationMs()
        );
    }
    
    /**
     * Validate JWT token and get user.
     * 
     * @param token JWT token
     * @return user if token is valid
     * @throws AuthenticationException if token is invalid
     */
    public User validateTokenAndGetUser(String token) {
        if (!jwtTokenProvider.validateToken(token)) {
            throw new AuthenticationException("Invalid or expired token");
        }
        
        String userId = jwtTokenProvider.getUserIdFromToken(token);
        
        return userRepository.findById(userId)
                .orElseThrow(() -> new AuthenticationException("User not found"));
    }
}
