package com.chronos.workflow.controller;

import com.chronos.workflow.dto.AuthResponse;
import com.chronos.workflow.dto.LoginRequest;
import com.chronos.workflow.dto.RegisterRequest;
import com.chronos.workflow.service.AuthenticationService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for authentication operations.
 * Handles user registration and login.
 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final AuthenticationService authenticationService;

    public AuthController(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    /**
     * Register a new user.
     *
     * POST /api/v1/auth/register
     *
     * @param request Registration request containing username, email, and password
     * @return Authentication response with JWT token
     */
    @PostMapping("/register")
    public ResponseEntity<AuthResponse> register(
            @Valid @RequestBody RegisterRequest request) {

        log.info("Received registration request: email={}, username={}", 
                request.getEmail(), request.getUsername());

        AuthResponse response = authenticationService.register(request);

        log.info("User registered successfully: userId={}, email={}", 
                response.getUserId(), request.getEmail());

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Authenticate a user and generate JWT token.
     *
     * POST /api/v1/auth/login
     *
     * @param request Login request containing email and password
     * @return Authentication response with JWT token
     */
    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(
            @Valid @RequestBody LoginRequest request) {

        log.info("Received login request: email={}", request.getEmail());

        AuthResponse response = authenticationService.login(request);

        log.info("User logged in successfully: userId={}, email={}", 
                response.getUserId(), request.getEmail());

        return ResponseEntity.ok(response);
    }

    /**
     * Health check endpoint to verify authentication service is running.
     *
     * GET /api/v1/auth/health
     *
     * @return 200 OK with message
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Authentication service is running");
    }
}
