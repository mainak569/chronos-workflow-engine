package com.chronos.workflow.controller;

import com.chronos.workflow.domain.User;
import com.chronos.workflow.dto.AuthResponse;
import com.chronos.workflow.dto.LoginRequest;
import com.chronos.workflow.dto.RegisterRequest;
import com.chronos.workflow.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for AuthController.
 * Tests the complete authentication flow including database operations.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AuthControllerIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("chronos.security.jwt.secret", 
                () -> "test-secret-key-for-jwt-signing-must-be-at-least-256-bits-long");
        registry.add("chronos.security.jwt.expiration", () -> "3600000"); // 1 hour
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        userRepository.deleteAll();
    }

    @Test
    @Order(1)
    @DisplayName("Should register new user successfully")
    void shouldRegisterNewUser() throws Exception {
        // Given
        RegisterRequest request = new RegisterRequest(
                "john@example.com",
                "johndoe",
                "SecurePass123!"
        );

        // When / Then
        MvcResult result = mockMvc.perform(post("/api/v1/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.token").exists())
                .andExpect(jsonPath("$.userId").exists())
                .andExpect(jsonPath("$.email").value("john@example.com"))
                .andExpect(jsonPath("$.username").value("johndoe"))
                .andReturn();

        // Verify response
        String responseJson = result.getResponse().getContentAsString();
        AuthResponse response = objectMapper.readValue(responseJson, AuthResponse.class);
        assertThat(response.getToken()).isNotEmpty();
        assertThat(response.getUserId()).isNotNull();

        // Verify user in database
        User savedUser = userRepository.findByEmail("john@example.com").orElseThrow();
        assertThat(savedUser.getEmail()).isEqualTo("john@example.com");
        assertThat(savedUser.getUsername()).isEqualTo("johndoe");
        assertThat(savedUser.getPassword()).isNotEqualTo("SecurePass123!"); // Should be hashed
        assertThat(savedUser.getPassword()).startsWith("$2a$"); // BCrypt hash
        assertThat(savedUser.isEnabled()).isTrue();
        assertThat(savedUser.getCreatedAt()).isNotNull();
    }

    @Test
    @Order(2)
    @DisplayName("Should reject registration with duplicate email")
    void shouldRejectDuplicateEmail() throws Exception {
        // Given - create existing user
        User existingUser = User.builder()
                .username("existinguser")
                .email("john@example.com")
                .password(passwordEncoder.encode("password"))
                .enabled(true)
                .build();
        userRepository.save(existingUser);

        RegisterRequest request = new RegisterRequest(
                "john@example.com",
                "johndoe",
                "SecurePass123!"
        );

        // When / Then
        mockMvc.perform(post("/api/v1/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("Email already registered: john@example.com"));
    }

    @Test
    @Order(3)
    @DisplayName("Should reject registration with invalid email")
    void shouldRejectInvalidEmail() throws Exception {
        // Given
        RegisterRequest request = new RegisterRequest(
                "invalid-email", // Invalid email format
                "johndoe",
                "SecurePass123!"
        );

        // When / Then
        mockMvc.perform(post("/api/v1/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(4)
    @DisplayName("Should reject registration with missing password")
    void shouldRejectMissingPassword() throws Exception {
        // Given
        String requestJson = """
                {
                    "username": "johndoe",
                    "email": "john@example.com"
                }
                """;

        // When / Then
        mockMvc.perform(post("/api/v1/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(5)
    @DisplayName("Should login user successfully with valid credentials")
    void shouldLoginSuccessfully() throws Exception {
        // Given - create user
        User user = User.builder()
                .username("johndoe")
                .email("john@example.com")
                .password(passwordEncoder.encode("SecurePass123!"))
                .enabled(true)
                .build();
        User savedUser = userRepository.save(user);

        LoginRequest request = new LoginRequest("john@example.com", "SecurePass123!");

        // When / Then
        MvcResult result = mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").exists())
                .andExpect(jsonPath("$.userId").value(savedUser.getId()))
                .andExpect(jsonPath("$.email").value("john@example.com"))
                .andExpect(jsonPath("$.username").value("johndoe"))
                .andReturn();

        // Verify JWT token is present
        String responseJson = result.getResponse().getContentAsString();
        AuthResponse response = objectMapper.readValue(responseJson, AuthResponse.class);
        assertThat(response.getToken()).isNotEmpty();

        // Verify lastLoginAt was updated
        User updatedUser = userRepository.findById(savedUser.getId()).orElseThrow();
        assertThat(updatedUser.getLastLoginAt()).isNotNull();
    }

    @Test
    @Order(6)
    @DisplayName("Should reject login with invalid email")
    void shouldRejectLoginWithInvalidEmail() throws Exception {
        // Given
        LoginRequest request = new LoginRequest("nonexistent@example.com", "password");

        // When / Then
        mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("Invalid email or password"));
    }

    @Test
    @Order(7)
    @DisplayName("Should reject login with incorrect password")
    void shouldRejectLoginWithIncorrectPassword() throws Exception {
        // Given - create user
        User user = User.builder()
                .username("johndoe")
                .email("john@example.com")
                .password(passwordEncoder.encode("CorrectPassword"))
                .enabled(true)
                .build();
        userRepository.save(user);

        LoginRequest request = new LoginRequest("john@example.com", "WrongPassword");

        // When / Then
        mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("Invalid email or password"));
    }

    @Test
    @Order(8)
    @DisplayName("Should reject login for disabled user")
    void shouldRejectLoginForDisabledUser() throws Exception {
        // Given - create disabled user
        User user = User.builder()
                .username("johndoe")
                .email("john@example.com")
                .password(passwordEncoder.encode("SecurePass123!"))
                .enabled(false) // Disabled
                .build();
        userRepository.save(user);

        LoginRequest request = new LoginRequest("john@example.com", "SecurePass123!");

        // When / Then
        mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("Account is disabled"));
    }

    @Test
    @Order(9)
    @DisplayName("Should access health endpoint without authentication")
    void shouldAccessHealthEndpoint() throws Exception {
        // When / Then
        mockMvc.perform(get("/api/v1/auth/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Authentication service is running"));
    }

    @Test
    @Order(10)
    @DisplayName("Should not store plaintext passwords")
    void shouldNotStorePlaintextPasswords() throws Exception {
        // Given
        RegisterRequest request = new RegisterRequest(
                "secure@example.com",
                "secureuser",
                "MySecretPassword123!"
        );

        // When
        mockMvc.perform(post("/api/v1/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());

        // Then - verify password is hashed
        User savedUser = userRepository.findByEmail("secure@example.com").orElseThrow();
        assertThat(savedUser.getPassword()).isNotEqualTo("MySecretPassword123!");
        assertThat(savedUser.getPassword()).startsWith("$2a$10$"); // BCrypt with strength 10
        assertThat(savedUser.getPassword().length()).isGreaterThan(50); // BCrypt hashes are ~60 chars

        // Verify BCrypt can validate the password
        assertThat(passwordEncoder.matches("MySecretPassword123!", savedUser.getPassword())).isTrue();
        assertThat(passwordEncoder.matches("WrongPassword", savedUser.getPassword())).isFalse();
    }
}
