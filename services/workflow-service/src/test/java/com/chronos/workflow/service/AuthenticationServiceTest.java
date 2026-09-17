package com.chronos.workflow.service;

import com.chronos.workflow.domain.User;
import com.chronos.workflow.dto.AuthResponse;
import com.chronos.workflow.dto.LoginRequest;
import com.chronos.workflow.dto.RegisterRequest;
import com.chronos.workflow.exception.AuthenticationException;
import com.chronos.workflow.exception.DuplicateResourceException;
import com.chronos.workflow.repository.UserRepository;
import com.chronos.workflow.security.JwtTokenProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AuthenticationService.
 * Tests registration, login, password validation, and error cases.
 */
@ExtendWith(MockitoExtension.class)
class AuthenticationServiceTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private JwtTokenProvider jwtTokenProvider;

    private AuthenticationService authenticationService;

    @BeforeEach
    void setUp() {
        authenticationService = new AuthenticationService(
                userRepository,
                passwordEncoder,
                jwtTokenProvider
        );
    }

    @Test
    @DisplayName("Should register new user successfully")
    void shouldRegisterNewUserSuccessfully() {
        // Given
        RegisterRequest request = new RegisterRequest(
                "johndoe",
                "john@example.com",
                "SecurePass123!"
        );

        String hashedPassword = "$2a$10$hashedpassword";
        String jwtToken = "jwt.token.here";

        when(userRepository.existsByEmail(request.getEmail())).thenReturn(false);
        when(passwordEncoder.encode(request.getPassword())).thenReturn(hashedPassword);
        when(userRepository.save(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            user.setId("user-123");
            user.setCreatedAt(Instant.now());
            user.setUpdatedAt(Instant.now());
            return user;
        });
        when(jwtTokenProvider.generateToken("user-123", request.getEmail()))
                .thenReturn(jwtToken);

        // When
        AuthResponse response = authenticationService.register(request);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getToken()).isEqualTo(jwtToken);
        assertThat(response.getUserId()).isEqualTo("user-123");
        assertThat(response.getEmail()).isEqualTo(request.getEmail());
        assertThat(response.getUsername()).isEqualTo(request.getUsername());

        // Verify password was hashed
        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(userCaptor.capture());
        User savedUser = userCaptor.getValue();
        assertThat(savedUser.getPassword()).isEqualTo(hashedPassword);
        assertThat(savedUser.getEmail()).isEqualTo(request.getEmail());
        assertThat(savedUser.getUsername()).isEqualTo(request.getUsername());
        assertThat(savedUser.isEnabled()).isTrue();

        verify(passwordEncoder).encode(request.getPassword());
        verify(jwtTokenProvider).generateToken("user-123", request.getEmail());
    }

    @Test
    @DisplayName("Should throw DuplicateResourceException when email already exists")
    void shouldThrowExceptionWhenEmailExists() {
        // Given
        RegisterRequest request = new RegisterRequest(
                "johndoe",
                "john@example.com",
                "SecurePass123!"
        );

        when(userRepository.existsByEmail(request.getEmail())).thenReturn(true);

        // When / Then
        assertThatThrownBy(() -> authenticationService.register(request))
                .isInstanceOf(DuplicateResourceException.class)
                .hasMessageContaining("Email already registered: john@example.com");

        verify(userRepository, never()).save(any(User.class));
        verify(passwordEncoder, never()).encode(anyString());
        verify(jwtTokenProvider, never()).generateToken(anyString(), anyString());
    }

    @Test
    @DisplayName("Should login user successfully with valid credentials")
    void shouldLoginSuccessfully() {
        // Given
        LoginRequest request = new LoginRequest("john@example.com", "SecurePass123!");

        User user = User.builder()
                .id("user-123")
                .username("johndoe")
                .email("john@example.com")
                .password("$2a$10$hashedpassword")
                .enabled(true)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        String jwtToken = "jwt.token.here";

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.of(user));
        when(passwordEncoder.matches(request.getPassword(), user.getPassword())).thenReturn(true);
        when(jwtTokenProvider.generateToken(user.getId(), user.getEmail())).thenReturn(jwtToken);

        // When
        AuthResponse response = authenticationService.login(request);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getToken()).isEqualTo(jwtToken);
        assertThat(response.getUserId()).isEqualTo("user-123");
        assertThat(response.getEmail()).isEqualTo("john@example.com");
        assertThat(response.getUsername()).isEqualTo("johndoe");

        verify(userRepository).findByEmail(request.getEmail());
        verify(passwordEncoder).matches(request.getPassword(), user.getPassword());
        verify(jwtTokenProvider).generateToken(user.getId(), user.getEmail());

        // Verify lastLoginAt was updated
        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(userCaptor.capture());
        User savedUser = userCaptor.getValue();
        assertThat(savedUser.getLastLoginAt()).isNotNull();
    }

    @Test
    @DisplayName("Should throw AuthenticationException when user not found")
    void shouldThrowExceptionWhenUserNotFound() {
        // Given
        LoginRequest request = new LoginRequest("nonexistent@example.com", "password");

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.empty());

        // When / Then
        assertThatThrownBy(() -> authenticationService.login(request))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Invalid email or password");

        verify(passwordEncoder, never()).matches(anyString(), anyString());
        verify(jwtTokenProvider, never()).generateToken(anyString(), anyString());
    }

    @Test
    @DisplayName("Should throw AuthenticationException when password is incorrect")
    void shouldThrowExceptionWhenPasswordIncorrect() {
        // Given
        LoginRequest request = new LoginRequest("john@example.com", "WrongPassword");

        User user = User.builder()
                .id("user-123")
                .username("johndoe")
                .email("john@example.com")
                .password("$2a$10$hashedpassword")
                .enabled(true)
                .build();

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.of(user));
        when(passwordEncoder.matches(request.getPassword(), user.getPassword())).thenReturn(false);

        // When / Then
        assertThatThrownBy(() -> authenticationService.login(request))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Invalid email or password");

        verify(jwtTokenProvider, never()).generateToken(anyString(), anyString());
        verify(userRepository, never()).save(any(User.class));
    }

    @Test
    @DisplayName("Should throw AuthenticationException when user is disabled")
    void shouldThrowExceptionWhenUserDisabled() {
        // Given
        LoginRequest request = new LoginRequest("john@example.com", "SecurePass123!");

        User user = User.builder()
                .id("user-123")
                .username("johndoe")
                .email("john@example.com")
                .password("$2a$10$hashedpassword")
                .enabled(false) // User disabled
                .build();

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.of(user));

        // When / Then
        assertThatThrownBy(() -> authenticationService.login(request))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Account is disabled");

        verify(passwordEncoder, never()).matches(anyString(), anyString());
        verify(jwtTokenProvider, never()).generateToken(anyString(), anyString());
    }

    @Test
    @DisplayName("Should not expose plaintext password in User object")
    void shouldNotExposePlaintextPassword() {
        // Given
        RegisterRequest request = new RegisterRequest(
                "johndoe",
                "john@example.com",
                "SecurePass123!"
        );

        String hashedPassword = "$2a$10$hashedpassword";

        when(userRepository.existsByEmail(request.getEmail())).thenReturn(false);
        when(passwordEncoder.encode(request.getPassword())).thenReturn(hashedPassword);
        when(userRepository.save(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            user.setId("user-123");
            return user;
        });
        when(jwtTokenProvider.generateToken(anyString(), anyString())).thenReturn("token");

        // When
        authenticationService.register(request);

        // Then
        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(userCaptor.capture());
        User savedUser = userCaptor.getValue();

        // Verify password is hashed, not plaintext
        assertThat(savedUser.getPassword()).isNotEqualTo(request.getPassword());
        assertThat(savedUser.getPassword()).isEqualTo(hashedPassword);
        assertThat(savedUser.getPassword()).startsWith("$2a$10$");
    }
}
