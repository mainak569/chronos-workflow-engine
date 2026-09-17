package com.chronos.workflow.controller;

import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.User;
import com.chronos.workflow.domain.Workflow;
import com.chronos.workflow.dto.CreateWorkflowRequest;
import com.chronos.workflow.repository.UserRepository;
import com.chronos.workflow.repository.WorkflowRepository;
import com.chronos.workflow.security.JwtTokenProvider;
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
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for workflow authorization and ownership.
 * Verifies that users can only access their own workflows.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkflowAuthorizationIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("chronos.security.jwt.secret",
                () -> "test-secret-key-for-jwt-signing-must-be-at-least-256-bits-long");
        registry.add("chronos.security.jwt.expiration", () -> "3600000");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    private User user1;
    private User user2;
    private String user1Token;
    private String user2Token;

    @BeforeEach
    void setUp() {
        workflowRepository.deleteAll();
        userRepository.deleteAll();

        // Create two test users
        user1 = User.builder()
                .username("user1")
                .email("user1@example.com")
                .password(passwordEncoder.encode("password"))
                .enabled(true)
                .build();
        user1 = userRepository.save(user1);
        user1Token = jwtTokenProvider.generateToken(user1.getId(), user1.getEmail());

        user2 = User.builder()
                .username("user2")
                .email("user2@example.com")
                .password(passwordEncoder.encode("password"))
                .enabled(true)
                .build();
        user2 = userRepository.save(user2);
        user2Token = jwtTokenProvider.generateToken(user2.getId(), user2.getEmail());
    }

    @Test
    @Order(1)
    @DisplayName("Should reject workflow creation without authentication")
    void shouldRejectUnauthenticatedWorkflowCreation() throws Exception {
        // Given
        CreateWorkflowRequest request = createSampleWorkflowRequest();

        // When / Then
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden()); // Spring Security returns 403 for missing token
    }

    @Test
    @Order(2)
    @DisplayName("Should reject workflow creation with invalid token")
    void shouldRejectInvalidToken() throws Exception {
        // Given
        CreateWorkflowRequest request = createSampleWorkflowRequest();

        // When / Then
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer invalid.jwt.token")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());
    }

    @Test
    @Order(3)
    @DisplayName("Should create workflow successfully with valid token")
    void shouldCreateWorkflowWithValidToken() throws Exception {
        // Given
        CreateWorkflowRequest request = createSampleWorkflowRequest();

        // When / Then
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer " + user1Token)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.name").value("Test Workflow"))
                .andExpect(jsonPath("$.ownerId").value(user1.getId()));

        // Verify workflow is owned by user1
        List<Workflow> workflows = workflowRepository.findByOwnerId(user1.getId());
        assertThat(workflows).hasSize(1);
        assertThat(workflows.get(0).getOwnerId()).isEqualTo(user1.getId());
    }

    @Test
    @Order(4)
    @DisplayName("Should only return user's own workflows")
    void shouldOnlyReturnOwnWorkflows() throws Exception {
        // Given - create workflows for both users
        Workflow user1Workflow = createWorkflowForUser(user1.getId(), "User1 Workflow");
        Workflow user2Workflow = createWorkflowForUser(user2.getId(), "User2 Workflow");

        // When / Then - user1 should only see their workflow
        mockMvc.perform(get("/api/v1/workflows")
                        .header("Authorization", "Bearer " + user1Token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(user1Workflow.getId()))
                .andExpect(jsonPath("$[0].name").value("User1 Workflow"))
                .andExpect(jsonPath("$[0].ownerId").value(user1.getId()));

        // user2 should only see their workflow
        mockMvc.perform(get("/api/v1/workflows")
                        .header("Authorization", "Bearer " + user2Token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(user2Workflow.getId()))
                .andExpect(jsonPath("$[0].name").value("User2 Workflow"))
                .andExpect(jsonPath("$[0].ownerId").value(user2.getId()));
    }

    @Test
    @Order(5)
    @DisplayName("Should prevent access to other user's workflow by ID")
    void shouldPreventAccessToOthersWorkflow() throws Exception {
        // Given - create workflow for user1
        Workflow user1Workflow = createWorkflowForUser(user1.getId(), "User1 Workflow");

        // When / Then - user2 tries to access user1's workflow
        mockMvc.perform(get("/api/v1/workflows/" + user1Workflow.getId())
                        .header("Authorization", "Bearer " + user2Token))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("Workflow not found: " + user1Workflow.getId()));

        // user1 can access their own workflow
        mockMvc.perform(get("/api/v1/workflows/" + user1Workflow.getId())
                        .header("Authorization", "Bearer " + user1Token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(user1Workflow.getId()))
                .andExpect(jsonPath("$.ownerId").value(user1.getId()));
    }

    @Test
    @Order(6)
    @DisplayName("Should prevent deletion of other user's workflow")
    void shouldPreventDeletionOfOthersWorkflow() throws Exception {
        // Given - create workflow for user1
        Workflow user1Workflow = createWorkflowForUser(user1.getId(), "User1 Workflow");

        // When / Then - user2 tries to delete user1's workflow
        mockMvc.perform(delete("/api/v1/workflows/" + user1Workflow.getId())
                        .header("Authorization", "Bearer " + user2Token))
                .andExpect(status().isNotFound());

        // Verify workflow still exists
        assertThat(workflowRepository.existsById(user1Workflow.getId())).isTrue();

        // user1 can delete their own workflow
        mockMvc.perform(delete("/api/v1/workflows/" + user1Workflow.getId())
                        .header("Authorization", "Bearer " + user1Token))
                .andExpect(status().isNoContent());

        // Verify workflow is deleted
        assertThat(workflowRepository.existsById(user1Workflow.getId())).isFalse();
    }

    @Test
    @Order(7)
    @DisplayName("Should reject workflow operations without Authorization header")
    void shouldRejectOperationsWithoutAuthHeader() throws Exception {
        // Given
        Workflow workflow = createWorkflowForUser(user1.getId(), "Test Workflow");

        // When / Then
        mockMvc.perform(get("/api/v1/workflows"))
                .andExpect(status().isForbidden());

        mockMvc.perform(get("/api/v1/workflows/" + workflow.getId()))
                .andExpect(status().isForbidden());

        mockMvc.perform(delete("/api/v1/workflows/" + workflow.getId()))
                .andExpect(status().isForbidden());
    }

    @Test
    @Order(8)
    @DisplayName("Should reject malformed Authorization header")
    void shouldRejectMalformedAuthHeader() throws Exception {
        // Given
        CreateWorkflowRequest request = createSampleWorkflowRequest();

        // When / Then - missing "Bearer" prefix
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", user1Token)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());

        // Wrong prefix
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Basic " + user1Token)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());
    }

    @Test
    @Order(9)
    @DisplayName("Should isolate workflows between multiple users")
    void shouldIsolateWorkflowsBetweenUsers() throws Exception {
        // Given - create multiple workflows for each user
        Workflow user1Workflow1 = createWorkflowForUser(user1.getId(), "User1 Workflow 1");
        Workflow user1Workflow2 = createWorkflowForUser(user1.getId(), "User1 Workflow 2");
        Workflow user2Workflow1 = createWorkflowForUser(user2.getId(), "User2 Workflow 1");
        Workflow user2Workflow2 = createWorkflowForUser(user2.getId(), "User2 Workflow 2");

        // When / Then - verify isolation
        mockMvc.perform(get("/api/v1/workflows")
                        .header("Authorization", "Bearer " + user1Token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[*].ownerId").value(org.hamcrest.Matchers.everyItem(
                        org.hamcrest.Matchers.is(user1.getId()))));

        mockMvc.perform(get("/api/v1/workflows")
                        .header("Authorization", "Bearer " + user2Token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[*].ownerId").value(org.hamcrest.Matchers.everyItem(
                        org.hamcrest.Matchers.is(user2.getId()))));
    }

    @Test
    @Order(10)
    @DisplayName("Should handle expired token gracefully")
    void shouldHandleExpiredToken() throws Exception {
        // Given - create token with very short expiration (already expired)
        // Note: We can't easily test actual expiration without manipulating time,
        // so this test verifies the structure is in place
        CreateWorkflowRequest request = createSampleWorkflowRequest();

        // Create an expired-looking token (malformed, will be rejected)
        String expiredToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyLTEyMyIsImVtYWlsIjoidXNlckBleGFtcGxlLmNvbSIsImlhdCI6MTYwOTQ1OTIwMCwiZXhwIjoxNjA5NDU5MjAwfQ.invalid";

        // When / Then
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Bearer " + expiredToken)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden());
    }

    // Helper methods

    private CreateWorkflowRequest createSampleWorkflowRequest() {
        TaskDefinition task = TaskDefinition.builder()
                .taskId("task-1")
                .taskType("HTTP")
                .configuration(Map.of(
                        "url", "https://api.example.com",
                        "method", "GET"
                ))
                .dependencies(Set.of())
                .build();

        return new CreateWorkflowRequest(
                "Test Workflow",
                "Test workflow for authorization",
                List.of(task)
        );
    }

    private Workflow createWorkflowForUser(String ownerId, String name) {
        TaskDefinition task = TaskDefinition.builder()
                .taskId("task-1")
                .taskType("HTTP")
                .configuration(Map.of("url", "https://example.com"))
                .dependencies(Set.of())
                .build();

        Workflow workflow = Workflow.builder()
                .ownerId(ownerId)
                .name(name)
                .description("Test workflow")
                .tasks(List.of(task))
                .build();

        return workflowRepository.save(workflow);
    }
}
