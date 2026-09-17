package com.chronos.workflow.repository;

import com.chronos.workflow.domain.RetryConfiguration;
import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.Workflow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for WorkflowRepository using Testcontainers.
 */
@DataMongoTest
@Testcontainers
@ActiveProfiles("test")
class WorkflowRepositoryIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    private WorkflowRepository workflowRepository;

    private Workflow testWorkflow;
    private String ownerId;

    @BeforeEach
    void setUp() {
        workflowRepository.deleteAll();

        ownerId = "user-123";

        TaskDefinition task = TaskDefinition.builder()
                .taskId("task1")
                .name("Test Task")
                .taskType("TEST_TYPE")
                .retryConfig(RetryConfiguration.builder().build())
                .build();

        testWorkflow = Workflow.builder()
                .ownerId(ownerId)
                .name("Test Workflow")
                .description("Test Description")
                .tasks(List.of(task))
                .build();
    }

    @Test
    void shouldSaveAndRetrieveWorkflow() {
        // When
        Workflow saved = workflowRepository.save(testWorkflow);

        // Then
        assertNotNull(saved.getId());
        assertNotNull(saved.getCreatedAt());
        assertNotNull(saved.getUpdatedAt());

        // Retrieve
        Optional<Workflow> retrieved = workflowRepository.findById(saved.getId());
        assertTrue(retrieved.isPresent());
        assertEquals(saved.getName(), retrieved.get().getName());
        assertEquals(saved.getOwnerId(), retrieved.get().getOwnerId());
    }

    @Test
    void shouldFindWorkflowsByOwnerId() {
        // Given
        Workflow workflow1 = workflowRepository.save(testWorkflow);
        Workflow workflow2 = Workflow.builder()
                .ownerId(ownerId)
                .name("Another Workflow")
                .tasks(testWorkflow.getTasks())
                .build();
        workflowRepository.save(workflow2);

        // Workflow from different owner
        Workflow differentOwner = Workflow.builder()
                .ownerId("user-456")
                .name("Different Owner Workflow")
                .tasks(testWorkflow.getTasks())
                .build();
        workflowRepository.save(differentOwner);

        // When
        List<Workflow> userWorkflows = workflowRepository.findByOwnerId(ownerId);

        // Then
        assertEquals(2, userWorkflows.size());
        assertTrue(userWorkflows.stream().allMatch(w -> w.getOwnerId().equals(ownerId)));
    }

    @Test
    void shouldFindWorkflowByIdAndOwnerId() {
        // Given
        Workflow saved = workflowRepository.save(testWorkflow);

        // When
        Optional<Workflow> found = workflowRepository.findByIdAndOwnerId(saved.getId(), ownerId);

        // Then
        assertTrue(found.isPresent());
        assertEquals(saved.getId(), found.get().getId());
    }

    @Test
    void shouldNotFindWorkflowWithWrongOwnerId() {
        // Given
        Workflow saved = workflowRepository.save(testWorkflow);

        // When
        Optional<Workflow> found = workflowRepository.findByIdAndOwnerId(saved.getId(), "wrong-owner");

        // Then
        assertFalse(found.isPresent());
    }

    @Test
    void shouldFindWorkflowsByNameAndOwnerId() {
        // Given
        String workflowName = "Unique Workflow Name";
        testWorkflow.setName(workflowName);
        workflowRepository.save(testWorkflow);

        // When
        List<Workflow> found = workflowRepository.findByNameAndOwnerId(workflowName, ownerId);

        // Then
        assertEquals(1, found.size());
        assertEquals(workflowName, found.get(0).getName());
    }

    @Test
    void shouldCheckExistenceByIdAndOwnerId() {
        // Given
        Workflow saved = workflowRepository.save(testWorkflow);

        // When
        boolean exists = workflowRepository.existsByIdAndOwnerId(saved.getId(), ownerId);
        boolean notExists = workflowRepository.existsByIdAndOwnerId(saved.getId(), "wrong-owner");

        // Then
        assertTrue(exists);
        assertFalse(notExists);
    }

    @Test
    void shouldDeleteWorkflow() {
        // Given
        Workflow saved = workflowRepository.save(testWorkflow);

        // When
        workflowRepository.deleteById(saved.getId());

        // Then
        Optional<Workflow> deleted = workflowRepository.findById(saved.getId());
        assertFalse(deleted.isPresent());
    }

    @Test
    void shouldUpdateWorkflow() {
        // Given
        Workflow saved = workflowRepository.save(testWorkflow);
        String updatedName = "Updated Name";

        // When
        saved.setName(updatedName);
        Workflow updated = workflowRepository.save(saved);

        // Then
        assertEquals(updatedName, updated.getName());
        assertNotNull(updated.getUpdatedAt());
    }
}
