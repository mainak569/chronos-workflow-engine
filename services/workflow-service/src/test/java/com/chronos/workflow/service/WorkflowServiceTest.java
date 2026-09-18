package com.chronos.workflow.service;

import com.chronos.workflow.domain.RetryConfiguration;
import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.Workflow;
import com.chronos.workflow.dto.CreateWorkflowRequest;
import com.chronos.workflow.dto.WorkflowResponse;
import com.chronos.workflow.exception.WorkflowNotFoundException;
import com.chronos.workflow.metrics.WorkflowMetrics;
import com.chronos.workflow.repository.WorkflowRepository;
import com.chronos.workflow.validation.WorkflowValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WorkflowServiceTest {

    @Mock
    private WorkflowRepository workflowRepository;

    @Mock
    private WorkflowValidator workflowValidator;

    @Mock
    private WorkflowMetrics metrics;

    @InjectMocks
    private WorkflowService workflowService;

    private String ownerId;
    private CreateWorkflowRequest createRequest;
    private Workflow savedWorkflow;

    @BeforeEach
    void setUp() {
        ownerId = "user-123";

        TaskDefinition task = TaskDefinition.builder()
                .taskId("task1")
                .name("Test Task")
                .taskType("TEST_TYPE")
                .retryConfig(RetryConfiguration.builder().build())
                .build();

        createRequest = CreateWorkflowRequest.builder()
                .name("Test Workflow")
                .description("Test Description")
                .tasks(List.of(task))
                .build();

        savedWorkflow = Workflow.builder()
                .id("workflow-123")
                .ownerId(ownerId)
                .name("Test Workflow")
                .description("Test Description")
                .tasks(List.of(task))
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    @Test
    void shouldCreateWorkflow() {
        // Given
        when(workflowRepository.save(any(Workflow.class))).thenReturn(savedWorkflow);

        // When
        WorkflowResponse response = workflowService.createWorkflow(createRequest, ownerId);

        // Then
        assertNotNull(response);
        assertEquals(savedWorkflow.getId(), response.getWorkflowId());
        assertEquals(savedWorkflow.getName(), response.getName());
        assertEquals(savedWorkflow.getOwnerId(), response.getOwnerId());

        // Verify validation was called
        verify(workflowValidator).validate(any(Workflow.class));

        // Verify save was called
        ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
        verify(workflowRepository).save(workflowCaptor.capture());

        Workflow capturedWorkflow = workflowCaptor.getValue();
        assertEquals(ownerId, capturedWorkflow.getOwnerId());
        assertEquals(createRequest.getName(), capturedWorkflow.getName());
        assertEquals(createRequest.getTasks(), capturedWorkflow.getTasks());
    }

    @Test
    void shouldGetWorkflow() {
        // Given
        String workflowId = "workflow-123";
        when(workflowRepository.findByIdAndOwnerId(workflowId, ownerId))
                .thenReturn(Optional.of(savedWorkflow));

        // When
        WorkflowResponse response = workflowService.getWorkflow(workflowId, ownerId);

        // Then
        assertNotNull(response);
        assertEquals(savedWorkflow.getId(), response.getWorkflowId());
        assertEquals(savedWorkflow.getName(), response.getName());
        verify(workflowRepository).findByIdAndOwnerId(workflowId, ownerId);
    }

    @Test
    void shouldThrowExceptionWhenWorkflowNotFound() {
        // Given
        String workflowId = "nonexistent";
        when(workflowRepository.findByIdAndOwnerId(workflowId, ownerId))
                .thenReturn(Optional.empty());

        // When/Then
        assertThrows(WorkflowNotFoundException.class,
                () -> workflowService.getWorkflow(workflowId, ownerId));

        verify(workflowRepository).findByIdAndOwnerId(workflowId, ownerId);
    }

    @Test
    void shouldGetUserWorkflows() {
        // Given
        List<Workflow> workflows = List.of(savedWorkflow);
        when(workflowRepository.findByOwnerId(ownerId)).thenReturn(workflows);

        // When
        List<WorkflowResponse> responses = workflowService.getUserWorkflows(ownerId);

        // Then
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertEquals(savedWorkflow.getId(), responses.get(0).getWorkflowId());
        verify(workflowRepository).findByOwnerId(ownerId);
    }

    @Test
    void shouldDeleteWorkflow() {
        // Given
        String workflowId = "workflow-123";
        when(workflowRepository.existsByIdAndOwnerId(workflowId, ownerId))
                .thenReturn(true);

        // When
        workflowService.deleteWorkflow(workflowId, ownerId);

        // Then
        verify(workflowRepository).existsByIdAndOwnerId(workflowId, ownerId);
        verify(workflowRepository).deleteById(workflowId);
    }

    @Test
    void shouldThrowExceptionWhenDeletingNonexistentWorkflow() {
        // Given
        String workflowId = "nonexistent";
        when(workflowRepository.existsByIdAndOwnerId(workflowId, ownerId))
                .thenReturn(false);

        // When/Then
        assertThrows(WorkflowNotFoundException.class,
                () -> workflowService.deleteWorkflow(workflowId, ownerId));

        verify(workflowRepository).existsByIdAndOwnerId(workflowId, ownerId);
        verify(workflowRepository, never()).deleteById(any());
    }
}
