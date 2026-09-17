package com.chronos.workflow.validation;

import com.chronos.workflow.domain.RetryConfiguration;
import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.Workflow;
import com.chronos.workflow.exception.WorkflowValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowValidatorTest {

    private WorkflowValidator validator;

    @BeforeEach
    void setUp() {
        validator = new WorkflowValidator();
    }

    @Test
    void shouldValidateSimpleWorkflow() {
        // Given
        Workflow workflow = createWorkflow(
                createTask("task1", List.of())
        );

        // When/Then
        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldValidateWorkflowWithDependencies() {
        // Given
        Workflow workflow = createWorkflow(
                createTask("task1", List.of()),
                createTask("task2", List.of("task1")),
                createTask("task3", List.of("task1", "task2"))
        );

        // When/Then
        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldRejectDuplicateTaskIds() {
        // Given
        Workflow workflow = createWorkflow(
                createTask("task1", List.of()),
                createTask("task1", List.of())  // Duplicate
        );

        // When/Then
        WorkflowValidationException exception = assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );

        assertTrue(exception.getMessage().contains("Duplicate task IDs"));
        assertTrue(exception.getMessage().contains("task1"));
    }

    @Test
    void shouldRejectInvalidDependency() {
        // Given
        Workflow workflow = createWorkflow(
                createTask("task1", List.of("nonexistent"))
        );

        // When/Then
        WorkflowValidationException exception = assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );

        assertTrue(exception.getMessage().contains("Invalid dependencies"));
        assertTrue(exception.getMessage().contains("nonexistent"));
    }

    @Test
    void shouldDetectSimpleCycle() {
        // Given: A -> B -> A
        Workflow workflow = createWorkflow(
                createTask("taskA", List.of("taskB")),
                createTask("taskB", List.of("taskA"))
        );

        // When/Then
        WorkflowValidationException exception = assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );

        assertTrue(exception.getMessage().contains("Cyclic dependency"));
    }

    @Test
    void shouldDetectComplexCycle() {
        // Given: A -> B -> C -> A
        Workflow workflow = createWorkflow(
                createTask("taskA", List.of("taskC")),
                createTask("taskB", List.of("taskA")),
                createTask("taskC", List.of("taskB"))
        );

        // When/Then
        WorkflowValidationException exception = assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );

        assertTrue(exception.getMessage().contains("Cyclic dependency"));
    }

    @Test
    void shouldDetectSelfDependency() {
        // Given: A -> A
        Workflow workflow = createWorkflow(
                createTask("taskA", List.of("taskA"))
        );

        // When/Then
        WorkflowValidationException exception = assertThrows(
                WorkflowValidationException.class,
                () -> validator.validate(workflow)
        );

        assertTrue(exception.getMessage().contains("Cyclic dependency"));
    }

    @Test
    void shouldAllowComplexDAG() {
        // Given:    A
        //          / \
        //         B   C
        //          \ /
        //           D
        Workflow workflow = createWorkflow(
                createTask("A", List.of()),
                createTask("B", List.of("A")),
                createTask("C", List.of("A")),
                createTask("D", List.of("B", "C"))
        );

        // When/Then
        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    @Test
    void shouldAllowDisconnectedTasks() {
        // Given: Two independent task chains
        Workflow workflow = createWorkflow(
                createTask("task1", List.of()),
                createTask("task2", List.of("task1")),
                createTask("task3", List.of()),
                createTask("task4", List.of("task3"))
        );

        // When/Then
        assertDoesNotThrow(() -> validator.validate(workflow));
    }

    // Helper methods

    private Workflow createWorkflow(TaskDefinition... tasks) {
        return Workflow.builder()
                .ownerId("owner-123")
                .name("Test Workflow")
                .tasks(Arrays.asList(tasks))
                .build();
    }

    private TaskDefinition createTask(String taskId, List<String> dependencies) {
        return TaskDefinition.builder()
                .taskId(taskId)
                .name("Task " + taskId)
                .taskType("TEST_TASK")
                .dependencies(new HashSet<>(dependencies))
                .retryConfig(RetryConfiguration.builder().build())
                .build();
    }
}
