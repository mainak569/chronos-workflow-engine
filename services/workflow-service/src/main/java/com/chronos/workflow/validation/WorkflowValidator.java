package com.chronos.workflow.validation;

import com.chronos.workflow.domain.TaskDefinition;
import com.chronos.workflow.domain.Workflow;
import com.chronos.workflow.exception.WorkflowValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Validates workflow definitions.
 * Ensures workflows are well-formed and can be executed.
 */
@Component
public class WorkflowValidator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidator.class);

    /**
     * Validate a workflow for creation.
     *
     * @param workflow Workflow to validate
     * @throws WorkflowValidationException if validation fails
     */
    public void validate(Workflow workflow) {
        log.debug("Validating workflow: {}", workflow.getName());

        validateTaskIds(workflow);
        validateDependencies(workflow);
        detectCycles(workflow);

        log.debug("Workflow validation passed: {}", workflow.getName());
    }

    /**
     * Ensure all task IDs are unique within the workflow.
     */
    private void validateTaskIds(Workflow workflow) {
        Set<String> taskIds = new HashSet<>();
        List<String> duplicates = new ArrayList<>();

        for (TaskDefinition task : workflow.getTasks()) {
            if (!taskIds.add(task.getTaskId())) {
                duplicates.add(task.getTaskId());
            }
        }

        if (!duplicates.isEmpty()) {
            throw new WorkflowValidationException(
                    "Duplicate task IDs found: " + String.join(", ", duplicates));
        }
    }

    /**
     * Ensure all task dependencies reference valid tasks.
     */
    private void validateDependencies(Workflow workflow) {
        Set<String> taskIds = new HashSet<>(workflow.getTaskIds());
        List<String> invalidDependencies = new ArrayList<>();

        for (TaskDefinition task : workflow.getTasks()) {
            for (String dependency : task.getDependencies()) {
                if (!taskIds.contains(dependency)) {
                    invalidDependencies.add(
                            String.format("Task '%s' depends on non-existent task '%s'",
                                    task.getTaskId(), dependency));
                }
            }
        }

        if (!invalidDependencies.isEmpty()) {
            throw new WorkflowValidationException(
                    "Invalid dependencies: " + String.join("; ", invalidDependencies));
        }
    }

    /**
     * Detect cyclic dependencies in the workflow DAG.
     * Uses depth-first search to detect cycles.
     */
    private void detectCycles(Workflow workflow) {
        Map<String, TaskDefinition> taskMap = buildTaskMap(workflow);
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (TaskDefinition task : workflow.getTasks()) {
            if (hasCycle(task, taskMap, visited, recursionStack)) {
                throw new WorkflowValidationException(
                        "Cyclic dependency detected involving task: " + task.getTaskId());
            }
        }
    }

    /**
     * Check if there's a cycle starting from the given task.
     */
    private boolean hasCycle(TaskDefinition task,
                             Map<String, TaskDefinition> taskMap,
                             Set<String> visited,
                             Set<String> recursionStack) {

        // If already in recursion stack, we found a cycle
        if (recursionStack.contains(task.getTaskId())) {
            return true;
        }

        // If already fully explored, no cycle from this node
        if (visited.contains(task.getTaskId())) {
            return false;
        }

        // Mark as being explored
        visited.add(task.getTaskId());
        recursionStack.add(task.getTaskId());

        // Explore dependencies
        for (String dependencyId : task.getDependencies()) {
            TaskDefinition dependency = taskMap.get(dependencyId);
            if (dependency != null) {
                if (hasCycle(dependency, taskMap, visited, recursionStack)) {
                    return true;
                }
            }
        }

        // Remove from recursion stack after exploring all dependencies
        recursionStack.remove(task.getTaskId());
        return false;
    }

    /**
     * Build a map of task ID to task definition for quick lookup.
     */
    private Map<String, TaskDefinition> buildTaskMap(Workflow workflow) {
        Map<String, TaskDefinition> taskMap = new HashMap<>();
        for (TaskDefinition task : workflow.getTasks()) {
            taskMap.put(task.getTaskId(), task);
        }
        return taskMap;
    }
}
