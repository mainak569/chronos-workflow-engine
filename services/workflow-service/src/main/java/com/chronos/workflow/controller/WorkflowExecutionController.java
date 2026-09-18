package com.chronos.workflow.controller;

import com.chronos.workflow.domain.WorkflowExecution;
import com.chronos.workflow.dto.ExecutionRequest;
import com.chronos.workflow.dto.ExecutionResponse;
import com.chronos.workflow.dto.TaskExecutionResponse;
import com.chronos.workflow.service.WorkflowExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for workflow execution operations.
 * Every operation is scoped to the authenticated user: executions of other users are reported as not found.
 */
@RestController
@RequestMapping("/api/v1/workflows")
@PreAuthorize("isAuthenticated()")
public class WorkflowExecutionController {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionController.class);
    
    private final WorkflowExecutionService executionService;
    
    public WorkflowExecutionController(WorkflowExecutionService executionService) {
        this.executionService = executionService;
    }
    
    /**
     * Get the authenticated user's ID (the JWT subject).
     */
    private String getAuthenticatedUserId() {
        return SecurityContextHolder.getContext().getAuthentication().getName();
    }
    
    /**
     * Start a workflow execution.
     * 
     * POST /api/v1/workflows/{workflowId}/execute
     * 
     * @param workflowId Workflow definition ID
     * @param request Optional execution request with input parameters
     * @return Created execution details
     */
    @PostMapping("/{workflowId}/execute")
    public ResponseEntity<ExecutionResponse> executeWorkflow(
            @PathVariable String workflowId,
            @RequestBody(required = false) ExecutionRequest request) {
        
        String userId = getAuthenticatedUserId();
        logger.info("Received execution request: workflowId={}, userId={}", workflowId, userId);
        
        Map<String, Object> input = request != null ? request.getInput() : null;
        WorkflowExecution execution = executionService.startExecution(workflowId, userId, input);
        
        logger.info("Workflow execution started: executionId={}, workflowId={}", 
                execution.getId(), workflowId);
        
        return ResponseEntity.status(HttpStatus.CREATED).body(new ExecutionResponse(execution));
    }
    
    /**
     * Get execution details.
     * 
     * GET /api/v1/workflows/executions/{executionId}
     */
    @GetMapping("/executions/{executionId}")
    public ResponseEntity<ExecutionResponse> getExecution(@PathVariable String executionId) {
        logger.debug("Getting execution: executionId={}", executionId);
        
        WorkflowExecution execution = executionService.getExecution(executionId, getAuthenticatedUserId());
        return ResponseEntity.ok(new ExecutionResponse(execution));
    }
    
    /**
     * Get all task executions for a workflow execution.
     * 
     * GET /api/v1/workflows/executions/{executionId}/tasks
     */
    @GetMapping("/executions/{executionId}/tasks")
    public ResponseEntity<List<TaskExecutionResponse>> getTaskExecutions(@PathVariable String executionId) {
        logger.debug("Getting task executions: executionId={}", executionId);
        
        List<TaskExecutionResponse> responses = executionService
                .getTaskExecutions(executionId, getAuthenticatedUserId())
                .stream()
                .map(TaskExecutionResponse::new)
                .toList();
        
        return ResponseEntity.ok(responses);
    }
    
    /**
     * Get a specific task execution.
     * 
     * GET /api/v1/workflows/executions/{executionId}/tasks/{taskId}
     */
    @GetMapping("/executions/{executionId}/tasks/{taskId}")
    public ResponseEntity<TaskExecutionResponse> getTaskExecution(
            @PathVariable String executionId,
            @PathVariable String taskId) {
        
        logger.debug("Getting task execution: executionId={}, taskId={}", executionId, taskId);
        
        return ResponseEntity.ok(new TaskExecutionResponse(
                executionService.getTaskExecution(executionId, taskId, getAuthenticatedUserId())));
    }
    
    /**
     * Get execution statistics.
     * 
     * GET /api/v1/workflows/executions/{executionId}/statistics
     */
    @GetMapping("/executions/{executionId}/statistics")
    public ResponseEntity<WorkflowExecutionService.ExecutionStatistics> getExecutionStatistics(
            @PathVariable String executionId) {
        
        logger.debug("Getting execution statistics: executionId={}", executionId);
        
        return ResponseEntity.ok(executionService.getExecutionStatistics(executionId, getAuthenticatedUserId()));
    }
    
    /**
     * Cancel a workflow execution.
     * 
     * POST /api/v1/workflows/executions/{executionId}/cancel
     */
    @PostMapping("/executions/{executionId}/cancel")
    public ResponseEntity<Map<String, String>> cancelExecution(@PathVariable String executionId) {
        logger.info("Cancelling execution: executionId={}", executionId);
        
        executionService.cancelExecution(executionId, getAuthenticatedUserId());
        
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Execution cancelled",
                "executionId", executionId
        ));
    }
}
