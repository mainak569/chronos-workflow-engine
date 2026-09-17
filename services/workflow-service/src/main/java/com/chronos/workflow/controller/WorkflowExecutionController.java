package com.chronos.workflow.controller;

import com.chronos.workflow.domain.TaskExecution;
import com.chronos.workflow.domain.WorkflowExecution;
import com.chronos.workflow.dto.ExecutionRequest;
import com.chronos.workflow.dto.ExecutionResponse;
import com.chronos.workflow.dto.TaskExecutionResponse;
import com.chronos.workflow.service.WorkflowExecutionService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST controller for workflow execution operations.
 */
@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowExecutionController {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionController.class);
    
    private final WorkflowExecutionService executionService;
    
    public WorkflowExecutionController(WorkflowExecutionService executionService) {
        this.executionService = executionService;
    }
    
    /**
     * Start a workflow execution.
     * 
     * POST /api/v1/workflows/{workflowId}/execute
     * 
     * @param workflowId Workflow definition ID
     * @param request Execution request with triggeredBy and input
     * @return Created execution details
     */
    @PostMapping("/{workflowId}/execute")
    public ResponseEntity<ExecutionResponse> executeWorkflow(
            @PathVariable String workflowId,
            @Valid @RequestBody ExecutionRequest request) {
        
        logger.info("Received execution request: workflowId={}, triggeredBy={}", 
                workflowId, request.getTriggeredBy());
        
        WorkflowExecution execution = executionService.startExecution(
                workflowId,
                request.getTriggeredBy(),
                request.getInput()
        );
        
        ExecutionResponse response = new ExecutionResponse(execution);
        
        logger.info("Workflow execution started: executionId={}, workflowId={}", 
                execution.getId(), workflowId);
        
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
    
    /**
     * Get workflow execution details.
     * 
     * GET /api/v1/workflows/executions/{executionId}
     * 
     * @param executionId Execution ID
     * @return Execution details
     */
    @GetMapping("/executions/{executionId}")
    public ResponseEntity<ExecutionResponse> getExecution(@PathVariable String executionId) {
        logger.debug("Getting execution: executionId={}", executionId);
        
        return executionService.getExecution(executionId)
                .map(ExecutionResponse::new)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Get all task executions for a workflow execution.
     * 
     * GET /api/v1/workflows/executions/{executionId}/tasks
     * 
     * @param executionId Execution ID
     * @return List of task executions
     */
    @GetMapping("/executions/{executionId}/tasks")
    public ResponseEntity<List<TaskExecutionResponse>> getTaskExecutions(@PathVariable String executionId) {
        logger.debug("Getting task executions: executionId={}", executionId);
        
        List<TaskExecution> taskExecutions = executionService.getTaskExecutions(executionId);
        List<TaskExecutionResponse> responses = taskExecutions.stream()
                .map(TaskExecutionResponse::new)
                .collect(Collectors.toList());
        
        return ResponseEntity.ok(responses);
    }
    
    /**
     * Get a specific task execution.
     * 
     * GET /api/v1/workflows/executions/{executionId}/tasks/{taskId}
     * 
     * @param executionId Execution ID
     * @param taskId Task ID
     * @return Task execution details
     */
    @GetMapping("/executions/{executionId}/tasks/{taskId}")
    public ResponseEntity<TaskExecutionResponse> getTaskExecution(
            @PathVariable String executionId,
            @PathVariable String taskId) {
        
        logger.debug("Getting task execution: executionId={}, taskId={}", executionId, taskId);
        
        return executionService.getTaskExecution(executionId, taskId)
                .map(TaskExecutionResponse::new)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Get execution statistics.
     * 
     * GET /api/v1/workflows/executions/{executionId}/statistics
     * 
     * @param executionId Execution ID
     * @return Execution statistics
     */
    @GetMapping("/executions/{executionId}/statistics")
    public ResponseEntity<WorkflowExecutionService.ExecutionStatistics> getExecutionStatistics(
            @PathVariable String executionId) {
        
        logger.debug("Getting execution statistics: executionId={}", executionId);
        
        WorkflowExecutionService.ExecutionStatistics stats = 
                executionService.getExecutionStatistics(executionId);
        
        return ResponseEntity.ok(stats);
    }
    
    /**
     * Cancel a workflow execution.
     * 
     * POST /api/v1/workflows/executions/{executionId}/cancel
     * 
     * @param executionId Execution ID
     * @return Success response
     */
    @PostMapping("/executions/{executionId}/cancel")
    public ResponseEntity<Map<String, String>> cancelExecution(@PathVariable String executionId) {
        logger.info("Cancelling execution: executionId={}", executionId);
        
        executionService.cancelExecution(executionId);
        
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Execution cancelled",
                "executionId", executionId
        ));
    }
}
