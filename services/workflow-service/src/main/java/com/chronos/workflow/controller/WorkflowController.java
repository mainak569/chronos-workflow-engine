package com.chronos.workflow.controller;

import com.chronos.workflow.dto.CreateWorkflowRequest;
import com.chronos.workflow.dto.WorkflowResponse;
import com.chronos.workflow.service.WorkflowService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST controller for workflow management operations.
 * All endpoints require authentication.
 */
@RestController
@RequestMapping("/api/v1/workflows")
@PreAuthorize("isAuthenticated()")
public class WorkflowController {

    private static final Logger log = LoggerFactory.getLogger(WorkflowController.class);

    private final WorkflowService workflowService;
    
    public WorkflowController(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    /**
     * Extract the authenticated user ID from the security context.
     *
     * @return User ID (subject from JWT)
     */
    private String getAuthenticatedUserId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication.getName(); // Returns the subject (user ID) from JWT
    }

    /**
     * Create a new workflow.
     *
     * POST /api/v1/workflows
     *
     * @param request Create workflow request
     * @return Created workflow response
     */
    @PostMapping
    public ResponseEntity<WorkflowResponse> createWorkflow(
            @Valid @RequestBody CreateWorkflowRequest request) {

        String ownerId = getAuthenticatedUserId();

        log.info("Received create workflow request: name={}, owner={}", request.getName(), ownerId);

        WorkflowResponse response = workflowService.createWorkflow(request, ownerId);

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Get a workflow by ID.
     * Only returns workflow if owned by authenticated user.
     *
     * GET /api/v1/workflows/{workflowId}
     *
     * @param workflowId Workflow ID
     * @return Workflow response
     */
    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowResponse> getWorkflow(
            @PathVariable String workflowId) {

        String ownerId = getAuthenticatedUserId();

        log.debug("Received get workflow request: id={}, owner={}", workflowId, ownerId);

        WorkflowResponse response = workflowService.getWorkflow(workflowId, ownerId);

        return ResponseEntity.ok(response);
    }

    /**
     * Get all workflows for the authenticated user.
     *
     * GET /api/v1/workflows
     *
     * @return List of workflow responses
     */
    @GetMapping
    public ResponseEntity<List<WorkflowResponse>> getUserWorkflows() {

        String ownerId = getAuthenticatedUserId();

        log.debug("Received get user workflows request: owner={}", ownerId);

        List<WorkflowResponse> response = workflowService.getUserWorkflows(ownerId);

        return ResponseEntity.ok(response);
    }

    /**
     * Delete a workflow.
     * Only deletes workflow if owned by authenticated user.
     *
     * DELETE /api/v1/workflows/{workflowId}
     *
     * @param workflowId Workflow ID
     * @return No content response
     */
    @DeleteMapping("/{workflowId}")
    public ResponseEntity<Void> deleteWorkflow(
            @PathVariable String workflowId) {

        String ownerId = getAuthenticatedUserId();

        log.info("Received delete workflow request: id={}, owner={}", workflowId, ownerId);

        workflowService.deleteWorkflow(workflowId, ownerId);

        return ResponseEntity.noContent().build();
    }
}
