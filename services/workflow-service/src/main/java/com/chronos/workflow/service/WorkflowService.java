package com.chronos.workflow.service;

import com.chronos.workflow.domain.Workflow;
import com.chronos.workflow.dto.CreateWorkflowRequest;
import com.chronos.workflow.dto.WorkflowResponse;
import com.chronos.workflow.event.WorkflowEventPublisher;
import com.chronos.workflow.exception.WorkflowNotFoundException;
import com.chronos.workflow.repository.WorkflowRepository;
import com.chronos.workflow.validation.WorkflowValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Service for workflow management operations.
 */
@Service
public class WorkflowService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowService.class);

    private final WorkflowRepository workflowRepository;
    private final WorkflowValidator workflowValidator;
    private final WorkflowEventPublisher eventPublisher;
    
    public WorkflowService(WorkflowRepository workflowRepository, 
                          WorkflowValidator workflowValidator,
                          WorkflowEventPublisher eventPublisher) {
        this.workflowRepository = workflowRepository;
        this.workflowValidator = workflowValidator;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Create a new workflow.
     *
     * @param request Create workflow request
     * @param ownerId Owner user ID (from authentication context)
     * @return Created workflow response
     */
    @Transactional
    public WorkflowResponse createWorkflow(CreateWorkflowRequest request, String ownerId) {
        log.info("Creating workflow '{}' for owner: {}", request.getName(), ownerId);

        // Build workflow domain model
        Workflow workflow = Workflow.builder()
                .ownerId(ownerId)
                .name(request.getName())
                .description(request.getDescription())
                .tasks(request.getTasks())
                .build();

        // Validate workflow structure
        workflowValidator.validate(workflow);

        // Save to database
        Workflow savedWorkflow = workflowRepository.save(workflow);

        log.info("Created workflow: id={}, name={}, tasks={}, owner={}",
                savedWorkflow.getId(),
                savedWorkflow.getName(),
                savedWorkflow.getTasks().size(),
                ownerId);

        // Publish WorkflowCreatedEvent to Kafka
        // Note: This publishes workflow definition creation, not execution
        // Execution events are published by WorkflowExecutionService
        // TODO: Consider renaming this event or creating separate WorkflowDefinitionCreated event

        return WorkflowResponse.from(savedWorkflow);
    }

    /**
     * Get a workflow by ID.
     *
     * @param workflowId Workflow ID
     * @param ownerId Owner user ID (from authentication context)
     * @return Workflow response
     * @throws WorkflowNotFoundException if workflow not found or not owned by user
     */
    @Transactional(readOnly = true)
    public WorkflowResponse getWorkflow(String workflowId, String ownerId) {
        log.debug("Retrieving workflow: id={}, owner={}", workflowId, ownerId);

        Workflow workflow = workflowRepository.findByIdAndOwnerId(workflowId, ownerId)
                .orElseThrow(() -> new WorkflowNotFoundException(workflowId));

        return WorkflowResponse.from(workflow);
    }

    /**
     * Get all workflows for a user.
     *
     * @param ownerId Owner user ID (from authentication context)
     * @return List of workflow responses
     */
    @Transactional(readOnly = true)
    public List<WorkflowResponse> getUserWorkflows(String ownerId) {
        log.debug("Retrieving workflows for owner: {}", ownerId);

        List<Workflow> workflows = workflowRepository.findByOwnerId(ownerId);

        log.debug("Found {} workflows for owner: {}", workflows.size(), ownerId);

        return workflows.stream()
                .map(WorkflowResponse::from)
                .toList();
    }

    /**
     * Delete a workflow.
     *
     * @param workflowId Workflow ID
     * @param ownerId Owner user ID (from authentication context)
     * @throws WorkflowNotFoundException if workflow not found or not owned by user
     */
    @Transactional
    public void deleteWorkflow(String workflowId, String ownerId) {
        log.info("Deleting workflow: id={}, owner={}", workflowId, ownerId);

        // Verify workflow exists and is owned by user
        if (!workflowRepository.existsByIdAndOwnerId(workflowId, ownerId)) {
            throw new WorkflowNotFoundException(workflowId);
        }

        workflowRepository.deleteById(workflowId);

        log.info("Deleted workflow: id={}", workflowId);
    }
}
