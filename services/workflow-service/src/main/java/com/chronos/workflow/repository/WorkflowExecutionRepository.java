package com.chronos.workflow.repository;

import com.chronos.workflow.domain.ExecutionStatus;
import com.chronos.workflow.domain.WorkflowExecution;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository for WorkflowExecution persistence.
 */
@Repository
public interface WorkflowExecutionRepository extends MongoRepository<WorkflowExecution, String> {
    
    /**
     * Find all executions for a specific workflow.
     */
    List<WorkflowExecution> findByWorkflowId(String workflowId);
    
    /**
     * Find all executions for a specific workflow with a specific status.
     */
    List<WorkflowExecution> findByWorkflowIdAndStatus(String workflowId, ExecutionStatus status);
    
    /**
     * Find all executions triggered by a specific user.
     */
    List<WorkflowExecution> findByTriggeredBy(String userId);
    
    /**
     * Find all executions with a specific status.
     */
    List<WorkflowExecution> findByStatus(ExecutionStatus status);
    
    /**
     * Find all executions created after a specific timestamp.
     */
    List<WorkflowExecution> findByCreatedAtAfter(Instant timestamp);
    
    /**
     * Find all running executions (for monitoring and recovery).
     */
    @Query("{ 'status': 'RUNNING' }")
    List<WorkflowExecution> findAllRunningExecutions();
    
    /**
     * Find all non-terminal executions (PENDING or RUNNING).
     */
    @Query("{ 'status': { $in: ['PENDING', 'RUNNING'] } }")
    List<WorkflowExecution> findAllActiveExecutions();
    
    /**
     * Count executions by workflow and status.
     */
    long countByWorkflowIdAndStatus(String workflowId, ExecutionStatus status);
    
    /**
     * Check if an execution exists with the given ID.
     */
    boolean existsById(String id);
}
