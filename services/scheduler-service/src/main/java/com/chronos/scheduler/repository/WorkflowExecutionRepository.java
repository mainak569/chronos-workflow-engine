package com.chronos.scheduler.repository;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.WorkflowExecution;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for WorkflowExecution persistence in scheduler-service.
 */
@Repository
public interface WorkflowExecutionRepository extends MongoRepository<WorkflowExecution, String> {
    
    List<WorkflowExecution> findByWorkflowId(String workflowId);
    
    List<WorkflowExecution> findByStatus(ExecutionStatus status);
    
    List<WorkflowExecution> findByStatusIn(List<ExecutionStatus> statuses);
    
    long countByStatusIn(List<ExecutionStatus> statuses);
    
    Optional<WorkflowExecution> findById(String id);
}
