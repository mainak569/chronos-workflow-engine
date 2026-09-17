package com.chronos.workflow.repository;

import com.chronos.workflow.domain.ExecutionStatus;
import com.chronos.workflow.domain.TaskExecution;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for TaskExecution persistence.
 */
@Repository
public interface TaskExecutionRepository extends MongoRepository<TaskExecution, String> {
    
    /**
     * Find all task executions for a specific workflow execution.
     */
    List<TaskExecution> findByExecutionId(String executionId);
    
    /**
     * Find all task executions for a specific workflow execution with a specific status.
     */
    List<TaskExecution> findByExecutionIdAndStatus(String executionId, ExecutionStatus status);
    
    /**
     * Find a specific task execution within a workflow execution.
     */
    Optional<TaskExecution> findByExecutionIdAndTaskId(String executionId, String taskId);
    
    /**
     * Find all task executions for a workflow.
     */
    List<TaskExecution> findByWorkflowId(String workflowId);
    
    /**
     * Find all task executions assigned to a specific worker.
     */
    List<TaskExecution> findByWorkerId(String workerId);
    
    /**
     * Find all pending tasks for a specific execution (ready to be scheduled).
     */
    @Query("{ 'executionId': ?0, 'status': 'PENDING' }")
    List<TaskExecution> findPendingTasksByExecution(String executionId);
    
    /**
     * Find all running tasks for a specific execution.
     */
    @Query("{ 'executionId': ?0, 'status': 'RUNNING' }")
    List<TaskExecution> findRunningTasksByExecution(String executionId);
    
    /**
     * Find all completed tasks for a specific execution.
     */
    @Query("{ 'executionId': ?0, 'status': 'COMPLETED' }")
    List<TaskExecution> findCompletedTasksByExecution(String executionId);
    
    /**
     * Find all failed tasks for a specific execution.
     */
    @Query("{ 'executionId': ?0, 'status': 'FAILED' }")
    List<TaskExecution> findFailedTasksByExecution(String executionId);
    
    /**
     * Count tasks by execution and status.
     */
    long countByExecutionIdAndStatus(String executionId, ExecutionStatus status);
    
    /**
     * Count total tasks for an execution.
     */
    long countByExecutionId(String executionId);
    
    /**
     * Check if a task execution exists.
     */
    boolean existsByExecutionIdAndTaskId(String executionId, String taskId);
    
    /**
     * Delete all task executions for a workflow execution.
     */
    void deleteByExecutionId(String executionId);
}
