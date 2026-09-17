package com.chronos.scheduler.repository;

import com.chronos.scheduler.domain.SchedulerState;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository for SchedulerState persistence.
 */
@Repository
public interface SchedulerStateRepository extends MongoRepository<SchedulerState, String> {
    
    /**
     * Find scheduler state by workflow ID.
     */
    Optional<SchedulerState> findByWorkflowId(String workflowId);
    
    /**
     * Find all enabled workflows ready to execute.
     * Used by scheduler to identify workflows that should run now.
     * 
     * @param now current time
     * @return list of workflows ready to execute
     */
    @Query("{ 'enabled': true, 'nextScheduledTime': { $lte: ?0 } }")
    List<SchedulerState> findReadyToExecute(Instant now);
    
    /**
     * Find all enabled workflows.
     */
    List<SchedulerState> findByEnabledTrue();
    
    /**
     * Find workflows that haven't been executed recently.
     * Useful for detecting stale or stuck workflows.
     * 
     * @param threshold time threshold
     * @return workflows not executed since threshold
     */
    @Query("{ 'enabled': true, 'lastExecutionTime': { $lt: ?0 } }")
    List<SchedulerState> findStaleWorkflows(Instant threshold);
    
    /**
     * Count enabled workflows.
     */
    long countByEnabledTrue();
    
    /**
     * Check if workflow has scheduler state.
     */
    boolean existsByWorkflowId(String workflowId);
}
