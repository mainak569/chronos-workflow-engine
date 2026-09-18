package com.chronos.scheduler.repository;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository for TaskExecution persistence in scheduler-service.
 */
@Repository
public interface TaskExecutionRepository extends MongoRepository<TaskExecution, String> {
    
    List<TaskExecution> findByExecutionId(String executionId);
    
    List<TaskExecution> findByExecutionIdAndStatus(String executionId, ExecutionStatus status);
    
    Optional<TaskExecution> findByExecutionIdAndTaskId(String executionId, String taskId);
    
    @Query("{ 'executionId': ?0, 'status': 'PENDING' }")
    List<TaskExecution> findPendingTasksByExecution(String executionId);
    
    @Query("{ 'executionId': ?0, 'status': 'COMPLETED' }")
    List<TaskExecution> findCompletedTasksByExecution(String executionId);
    
    long countByExecutionIdAndStatus(String executionId, ExecutionStatus status);
    
    long countByExecutionId(String executionId);
    
    /**
     * Tasks currently executing on the given worker.
     */
    List<TaskExecution> findByStatusAndWorkerId(ExecutionStatus status, String workerId);
    
    /**
     * Retry attempts whose backoff has elapsed but that have not been dispatched yet.
     */
    @Query("{ 'status': 'PENDING', 'dispatchedAt': null, 'nextRetryAt': { $lte: ?0 } }")
    List<TaskExecution> findRetriesDue(Instant now, Pageable pageable);
    
    /**
     * Dispatched attempts that no worker has picked up since the cutoff
     * (TaskReady lost, dropped by a worker, or the scheduler crashed mid-dispatch).
     */
    @Query("{ 'status': 'PENDING', 'dispatchedAt': { $lt: ?0 } }")
    List<TaskExecution> findStaleDispatches(Instant cutoff, Pageable pageable);
}
