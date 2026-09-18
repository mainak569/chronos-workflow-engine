package com.chronos.scheduler.repository;

import com.chronos.scheduler.domain.WorkflowDefinition;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Read access to workflow definitions stored by workflow-service.
 */
@Repository
public interface WorkflowDefinitionRepository extends MongoRepository<WorkflowDefinition, String> {

    /**
     * Workflows that have a cron schedule.
     */
    @Query(value = "{ 'schedule': { $nin: [null, ''] } }")
    List<WorkflowDefinition> findScheduled();
}
