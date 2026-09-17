package com.chronos.workflow.repository;

import com.chronos.workflow.domain.Workflow;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for workflow persistence.
 */
@Repository
public interface WorkflowRepository extends MongoRepository<Workflow, String> {

    /**
     * Find all workflows owned by a specific user.
     *
     * @param ownerId User ID
     * @return List of workflows
     */
    List<Workflow> findByOwnerId(String ownerId);

    /**
     * Find a workflow by ID and owner ID.
     * Ensures users can only access their own workflows.
     *
     * @param id Workflow ID
     * @param ownerId Owner ID
     * @return Optional workflow
     */
    Optional<Workflow> findByIdAndOwnerId(String id, String ownerId);

    /**
     * Find workflows by name and owner ID.
     *
     * @param name Workflow name
     * @param ownerId Owner ID
     * @return List of workflows
     */
    List<Workflow> findByNameAndOwnerId(String name, String ownerId);

    /**
     * Check if a workflow exists for a given owner.
     *
     * @param id Workflow ID
     * @param ownerId Owner ID
     * @return true if exists, false otherwise
     */
    boolean existsByIdAndOwnerId(String id, String ownerId);
}
