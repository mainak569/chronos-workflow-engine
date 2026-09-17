package com.chronos.workflow.repository;

import com.chronos.workflow.domain.User;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for User persistence.
 */
@Repository
public interface UserRepository extends MongoRepository<User, String> {
    
    /**
     * Find user by email address.
     * Used for login and uniqueness checks.
     */
    Optional<User> findByEmail(String email);
    
    /**
     * Check if email is already registered.
     */
    boolean existsByEmail(String email);
    
    /**
     * Find user by username (for display purposes).
     */
    Optional<User> findByUsername(String username);
}
