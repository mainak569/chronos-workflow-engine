package com.chronos.scheduler.controller;

import com.chronos.scheduler.service.LockCleanupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for lock management and monitoring.
 * Provides endpoints for viewing lock statistics and manually triggering cleanup.
 * Requires the X-Admin-Token header (see AdminApiSecurityConfig).
 */
@RestController
@RequestMapping("/api/locks")
public class LockManagementController {
    
    private static final Logger logger = LoggerFactory.getLogger(LockManagementController.class);
    
    private final LockCleanupService lockCleanupService;
    
    public LockManagementController(LockCleanupService lockCleanupService) {
        this.lockCleanupService = lockCleanupService;
    }
    
    /**
     * Get statistics about current locks.
     * 
     * GET /api/locks/statistics
     * 
     * @return lock statistics including counts by type
     */
    @GetMapping("/statistics")
    public ResponseEntity<LockCleanupService.LockStatistics> getLockStatistics() {
        logger.debug("Retrieving lock statistics");
        
        LockCleanupService.LockStatistics stats = lockCleanupService.getLockStatistics();
        
        return ResponseEntity.ok(stats);
    }
    
    /**
     * Manually trigger cleanup of orphaned locks.
     * 
     * POST /api/locks/cleanup
     * 
     * @return response indicating cleanup was triggered
     */
    @PostMapping("/cleanup")
    public ResponseEntity<Map<String, String>> triggerCleanup() {
        logger.info("Manual lock cleanup triggered");
        
        // Trigger cleanup asynchronously
        try {
            lockCleanupService.cleanupOrphanedLocks();
            
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Lock cleanup completed"
            ));
        } catch (Exception e) {
            logger.error("Error during manual cleanup", e);
            
            return ResponseEntity.status(500).body(Map.of(
                    "status", "error",
                    "message", "Lock cleanup failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Clean up a specific lock by key.
     * 
     * DELETE /api/locks/{lockKey}
     * 
     * @param lockKey the lock key to clean up
     * @return response indicating whether the lock was cleaned up
     */
    @DeleteMapping("/{lockKey}")
    public ResponseEntity<Map<String, Object>> cleanupSpecificLock(@PathVariable String lockKey) {
        logger.info("Manual cleanup requested for lock: key={}", lockKey);
        
        if (!lockCleanupService.isManagedLockKey(lockKey)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "invalid",
                    "message", "Only task and execution lock keys can be deleted",
                    "lockKey", lockKey
            ));
        }
        
        boolean cleaned = lockCleanupService.cleanupSpecificLock(lockKey);
        
        if (cleaned) {
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Lock cleaned up successfully",
                    "lockKey", lockKey
            ));
        } else {
            return ResponseEntity.status(404).body(Map.of(
                    "status", "not_found",
                    "message", "Lock not found or could not be cleaned",
                    "lockKey", lockKey
            ));
        }
    }
}
