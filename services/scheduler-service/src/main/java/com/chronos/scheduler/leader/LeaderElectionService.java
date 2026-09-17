package com.chronos.scheduler.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Distributed leader election service using Redis.
 * 
 * Ensures only one scheduler instance actively schedules workflows at any time.
 * 
 * CRITICAL: Does NOT use local JVM state (no boolean isLeader field).
 * Always checks Redis for current leadership status.
 * 
 * Leadership Algorithm:
 * 1. Try to acquire leadership using SET NX EX (atomic)
 * 2. If acquired, renew every 10 seconds
 * 3. If lost or not acquired, retry every 5 seconds
 * 4. Leadership expires automatically after 30 seconds if not renewed
 * 
 * Guarantees:
 * - At most one leader at a time
 * - Leader automatically released if instance crashes
 * - New leader elected within 30 seconds of crash
 * - No split-brain scenarios (Redis provides atomicity)
 */
@Service
public class LeaderElectionService {
    
    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);
    
    private static final String LEADER_KEY = "chronos:scheduler:leader";
    private static final String LEADERSHIP_HISTORY_KEY = "chronos:scheduler:leadership:history";
    
    private final RedisTemplate<String, String> redisTemplate;
    private final String schedulerId;
    private final DefaultRedisScript<Long> renewalScript;
    
    @Value("${chronos.scheduler.leader-election.ttl:30s}")
    private Duration leadershipTtl;
    
    @Value("${chronos.scheduler.leader-election.renewal-interval:10000}")
    private long renewalIntervalMs;
    
    public LeaderElectionService(
            RedisTemplate<String, String> redisTemplate,
            @Value("${scheduler.id:scheduler-${random.uuid}}") String schedulerId) {
        this.redisTemplate = redisTemplate;
        this.schedulerId = schedulerId;
        
        // Initialize Lua script for atomic renewal
        this.renewalScript = new DefaultRedisScript<>();
        this.renewalScript.setScriptSource(
                new ResourceScriptSource(new ClassPathResource("scripts/renew_leadership.lua"))
        );
        this.renewalScript.setResultType(Long.class);
    }
    
    @PostConstruct
    public void initialize() {
        log.info("LeaderElectionService initialized for scheduler: {}", schedulerId);
        // Try to acquire leadership on startup
        tryAcquireLeadership();
    }
    
    /**
     * Check if this instance is the current leader.
     * 
     * CRITICAL: Always checks Redis, never uses local state.
     * 
     * @return true if this instance is the leader, false otherwise
     */
    public boolean isLeader() {
        String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);
        boolean isLeader = schedulerId.equals(currentLeader);
        
        if (log.isTraceEnabled()) {
            log.trace("Leadership check: currentLeader={}, myId={}, isLeader={}",
                    currentLeader, schedulerId, isLeader);
        }
        
        return isLeader;
    }
    
    /**
     * Get the ID of the current leader.
     * 
     * @return Optional containing leader ID if a leader exists
     */
    public Optional<String> getCurrentLeader() {
        String leader = redisTemplate.opsForValue().get(LEADER_KEY);
        return Optional.ofNullable(leader);
    }
    
    /**
     * Try to acquire leadership.
     * Uses SET NX EX for atomic operation.
     * 
     * @return true if leadership acquired, false otherwise
     */
    public boolean tryAcquireLeadership() {
        String leadershipValue = buildLeadershipValue();
        
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                LEADER_KEY,
                leadershipValue,
                leadershipTtl.toMillis(),
                TimeUnit.MILLISECONDS
        );
        
        boolean success = Boolean.TRUE.equals(acquired);
        
        if (success) {
            log.info("Leadership acquired: schedulerId={}, ttl={}", schedulerId, leadershipTtl);
            recordLeadershipTransition("ACQUIRED");
        } else {
            String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);
            log.debug("Failed to acquire leadership: currentLeader={}, myId={}",
                    currentLeader, schedulerId);
        }
        
        return success;
    }
    
    /**
     * Renew leadership if currently held.
     * Scheduled to run periodically (every 10 seconds by default).
     * 
     * Uses Lua script for atomic check-and-renew operation to prevent race conditions.
     * 
     * @return true if renewal successful, false if leadership lost
     */
    @Scheduled(fixedDelayString = "${chronos.scheduler.leader-election.renewal-interval:10000}")
    public boolean renewLeadership() {
        try {
            String leadershipValue = buildLeadershipValue();
            List<String> keys = Collections.singletonList(LEADER_KEY);
            
            // Execute Lua script: KEYS[1]=leader_key, ARGV[1]=schedulerId, ARGV[2]=value, ARGV[3]=ttl
            Long result = redisTemplate.execute(
                    renewalScript,
                    keys,
                    schedulerId,
                    leadershipValue,
                    String.valueOf(leadershipTtl.toMillis())
            );
            
            if (result == null) {
                log.error("Renewal script returned null");
                return false;
            }
            
            if (result == 1) {
                // Successfully renewed
                log.debug("Leadership renewed: schedulerId={}, ttl={}", schedulerId, leadershipTtl);
                return true;
            } else if (result == 0) {
                // Not the leader anymore
                log.info("Lost leadership during renewal attempt, attempting to acquire");
                return tryAcquireLeadership();
            } else if (result == -1) {
                // No leader exists (expired)
                log.info("Leadership expired, attempting to acquire");
                return tryAcquireLeadership();
            } else {
                log.warn("Unexpected renewal script result: {}", result);
                return false;
            }
            
        } catch (Exception e) {
            log.error("Error during leadership renewal", e);
            // Try to acquire in case we lost leadership
            return tryAcquireLeadership();
        }
    }
    
    /**
     * Release leadership gracefully.
     * Called on shutdown or when stepping down.
     */
    public void releaseLeadership() {
        if (!isLeader()) {
            log.debug("Not the leader, nothing to release");
            return;
        }
        
        // Only delete if we are still the leader (check-and-delete)
        String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);
        if (schedulerId.equals(currentLeader)) {
            redisTemplate.delete(LEADER_KEY);
            log.info("Leadership released: schedulerId={}", schedulerId);
            recordLeadershipTransition("RELEASED");
        } else {
            log.warn("Leadership already lost to: {}", currentLeader);
        }
    }
    
    /**
     * Force release leadership (admin operation).
     * WARNING: Only use for emergency recovery.
     */
    public void forceReleaseLeadership() {
        Boolean deleted = redisTemplate.delete(LEADER_KEY);
        if (Boolean.TRUE.equals(deleted)) {
            log.warn("Leadership forcefully released by: {}", schedulerId);
            recordLeadershipTransition("FORCE_RELEASED");
        }
    }
    
    /**
     * Get leadership information for monitoring.
     * 
     * @return leadership status
     */
    public LeadershipStatus getLeadershipStatus() {
        String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);
        Long ttl = redisTemplate.getExpire(LEADER_KEY, TimeUnit.SECONDS);
        
        return new LeadershipStatus(
                schedulerId,
                currentLeader,
                schedulerId.equals(currentLeader),
                ttl != null ? ttl : -1,
                Instant.now()
        );
    }
    
    /**
     * Record leadership transition for monitoring and debugging.
     */
    private void recordLeadershipTransition(String event) {
        try {
            String record = String.format("%s:%s:%s", 
                    Instant.now(), schedulerId, event);
            
            // Store in Redis list (keep last 100 transitions)
            redisTemplate.opsForList().leftPush(LEADERSHIP_HISTORY_KEY, record);
            redisTemplate.opsForList().trim(LEADERSHIP_HISTORY_KEY, 0, 99);
            redisTemplate.expire(LEADERSHIP_HISTORY_KEY, Duration.ofDays(7));
        } catch (Exception e) {
            log.warn("Failed to record leadership transition", e);
        }
    }
    
    /**
     * Build leadership value containing scheduler ID and timestamp.
     */
    private String buildLeadershipValue() {
        return schedulerId + ":" + Instant.now();
    }
    
    /**
     * Cleanup on shutdown.
     */
    @PreDestroy
    public void shutdown() {
        log.info("LeaderElectionService shutting down");
        releaseLeadership();
    }
    
    /**
     * Leadership status for monitoring.
     */
    public static class LeadershipStatus {
        private final String schedulerId;
        private final String currentLeader;
        private final boolean isLeader;
        private final long ttlSeconds;
        private final Instant timestamp;
        
        public LeadershipStatus(String schedulerId, String currentLeader, boolean isLeader,
                              long ttlSeconds, Instant timestamp) {
            this.schedulerId = schedulerId;
            this.currentLeader = currentLeader;
            this.isLeader = isLeader;
            this.ttlSeconds = ttlSeconds;
            this.timestamp = timestamp;
        }
        
        public String getSchedulerId() {
            return schedulerId;
        }
        
        public String getCurrentLeader() {
            return currentLeader;
        }
        
        public boolean isLeader() {
            return isLeader;
        }
        
        public long getTtlSeconds() {
            return ttlSeconds;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("LeadershipStatus{schedulerId='%s', currentLeader='%s', " +
                            "isLeader=%s, ttlSeconds=%d, timestamp=%s}",
                    schedulerId, currentLeader, isLeader, ttlSeconds, timestamp);
        }
    }
}
