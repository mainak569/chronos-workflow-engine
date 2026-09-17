package com.chronos.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * Configuration for distributed locking using Redis.
 * Provides RedisTemplate configuration and lock parameters.
 */
@Configuration
public class LockConfiguration {
    
    /**
     * Configure RedisTemplate for lock operations.
     * Uses String serializers for both keys and values to ensure compatibility.
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Use String serializers for lock operations
        StringRedisSerializer serializer = new StringRedisSerializer();
        template.setKeySerializer(serializer);
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(serializer);
        template.setHashValueSerializer(serializer);
        
        template.afterPropertiesSet();
        return template;
    }
    
    /**
     * Lock properties loaded from application configuration.
     */
    @Bean
    @ConfigurationProperties(prefix = "chronos.lock")
    public LockProperties lockProperties() {
        return new LockProperties();
    }
    
    /**
     * Properties for distributed lock configuration.
     */
    public static class LockProperties {
        
        /**
         * Default TTL for task assignment locks (default: 5 minutes).
         * Should be longer than the maximum expected task execution time.
         */
        private Duration taskLockTtl = Duration.ofMinutes(5);
        
        /**
         * Lock key prefix for task locks.
         */
        private String taskLockPrefix = "chronos:lock:task:";
        
        // Getters and setters
        
        public Duration getTaskLockTtl() {
            return taskLockTtl;
        }
        
        public void setTaskLockTtl(Duration taskLockTtl) {
            this.taskLockTtl = taskLockTtl;
        }
        
        public String getTaskLockPrefix() {
            return taskLockPrefix;
        }
        
        public void setTaskLockPrefix(String taskLockPrefix) {
            this.taskLockPrefix = taskLockPrefix;
        }
        
        /**
         * Builds a task lock key for a given task ID.
         */
        public String buildTaskLockKey(String taskId) {
            return taskLockPrefix + taskId;
        }
    }
}
