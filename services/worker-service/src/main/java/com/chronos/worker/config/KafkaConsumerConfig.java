package com.chronos.worker.config;

import com.chronos.worker.event.TaskReadyEvent;
import com.chronos.worker.kafka.TaskConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for worker-service.
 * Configures consumers for TaskReadyEvent.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    
    private final TaskConsumerRebalanceListener rebalanceListener;
    
    public KafkaConsumerConfig(TaskConsumerRebalanceListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
    }

    @Bean
    public ConsumerFactory<String, TaskReadyEvent> taskReadyConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Session timeout and heartbeat for failure detection
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        // Max poll settings
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000); // 10 minutes for task execution
        
        // Error handling deserializer wraps the actual deserializer
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TaskReadyEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskReadyEvent> taskReadyKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TaskReadyEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskReadyConsumerFactory());
        
        // Manual acknowledgment for at-least-once semantics with idempotency
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Register rebalance listener for graceful partition reassignment
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        
        // Concurrency matches number of task types worker can handle
        factory.setConcurrency(4);
        
        return factory;
    }
}
