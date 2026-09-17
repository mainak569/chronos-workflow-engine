package com.chronos.scheduler.config;

import com.chronos.scheduler.event.WorkflowCreatedEvent;
import com.chronos.scheduler.event.TaskCompletedEvent;
import com.chronos.scheduler.event.TaskFailedEvent;
import com.chronos.scheduler.event.WorkerRegisteredEvent;
import com.chronos.scheduler.event.WorkerUnavailableEvent;
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
 * Kafka consumer configuration for scheduler-service.
 * Configures consumers for WorkflowCreatedEvent and task/worker status events.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private Map<String, Object> commonConsumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Session timeout and heartbeat for failure detection
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        // Max poll settings
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        return config;
    }

    @Bean
    public ConsumerFactory<String, WorkflowCreatedEvent> workflowCreatedConsumerFactory() {
        Map<String, Object> config = commonConsumerConfig();
        
        // Error handling deserializer wraps the actual deserializer
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, WorkflowCreatedEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkflowCreatedEvent> workflowCreatedKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, WorkflowCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(workflowCreatedConsumerFactory());
        
        // Manual acknowledgment for at-least-once semantics with idempotency
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Concurrency for parallel processing
        factory.setConcurrency(3);
        
        return factory;
    }

    @Bean
    public ConsumerFactory<String, TaskCompletedEvent> taskCompletedConsumerFactory() {
        Map<String, Object> config = commonConsumerConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TaskCompletedEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskCompletedEvent> taskCompletedKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TaskCompletedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskCompletedConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, TaskFailedEvent> taskFailedConsumerFactory() {
        Map<String, Object> config = commonConsumerConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TaskFailedEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskFailedEvent> taskFailedKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TaskFailedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskFailedConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, WorkerRegisteredEvent> workerRegisteredConsumerFactory() {
        Map<String, Object> config = commonConsumerConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, WorkerRegisteredEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkerRegisteredEvent> workerRegisteredKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, WorkerRegisteredEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(workerRegisteredConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(2);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, WorkerUnavailableEvent> workerUnavailableConsumerFactory() {
        Map<String, Object> config = commonConsumerConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, WorkerUnavailableEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkerUnavailableEvent> workerUnavailableKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, WorkerUnavailableEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(workerUnavailableConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(2);
        return factory;
    }
}
