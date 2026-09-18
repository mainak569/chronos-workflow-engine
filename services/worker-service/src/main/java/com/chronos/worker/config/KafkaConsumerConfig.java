package com.chronos.worker.config;

import com.chronos.worker.event.TaskReadyEvent;
import com.chronos.worker.kafka.TaskConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
    
    @Value("${chronos.kafka.listener.auto-startup:true}")
    private boolean autoStartup;
    
    @Value("${chronos.kafka.listener.retry-attempts:3}")
    private long retryAttempts;
    
    @Value("${chronos.kafka.listener.retry-interval:1000}")
    private long retryIntervalMs;
    
    private final TaskConsumerRebalanceListener rebalanceListener;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    
    public KafkaConsumerConfig(TaskConsumerRebalanceListener rebalanceListener,
                               KafkaTemplate<String, Object> kafkaTemplate,
                               MeterRegistry meterRegistry) {
        this.rebalanceListener = rebalanceListener;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
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
        // Events come from the scheduler without type headers: always use this service's class
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TaskReadyEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
        DefaultKafkaConsumerFactory<String, TaskReadyEvent> factory = new DefaultKafkaConsumerFactory<>(config);
        // Expose Kafka client metrics (e.g. consumer lag) via Micrometer
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskReadyEvent> taskReadyKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TaskReadyEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskReadyConsumerFactory());
        
        // Manual acknowledgment for at-least-once semantics with idempotency
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Retry failed records, then publish them to "{topic}.dlt"
        factory.setCommonErrorHandler(errorHandler());
        
        // Register rebalance listener for graceful partition reassignment
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        
        // Concurrency matches number of task types worker can handle
        factory.setConcurrency(4);
        factory.setAutoStartup(autoStartup);
        
        return factory;
    }

    /**
     * Retry failed records a few times, then publish them to "{topic}.dlt".
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaTemplate<String, byte[]> rawTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));

        // Undeserializable records carry their raw bytes; everything else is re-serialized as JSON
        Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, rawTemplate);
        templates.put(Object.class, kafkaTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".dlt", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(retryIntervalMs, retryAttempts));
    }
}
