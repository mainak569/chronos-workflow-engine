package com.chronos.scheduler.config;

import com.chronos.scheduler.event.TaskCompletedEvent;
import com.chronos.scheduler.event.TaskFailedEvent;
import com.chronos.scheduler.event.TaskStartedEvent;
import com.chronos.scheduler.event.WorkerRegisteredEvent;
import com.chronos.scheduler.event.WorkerUnavailableEvent;
import com.chronos.scheduler.event.WorkflowCreatedEvent;
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
 * Kafka consumer configuration for scheduler-service.
 * Configures consumers for WorkflowCreatedEvent and task/worker status events.
 *
 * Events are produced by other services without type headers, so each consumer
 * deserializes into its own event class. Records that still fail after retries
 * (or cannot be deserialized) are sent to the matching dead-letter topic.
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

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    public KafkaConsumerConfig(KafkaTemplate<String, Object> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> eventType) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Session and heartbeat configuration
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        // Poll configuration
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        // Deserialization with error handling, always into this service's event class
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, eventType.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, T> factory = new DefaultKafkaConsumerFactory<>(config);
        // Expose Kafka client metrics (e.g. consumer lag) via Micrometer
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory(Class<T> eventType,
                                                                                int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(eventType));
        
        // Manual acknowledgment for at-least-once processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConcurrency(concurrency);
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

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkflowCreatedEvent> workflowCreatedKafkaListenerContainerFactory() {
        return listenerFactory(WorkflowCreatedEvent.class, 3);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskStartedEvent> taskStartedKafkaListenerContainerFactory() {
        return listenerFactory(TaskStartedEvent.class, 3);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskCompletedEvent> taskCompletedKafkaListenerContainerFactory() {
        return listenerFactory(TaskCompletedEvent.class, 3);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskFailedEvent> taskFailedKafkaListenerContainerFactory() {
        return listenerFactory(TaskFailedEvent.class, 3);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkerRegisteredEvent> workerRegisteredKafkaListenerContainerFactory() {
        return listenerFactory(WorkerRegisteredEvent.class, 2);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkerUnavailableEvent> workerUnavailableKafkaListenerContainerFactory() {
        return listenerFactory(WorkerUnavailableEvent.class, 2);
    }
}
