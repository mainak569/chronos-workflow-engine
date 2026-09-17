package com.chronos.worker.config;

import com.chronos.worker.event.TaskStartedEvent;
import com.chronos.worker.event.TaskCompletedEvent;
import com.chronos.worker.event.TaskFailedEvent;
import com.chronos.worker.event.WorkerRegisteredEvent;
import com.chronos.worker.event.WorkerUnavailableEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for worker-service.
 * Configures producers for publishing task and worker status events.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> commonProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Idempotence for exactly-once semantics
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Retry backoff
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        
        // Compression for network efficiency
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return config;
    }

    @Bean
    public ProducerFactory<String, TaskStartedEvent> taskStartedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, TaskStartedEvent> taskStartedKafkaTemplate() {
        return new KafkaTemplate<>(taskStartedProducerFactory());
    }

    @Bean
    public ProducerFactory<String, TaskCompletedEvent> taskCompletedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, TaskCompletedEvent> taskCompletedKafkaTemplate() {
        return new KafkaTemplate<>(taskCompletedProducerFactory());
    }

    @Bean
    public ProducerFactory<String, TaskFailedEvent> taskFailedProducerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, TaskFailedEvent> taskFailedKafkaTemplate() {
        return new KafkaTemplate<>(taskFailedProducerFactory());
    }

    @Bean
    public ProducerFactory<String, WorkerRegisteredEvent> workerRegisteredProducerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, WorkerRegisteredEvent> workerRegisteredKafkaTemplate() {
        return new KafkaTemplate<>(workerRegisteredProducerFactory());
    }

    @Bean
    public ProducerFactory<String, WorkerUnavailableEvent> workerUnavailableProducerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, WorkerUnavailableEvent> workerUnavailableKafkaTemplate() {
        return new KafkaTemplate<>(workerUnavailableProducerFactory());
    }
}
