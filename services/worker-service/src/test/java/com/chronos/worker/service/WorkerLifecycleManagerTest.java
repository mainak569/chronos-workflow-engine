package com.chronos.worker.service;

import com.chronos.worker.domain.WorkerMetadata;
import com.chronos.worker.domain.WorkerStatus;
import com.chronos.worker.event.WorkerEventPublisher;
import com.chronos.worker.event.WorkerRegisteredEvent;
import com.chronos.worker.event.WorkerUnavailableEvent;
import com.chronos.worker.shutdown.GracefulShutdownManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("WorkerLifecycleManager Tests")
class WorkerLifecycleManagerTest {
    
    @Mock
    private WorkerRegistry workerRegistry;
    
    @Mock
    private WorkerHeartbeatScheduler heartbeatScheduler;
    
    @Mock
    private WorkerEventPublisher eventPublisher;
    
    @Mock
    private GracefulShutdownManager shutdownManager;
    
    @Mock
    private ContextRefreshedEvent contextRefreshedEvent;
    
    private WorkerLifecycleManager lifecycleManager;
    
    @BeforeEach
    void setUp() {
        lifecycleManager = new WorkerLifecycleManager(
                workerRegistry,
                heartbeatScheduler,
                eventPublisher,
                shutdownManager
        );
    }
    
    @Test
    @DisplayName("Should register worker on application ready")
    void shouldRegisterWorkerOnApplicationReady() {
        // Given
        String workerId = "worker-123";
        List<String> taskTypes = Arrays.asList("IMAGE_RESIZE", "IMAGE_COMPRESS");
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", taskTypes);
        
        // When
        lifecycleManager.onApplicationReady();
        
        // Then
        ArgumentCaptor<WorkerMetadata> workerCaptor = ArgumentCaptor.forClass(WorkerMetadata.class);
        verify(workerRegistry).registerWorker(workerCaptor.capture());
        
        WorkerMetadata registered = workerCaptor.getValue();
        assertThat(registered.getWorkerId()).isEqualTo(workerId);
        assertThat(registered.getStatus()).isEqualTo(WorkerStatus.REGISTERING);
        assertThat(registered.getSupportedTaskTypes()).containsExactlyElementsOf(taskTypes);
        
        verify(heartbeatScheduler).enableHeartbeat();
        verify(eventPublisher).publishWorkerRegistered(any(WorkerRegisteredEvent.class));
        
        assertThat(lifecycleManager.isRegistered()).isTrue();
        assertThat(lifecycleManager.getWorkerId()).isEqualTo(workerId);
    }
    
    @Test
    @DisplayName("Should fail fast if the worker ID is blank")
    void shouldFailIfWorkerIdBlank() {
        // Given: a blank ID (WorkerIdEnvironmentPostProcessor normally prevents this)
        ReflectionTestUtils.setField(lifecycleManager, "workerId", "");
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", Arrays.asList("IMAGE_RESIZE"));
        
        // When/Then: registration fails instead of inventing an ID that other components don't share
        assertThatThrownBy(() -> lifecycleManager.onApplicationReady())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Worker ID");
        
        verify(workerRegistry, never()).registerWorker(any(WorkerMetadata.class));
    }
    
    @Test
    @DisplayName("Should skip duplicate registration")
    void shouldSkipDuplicateRegistration() {
        // Given
        String workerId = "worker-123";
        List<String> taskTypes = Arrays.asList("IMAGE_RESIZE");
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", taskTypes);
        
        // Register once
        lifecycleManager.onApplicationReady();
        
        // When - try to register again
        lifecycleManager.onApplicationReady();
        
        // Then - should only register once
        verify(workerRegistry, times(1)).registerWorker(any(WorkerMetadata.class));
    }
    
    @Test
    @DisplayName("Should throw exception if no task types configured")
    void shouldThrowExceptionIfNoTaskTypes() {
        // Given
        String workerId = "worker-123";
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", null);
        
        // When/Then
        assertThatThrownBy(() -> lifecycleManager.onApplicationReady())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("at least one task type");
        
        verify(workerRegistry, never()).registerWorker(any(WorkerMetadata.class));
    }
    
    @Test
    @DisplayName("Should gracefully deregister worker on shutdown")
    void shouldGracefullyDeregisterWorkerOnShutdown() {
        // Given
        String workerId = "worker-123";
        List<String> taskTypes = Arrays.asList("IMAGE_RESIZE");
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", taskTypes);
        
        // Register first
        lifecycleManager.onApplicationReady();
        
        // When
        lifecycleManager.onShutdown();
        
        // Then
        verify(heartbeatScheduler).disableHeartbeat();
        verify(eventPublisher).publishWorkerUnavailable(any(WorkerUnavailableEvent.class));
        verify(workerRegistry).deregisterWorker(workerId);
        
        assertThat(lifecycleManager.isRegistered()).isFalse();
    }
    
    @Test
    @DisplayName("Should skip deregistration if not registered")
    void shouldSkipDeregistrationIfNotRegistered() {
        // Given
        String workerId = "worker-123";
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        
        // When - shutdown without registration
        lifecycleManager.onShutdown();
        
        // Then
        verify(workerRegistry, never()).deregisterWorker(anyString());
    }
    
    @Test
    @DisplayName("Should handle deregistration errors gracefully")
    void shouldHandleDeregistrationErrorsGracefully() {
        // Given
        String workerId = "worker-123";
        List<String> taskTypes = Arrays.asList("IMAGE_RESIZE");
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", taskTypes);
        
        lifecycleManager.onApplicationReady();
        
        // Simulate error during deregistration
        doThrow(new RuntimeException("Redis connection error"))
                .when(workerRegistry).deregisterWorker(workerId);
        
        // When - should not throw exception
        lifecycleManager.onShutdown();
        
        // Then - should complete shutdown despite error
        assertThat(lifecycleManager.isRegistered()).isFalse();
    }
    
    @Test
    @DisplayName("Should return correct registration status")
    void shouldReturnCorrectRegistrationStatus() {
        // Given
        String workerId = "worker-123";
        List<String> taskTypes = Arrays.asList("IMAGE_RESIZE");
        
        ReflectionTestUtils.setField(lifecycleManager, "workerId", workerId);
        ReflectionTestUtils.setField(lifecycleManager, "supportedTaskTypes", taskTypes);
        
        // Initially not registered
        assertThat(lifecycleManager.isRegistered()).isFalse();
        
        // After registration
        lifecycleManager.onApplicationReady();
        assertThat(lifecycleManager.isRegistered()).isTrue();
        
        // After deregistration
        lifecycleManager.onShutdown();
        assertThat(lifecycleManager.isRegistered()).isFalse();
    }
}
