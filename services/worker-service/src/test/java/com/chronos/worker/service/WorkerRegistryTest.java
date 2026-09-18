package com.chronos.worker.service;

import com.chronos.worker.domain.WorkerMetadata;
import com.chronos.worker.domain.WorkerStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("WorkerRegistry Tests")
class WorkerRegistryTest {
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    @Mock
    private SetOperations<String, String> setOperations;
    
    private ObjectMapper objectMapper;
    private WorkerRegistry workerRegistry;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules(); // Register JavaTimeModule
        workerRegistry = new WorkerRegistry(redisTemplate, objectMapper);
        
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForSet()).thenReturn(setOperations);
    }
    
    @Test
    @DisplayName("Should register worker successfully")
    void shouldRegisterWorkerSuccessfully() throws Exception {
        // Given
        WorkerMetadata worker = WorkerMetadata.builder()
                .workerId("worker-1")
                .status(WorkerStatus.REGISTERING)
                .supportedTaskTypes(Arrays.asList("IMAGE_RESIZE", "IMAGE_COMPRESS"))
                .build();
        
        // When
        workerRegistry.registerWorker(worker);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        
        // Verify metadata stored
        verify(valueOperations, atLeastOnce()).set(keyCaptor.capture(), valueCaptor.capture());
        
        // Verify heartbeat created with TTL
        verify(valueOperations).set(
                eq("worker:heartbeat:worker-1"),
                anyString(),
                eq(30L),
                eq(TimeUnit.SECONDS)
        );
        
        // Verify status index updated
        verify(setOperations).add("worker:index:status:AVAILABLE", "worker-1");
        
        // Verify task type indexes updated
        verify(setOperations).add("worker:index:taskType:IMAGE_RESIZE", "worker-1");
        verify(setOperations).add("worker:index:taskType:IMAGE_COMPRESS", "worker-1");
    }
    
    @Test
    @DisplayName("Should reject registration if worker not in REGISTERING status")
    void shouldRejectRegistrationIfNotRegistering() {
        // Given
        WorkerMetadata worker = WorkerMetadata.builder()
                .workerId("worker-1")
                .status(WorkerStatus.AVAILABLE)
                .supportedTaskTypes(Arrays.asList("IMAGE_RESIZE"))
                .build();
        
        // When/Then
        assertThatThrownBy(() -> workerRegistry.registerWorker(worker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REGISTERING");
    }
    
    @Test
    @DisplayName("Should update heartbeat timestamp and TTL")
    void shouldUpdateHeartbeat() throws Exception {
        // Given
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        workerRegistry.updateHeartbeat(workerId);
        
        // Then
        verify(valueOperations).set(
                eq("worker:heartbeat:worker-1"),
                anyString(),
                eq(30L),
                eq(TimeUnit.SECONDS)
        );
        
        // Verify metadata updated with the new lastHeartbeat
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq("worker:metadata:worker-1"), valueCaptor.capture());
        
        WorkerMetadata updated = objectMapper.readValue(valueCaptor.getValue(), WorkerMetadata.class);
        assertThat(updated.getLastHeartbeat()).isNotNull();
    }
    
    @Test
    @DisplayName("Should mark worker as busy")
    void shouldMarkWorkerBusy() throws Exception {
        // Given
        String workerId = "worker-1";
        String executionId = "exec-123";
        String taskId = "task-456";
        WorkerMetadata worker = createAvailableWorker(workerId);
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        workerRegistry.markWorkerBusy(workerId, executionId, taskId);
        
        // Then
        verify(setOperations).remove("worker:index:status:AVAILABLE", workerId);
        verify(setOperations).add("worker:index:status:BUSY", workerId);
        
        // Verify status updated in metadata
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq("worker:metadata:worker-1"), valueCaptor.capture());
        
        WorkerMetadata updated = objectMapper.readValue(valueCaptor.getValue(), WorkerMetadata.class);
        assertThat(updated.getStatus()).isEqualTo(WorkerStatus.BUSY);
        assertThat(updated.getCurrentExecutionId()).isEqualTo(executionId);
        assertThat(updated.getCurrentTaskId()).isEqualTo(taskId);
    }
    
    @Test
    @DisplayName("Should mark worker as available")
    void shouldMarkWorkerAvailable() throws Exception {
        // Given
        String workerId = "worker-1";
        WorkerMetadata worker = WorkerMetadata.builder()
                .workerId(workerId)
                .status(WorkerStatus.BUSY)
                .supportedTaskTypes(Arrays.asList("IMAGE_RESIZE"))
                .lastHeartbeat(Instant.now())
                .build();
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        workerRegistry.markWorkerAvailable(workerId);
        
        // Then
        verify(setOperations).remove("worker:index:status:BUSY", workerId);
        verify(setOperations).add("worker:index:status:AVAILABLE", workerId);
        
        // Verify status updated in metadata
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq("worker:metadata:worker-1"), valueCaptor.capture());
        
        WorkerMetadata updated = objectMapper.readValue(valueCaptor.getValue(), WorkerMetadata.class);
        assertThat(updated.getStatus()).isEqualTo(WorkerStatus.AVAILABLE);
    }
    
    @Test
    @DisplayName("Should mark worker as unavailable")
    void shouldMarkWorkerUnavailable() throws Exception {
        // Given
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        workerRegistry.markWorkerUnavailable(workerId);
        
        // Then
        verify(setOperations).remove("worker:index:status:AVAILABLE", workerId);
        verify(setOperations).add("worker:index:status:UNAVAILABLE", workerId);
        
        // Verify status updated in metadata
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq("worker:metadata:worker-1"), valueCaptor.capture());
        
        WorkerMetadata updated = objectMapper.readValue(valueCaptor.getValue(), WorkerMetadata.class);
        assertThat(updated.getStatus()).isEqualTo(WorkerStatus.UNAVAILABLE);
    }
    
    @Test
    @DisplayName("Should deregister worker and cleanup resources")
    void shouldDeregisterWorker() throws Exception {
        // Given
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        workerRegistry.deregisterWorker(workerId);
        
        // Then
        // Verify status index cleanup
        verify(setOperations).remove("worker:index:status:AVAILABLE", workerId);
        
        // Verify task type indexes cleanup
        verify(setOperations).remove("worker:index:taskType:IMAGE_RESIZE", workerId);
        verify(setOperations).remove("worker:index:taskType:IMAGE_COMPRESS", workerId);
        
        // Verify heartbeat key deleted
        verify(redisTemplate).delete("worker:heartbeat:worker-1");
        
        // Verify status updated to STOPPED
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq("worker:metadata:worker-1"), valueCaptor.capture());
        
        WorkerMetadata updated = objectMapper.readValue(valueCaptor.getValue(), WorkerMetadata.class);
        assertThat(updated.getStatus()).isEqualTo(WorkerStatus.STOPPED);
    }
    
    @Test
    @DisplayName("Should get worker by ID")
    void shouldGetWorkerById() throws Exception {
        // Given
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));
        
        // When
        WorkerMetadata result = workerRegistry.getWorker(workerId);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getWorkerId()).isEqualTo(workerId);
        assertThat(result.getStatus()).isEqualTo(WorkerStatus.AVAILABLE);
    }
    
    @Test
    @DisplayName("Should return null for non-existent worker")
    void shouldReturnNullForNonExistentWorker() {
        // Given
        when(valueOperations.get("worker:metadata:worker-999")).thenReturn(null);
        
        // When
        WorkerMetadata result = workerRegistry.getWorker("worker-999");
        
        // Then
        assertThat(result).isNull();
    }
    
    @Test
    @DisplayName("Should get workers by status")
    void shouldGetWorkersByStatus() throws Exception {
        // Given
        WorkerMetadata worker1 = createAvailableWorker("worker-1");
        WorkerMetadata worker2 = createAvailableWorker("worker-2");
        
        when(setOperations.members("worker:index:status:AVAILABLE"))
                .thenReturn(Set.of("worker-1", "worker-2"));
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker1));
        when(valueOperations.get("worker:metadata:worker-2"))
                .thenReturn(objectMapper.writeValueAsString(worker2));
        
        // When
        List<WorkerMetadata> workers = workerRegistry.getWorkersByStatus(WorkerStatus.AVAILABLE);
        
        // Then
        assertThat(workers).hasSize(2);
        assertThat(workers).extracting(WorkerMetadata::getWorkerId)
                .containsExactlyInAnyOrder("worker-1", "worker-2");
    }
    
    @Test
    @DisplayName("Should get workers by task type")
    void shouldGetWorkersByTaskType() throws Exception {
        // Given
        WorkerMetadata worker1 = createAvailableWorker("worker-1");
        
        when(setOperations.members("worker:index:taskType:IMAGE_RESIZE"))
                .thenReturn(Set.of("worker-1"));
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker1));
        
        // When
        List<WorkerMetadata> workers = workerRegistry.getWorkersByTaskType("IMAGE_RESIZE");
        
        // Then
        assertThat(workers).hasSize(1);
        assertThat(workers.get(0).getWorkerId()).isEqualTo("worker-1");
        assertThat(workers.get(0).getSupportedTaskTypes()).contains("IMAGE_RESIZE");
    }
    
    @Test
    @DisplayName("Should get available workers for task type")
    void shouldGetAvailableWorkersForTaskType() throws Exception {
        // Given
        WorkerMetadata availableWorker = createAvailableWorker("worker-1");
        WorkerMetadata busyWorker = WorkerMetadata.builder()
                .workerId("worker-2")
                .status(WorkerStatus.BUSY)
                .supportedTaskTypes(Arrays.asList("IMAGE_RESIZE"))
                .lastHeartbeat(Instant.now())
                .build();
        
        when(setOperations.members("worker:index:taskType:IMAGE_RESIZE"))
                .thenReturn(Set.of("worker-1", "worker-2"));
        
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(availableWorker));
        when(valueOperations.get("worker:metadata:worker-2"))
                .thenReturn(objectMapper.writeValueAsString(busyWorker));
        
        // When
        List<WorkerMetadata> workers = workerRegistry.getAvailableWorkers("IMAGE_RESIZE");
        
        // Then
        assertThat(workers).hasSize(1);
        assertThat(workers.get(0).getWorkerId()).isEqualTo("worker-1");
        assertThat(workers.get(0).getStatus()).isEqualTo(WorkerStatus.AVAILABLE);
    }
    
    @Test
    @DisplayName("Should detect expired heartbeat")
    void shouldDetectExpiredHeartbeat() {
        // Given
        String workerId = "worker-1";
        when(redisTemplate.hasKey("worker:heartbeat:worker-1")).thenReturn(false);
        
        // When
        boolean expired = workerRegistry.isHeartbeatExpired(workerId);
        
        // Then
        assertThat(expired).isTrue();
    }
    
    @Test
    @DisplayName("Should detect active heartbeat")
    void shouldDetectActiveHeartbeat() {
        // Given
        String workerId = "worker-1";
        when(redisTemplate.hasKey("worker:heartbeat:worker-1")).thenReturn(true);
        
        // When
        boolean expired = workerRegistry.isHeartbeatExpired(workerId);
        
        // Then
        assertThat(expired).isFalse();
    }
    
    // Helper methods
    
    private WorkerMetadata createAvailableWorker(String workerId) {
        return WorkerMetadata.builder()
                .workerId(workerId)
                .status(WorkerStatus.AVAILABLE)
                .supportedTaskTypes(Arrays.asList("IMAGE_RESIZE", "IMAGE_COMPRESS"))
                .lastHeartbeat(Instant.now())
                .build();
    }

    @Test
    @DisplayName("Should stay BUSY while concurrent tasks are running and become AVAILABLE after the last one")
    void shouldTrackConcurrentTasks() throws Exception {
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));

        // Two tasks start concurrently on the same worker
        workerRegistry.markWorkerBusy(workerId, "exec-1", "task-1");
        workerRegistry.markWorkerBusy(workerId, "exec-2", "task-2");

        // First task finishes: worker keeps running the second one
        workerRegistry.markWorkerAvailable(workerId);
        verify(setOperations, never()).add("worker:index:status:AVAILABLE", workerId);

        // Second task finishes: worker is available again
        workerRegistry.markWorkerAvailable(workerId);
        verify(setOperations).add("worker:index:status:AVAILABLE", workerId);
    }

    @Test
    @DisplayName("Should mark an already BUSY worker busy again without failing")
    void shouldAllowRepeatedBusyTransitions() throws Exception {
        String workerId = "worker-1";
        WorkerMetadata worker = createAvailableWorker(workerId);
        worker.markBusy("exec-0", "task-0");
        when(valueOperations.get("worker:metadata:worker-1"))
                .thenReturn(objectMapper.writeValueAsString(worker));

        assertThatCode(() -> workerRegistry.markWorkerBusy(workerId, "exec-1", "task-1"))
                .doesNotThrowAnyException();
    }
}
