package com.chronos.scheduler.service;

import com.chronos.scheduler.config.LockConfiguration;
import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.lock.DistributedLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WorkerFailureHandlerTest {

    @Mock
    private ExecutionOrchestrationService orchestrationService;

    @Mock
    private TaskDispatchService taskDispatchService;

    @Mock
    private DistributedLock distributedLock;

    @Test
    void requeuesReleasesLocksAndRedispatches() {
        WorkerFailureHandler handler = new WorkerFailureHandler(
                orchestrationService, taskDispatchService, distributedLock, new LockConfiguration.LockProperties());
        when(orchestrationService.requeueTasksOfWorker("worker-1")).thenReturn(List.of(
                task("exec-1", "a"), task("exec-1", "b"), task("exec-2", "a")));
        when(distributedLock.releaseIfOwnedBy(anyString(), anyString())).thenReturn(true);

        int requeued = handler.handleWorkerLost("worker-1");

        assertThat(requeued).isEqualTo(3);
        verify(distributedLock).releaseIfOwnedBy("chronos:lock:task:exec-1:a", "worker-1:");
        verify(distributedLock).releaseIfOwnedBy("chronos:lock:task:exec-1:b", "worker-1:");
        verify(distributedLock).releaseIfOwnedBy("chronos:lock:task:exec-2:a", "worker-1:");
        verify(taskDispatchService).dispatchReadyTasks("exec-1");
        verify(taskDispatchService).dispatchReadyTasks("exec-2");
    }

    @Test
    void nothingToDoWhenWorkerHadNoRunningTasks() {
        WorkerFailureHandler handler = new WorkerFailureHandler(
                orchestrationService, taskDispatchService, distributedLock, new LockConfiguration.LockProperties());
        when(orchestrationService.requeueTasksOfWorker("worker-1")).thenReturn(List.of());

        assertThat(handler.handleWorkerLost("worker-1")).isZero();
        verifyNoInteractions(distributedLock, taskDispatchService);
    }

    private static TaskExecution task(String executionId, String taskId) {
        return TaskExecution.builder().executionId(executionId).taskId(taskId)
                .status(ExecutionStatus.PENDING).attemptNumber(1).build();
    }
}
