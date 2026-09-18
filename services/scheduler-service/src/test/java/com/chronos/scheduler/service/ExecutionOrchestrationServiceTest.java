package com.chronos.scheduler.service;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.metrics.SchedulerMetrics;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.OptimisticLockingFailureException;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ExecutionOrchestrationService: applying (possibly duplicate or out-of-order)
 * task status events to task and workflow execution state.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ExecutionOrchestrationServiceTest {

    private static final String EXECUTION_ID = "exec-1";

    @Mock
    private WorkflowExecutionRepository executionRepository;

    @Mock
    private TaskExecutionRepository taskRepository;

    private ExecutionOrchestrationService service;
    
    private SimpleMeterRegistry meterRegistry;

    private WorkflowExecution execution;
    private Map<String, TaskExecution> tasks;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new ExecutionOrchestrationService(executionRepository, taskRepository,
                new SchedulerMetrics(meterRegistry));

        execution = WorkflowExecution.builder()
                .id(EXECUTION_ID)
                .workflowId("wf-1")
                .status(ExecutionStatus.PENDING)
                .build();
        tasks = new LinkedHashMap<>();
        addTask("a", List.of());
        addTask("b", List.of("a"));

        when(executionRepository.findById(EXECUTION_ID)).thenAnswer(inv -> Optional.of(execution));
        when(taskRepository.findByExecutionId(EXECUTION_ID)).thenAnswer(inv -> new ArrayList<>(tasks.values()));
        when(taskRepository.findByExecutionIdAndTaskId(eq(EXECUTION_ID), any()))
                .thenAnswer(inv -> Optional.ofNullable(tasks.get(inv.getArgument(1, String.class))));
        when(taskRepository.save(any(TaskExecution.class))).thenAnswer(inv -> inv.getArgument(0));
        when(executionRepository.save(any(WorkflowExecution.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void readyTasksRespectDependenciesAndDispatchState() {
        assertThat(service.getReadyTasks(EXECUTION_ID)).extracting(TaskExecution::getTaskId).containsExactly("a");

        tasks.get("a").setDispatchedAt(Instant.now());
        assertThat(service.getReadyTasks(EXECUTION_ID)).isEmpty();
    }

    @Test
    void completionBeforeStartedEventStartsAndCompletesTask() {
        service.markTaskAsCompleted(EXECUTION_ID, "a", "worker-1", 1, Map.of("x", 1));

        TaskExecution a = tasks.get("a");
        assertThat(a.getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(a.getWorkerId()).isEqualTo("worker-1");
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(service.getReadyTasks(EXECUTION_ID)).extracting(TaskExecution::getTaskId).containsExactly("b");
    }

    @Test
    void lateStartedEventAfterCompletionIsIgnored() {
        service.markTaskAsCompleted(EXECUTION_ID, "a", "worker-1", 1, Map.of());
        service.markTaskAsRunning(EXECUTION_ID, "a", "worker-1", 1);

        assertThat(tasks.get("a").getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
    }

    @Test
    void duplicateCompletionIsIgnored() {
        service.markTaskAsCompleted(EXECUTION_ID, "a", "worker-1", 1, Map.of());
        service.markTaskAsCompleted(EXECUTION_ID, "a", "worker-2", 1, Map.of());

        assertThat(tasks.get("a").getWorkerId()).isEqualTo("worker-1");
        verify(taskRepository, times(1)).save(tasks.get("a"));
    }

    @Test
    void allTasksCompletedCompletesExecutionWithOutputs() {
        service.markTaskAsCompleted(EXECUTION_ID, "a", "w", 1, Map.of("a", 1));
        service.markTaskAsCompleted(EXECUTION_ID, "b", "w", 1, Map.of("b", 2));

        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(execution.getOutput()).containsKeys("a", "b");
        
        // Metrics: one completed execution with a recorded duration, two completed tasks
        assertThat(meterRegistry.get("chronos.workflow.executions").tag("status", "COMPLETED").counter().count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.get("chronos.workflow.duration").timer().count()).isEqualTo(1);
        assertThat(meterRegistry.get("chronos.tasks").tag("outcome", "completed").counter().count())
                .isEqualTo(2.0);
    }

    @Test
    void retriableFailureSchedulesRetryWithBackoff() {
        TaskExecution a = tasks.get("a");
        a.setRetryInitialDelayMs(1000L);
        a.setRetryBackoffMultiplier(3.0);
        a.setDispatchedAt(Instant.now());

        service.markTaskAsRunning(EXECUTION_ID, "a", "w", 1);
        Instant before = Instant.now();
        service.markTaskAsFailed(EXECUTION_ID, "a", "w", 1, "boom", "IOException", true);

        assertThat(a.getStatus()).isEqualTo(ExecutionStatus.PENDING);
        assertThat(a.getAttemptNumber()).isEqualTo(2);
        assertThat(a.getDispatchedAt()).isNull();
        assertThat(a.getNextRetryAt()).isBetween(before.plusMillis(900), before.plusMillis(2000));
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.RUNNING);

        // Not dispatchable until the backoff elapsed
        assertThat(service.getReadyTasks(EXECUTION_ID)).isEmpty();
        a.setNextRetryAt(Instant.now().minusSeconds(1));
        assertThat(service.getReadyTasks(EXECUTION_ID)).extracting(TaskExecution::getTaskId).containsExactly("a");

        // Second failure backs off by initial * multiplier
        assertThat(service.calculateRetryDelay(a)).isEqualTo(Duration.ofMillis(3000));
    }

    @Test
    void eventsForOldAttemptsAreIgnored() {
        service.markTaskAsFailed(EXECUTION_ID, "a", "w", 1, "boom", "IOException", true);
        assertThat(tasks.get("a").getAttemptNumber()).isEqualTo(2);

        // A late completion of attempt 1 must not complete attempt 2
        service.markTaskAsCompleted(EXECUTION_ID, "a", "w", 1, Map.of());
        assertThat(tasks.get("a").getStatus()).isEqualTo(ExecutionStatus.PENDING);
    }

    @Test
    void exhaustedRetriesFailTaskAndExecution() {
        TaskExecution a = tasks.get("a");
        a.setMaxRetries(2);

        service.markTaskAsFailed(EXECUTION_ID, "a", "w", 1, "boom", "IOException", true);
        service.markTaskAsFailed(EXECUTION_ID, "a", "w", 2, "boom again", "IOException", true);

        assertThat(a.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(tasks.get("b").getStatus()).isEqualTo(ExecutionStatus.CANCELLED);
        
        assertThat(meterRegistry.get("chronos.tasks").tag("outcome", "retried").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.get("chronos.tasks").tag("outcome", "failed").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.get("chronos.workflow.executions").tag("status", "FAILED").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void nonRetriableFailureFailsImmediately() {
        service.markTaskAsFailed(EXECUTION_ID, "a", "w", 1, "bad input", "IllegalArgumentException", false);

        assertThat(tasks.get("a").getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.FAILED);
    }

    @Test
    void optimisticLockConflictsAreRetried() {
        // Each lookup returns freshly loaded state, as a real repository would after a conflict
        when(taskRepository.findByExecutionIdAndTaskId(EXECUTION_ID, "a")).thenAnswer(inv -> Optional.of(
                TaskExecution.builder().id("id-a").executionId(EXECUTION_ID).taskId("a")
                        .status(ExecutionStatus.PENDING).attemptNumber(1).maxRetries(3).retriable(true).build()));
        when(taskRepository.save(any(TaskExecution.class)))
                .thenThrow(new OptimisticLockingFailureException("conflict"))
                .thenAnswer(inv -> inv.getArgument(0));

        service.markTaskAsCompleted(EXECUTION_ID, "a", "w", 1, Map.of());

        verify(taskRepository, times(2)).findByExecutionIdAndTaskId(EXECUTION_ID, "a");
        verify(taskRepository, times(2)).save(argThat((TaskExecution t) -> t.getStatus() == ExecutionStatus.COMPLETED));
    }

    @Test
    void tasksOfLostWorkerAreRequeued() {
        TaskExecution a = tasks.get("a");
        a.setDispatchedAt(Instant.now());
        a.start("dead-worker");
        when(taskRepository.findByStatusAndWorkerId(ExecutionStatus.RUNNING, "dead-worker")).thenReturn(List.of(a));

        List<TaskExecution> requeued = service.requeueTasksOfWorker("dead-worker");

        assertThat(requeued).containsExactly(a);
        assertThat(a.getStatus()).isEqualTo(ExecutionStatus.PENDING);
        assertThat(a.getAttemptNumber()).isEqualTo(1);
        assertThat(a.getDispatchedAt()).isNull();
        assertThat(a.getWorkerId()).isNull();
    }

    private void addTask(String taskId, List<String> dependsOn) {
        tasks.put(taskId, TaskExecution.builder()
                .id("id-" + taskId)
                .executionId(EXECUTION_ID)
                .workflowId("wf-1")
                .taskId(taskId)
                .taskType("DATA_PROCESSING")
                .status(ExecutionStatus.PENDING)
                .dependsOn(new ArrayList<>(dependsOn))
                .attemptNumber(1)
                .maxRetries(3)
                .retriable(true)
                .build());
    }
}
