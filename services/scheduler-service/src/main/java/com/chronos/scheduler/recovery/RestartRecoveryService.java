package com.chronos.scheduler.recovery;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.domain.WorkflowExecution;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.repository.TaskExecutionRepository;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import com.chronos.scheduler.service.TaskDispatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps workflow executions moving when events are delayed, lost or the scheduler restarts.
 *
 * Runs on the leader only:
 * - Retry sweep (frequent): dispatches retry attempts whose backoff has elapsed.
 * - Recovery sweep (periodic, and shortly after startup):
 *   1. releases dispatch claims of attempts no worker picked up within the dispatch timeout
 *      (TaskReady lost, dropped by a worker, or the scheduler crashed mid-dispatch)
 *   2. dispatches ready tasks of all in-progress executions (e.g. a WorkflowCreated event was lost)
 *
 * Safety: dispatching is idempotent per task attempt, and workers de-duplicate with
 * per-task locks, so running the sweeps repeatedly or concurrently with the event consumers is safe.
 */
@Service
public class RestartRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryService.class);

    private static final int BATCH_SIZE = 500;

    private final WorkflowExecutionRepository workflowExecutionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final TaskDispatchService taskDispatchService;
    private final LeaderElectionService leaderElectionService;

    @Value("${chronos.scheduler.restart-recovery.enabled:true}")
    private boolean recoveryEnabled;

    @Value("${chronos.scheduler.restart-recovery.dispatch-timeout:5m}")
    private Duration dispatchTimeout;

    public RestartRecoveryService(
            WorkflowExecutionRepository workflowExecutionRepository,
            TaskExecutionRepository taskExecutionRepository,
            TaskDispatchService taskDispatchService,
            LeaderElectionService leaderElectionService) {
        this.workflowExecutionRepository = workflowExecutionRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.taskDispatchService = taskDispatchService;
        this.leaderElectionService = leaderElectionService;
    }

    /**
     * Dispatch retry attempts whose backoff has elapsed.
     */
    @Scheduled(fixedDelayString = "${chronos.scheduler.restart-recovery.retry-sweep-interval:2000}")
    public void dispatchDueRetries() {
        if (!recoveryEnabled || !leaderElectionService.isLeader()) {
            return;
        }
        try {
            List<TaskExecution> due = taskExecutionRepository.findRetriesDue(Instant.now(), PageRequest.of(0, BATCH_SIZE));
            Set<String> executionIds = new LinkedHashSet<>();
            due.forEach(task -> executionIds.add(task.getExecutionId()));
            for (String executionId : executionIds) {
                taskDispatchService.dispatchReadyTasks(executionId);
            }
        } catch (Exception e) {
            log.error("Error dispatching due retries", e);
        }
    }

    /**
     * Periodic recovery sweep; the first run happens shortly after startup.
     */
    @Scheduled(
            initialDelayString = "${chronos.scheduler.restart-recovery.initial-delay:15000}",
            fixedDelayString = "${chronos.scheduler.restart-recovery.sweep-interval:60000}"
    )
    public void recoverScheduled() {
        if (!recoveryEnabled || !leaderElectionService.isLeader()) {
            return;
        }
        try {
            RecoveryStats stats = performRecovery();
            if (stats.getStaleDispatchesReleased() > 0 || stats.getTasksDispatched() > 0) {
                log.info("Recovery sweep completed: {}", stats);
            } else {
                log.debug("Recovery sweep completed: {}", stats);
            }
        } catch (Exception e) {
            log.error("Error during recovery sweep", e);
        }
    }

    /**
     * Perform a full recovery pass.
     */
    public RecoveryStats performRecovery() {
        int released = 0;
        Instant cutoff = Instant.now().minus(dispatchTimeout);
        for (TaskExecution task : taskExecutionRepository.findStaleDispatches(cutoff, PageRequest.of(0, BATCH_SIZE))) {
            if (taskDispatchService.releaseStaleDispatch(task)) {
                released++;
                log.warn("Released stale dispatch: executionId={}, taskId={}, attempt={}, dispatchedAt={}",
                        task.getExecutionId(), task.getTaskId(), task.getAttemptNumber(), task.getDispatchedAt());
            }
        }

        List<WorkflowExecution> inProgress = workflowExecutionRepository
                .findByStatusIn(List.of(ExecutionStatus.PENDING, ExecutionStatus.RUNNING));

        int dispatched = 0;
        for (WorkflowExecution execution : inProgress) {
            try {
                dispatched += taskDispatchService.dispatchReadyTasks(execution.getId());
            } catch (Exception e) {
                log.error("Failed to recover execution: executionId={}", execution.getId(), e);
            }
        }

        return new RecoveryStats(inProgress.size(), released, dispatched);
    }

    /**
     * Statistics for a recovery pass.
     */
    public static class RecoveryStats {
        private final int executionsProcessed;
        private final int staleDispatchesReleased;
        private final int tasksDispatched;

        public RecoveryStats(int executionsProcessed, int staleDispatchesReleased, int tasksDispatched) {
            this.executionsProcessed = executionsProcessed;
            this.staleDispatchesReleased = staleDispatchesReleased;
            this.tasksDispatched = tasksDispatched;
        }

        public int getExecutionsProcessed() {
            return executionsProcessed;
        }

        public int getStaleDispatchesReleased() {
            return staleDispatchesReleased;
        }

        public int getTasksDispatched() {
            return tasksDispatched;
        }

        @Override
        public String toString() {
            return String.format("RecoveryStats{executionsProcessed=%d, staleDispatchesReleased=%d, tasksDispatched=%d}",
                    executionsProcessed, staleDispatchesReleased, tasksDispatched);
        }
    }
}
