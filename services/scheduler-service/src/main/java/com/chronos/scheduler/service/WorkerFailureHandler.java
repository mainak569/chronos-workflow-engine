package com.chronos.scheduler.service;

import com.chronos.scheduler.config.LockConfiguration;
import com.chronos.scheduler.domain.TaskExecution;
import com.chronos.scheduler.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Recovers the work of a worker that left the cluster (heartbeat expired or shutdown event).
 *
 * 1. Requeues tasks that were RUNNING on the worker
 * 2. Releases the worker's task locks, so another worker can claim the tasks right away
 *    instead of waiting for the lock TTL (only locks still held by that worker are removed)
 * 3. Dispatches the requeued tasks again
 */
@Service
public class WorkerFailureHandler {

    private static final Logger log = LoggerFactory.getLogger(WorkerFailureHandler.class);

    private final ExecutionOrchestrationService orchestrationService;
    private final TaskDispatchService taskDispatchService;
    private final DistributedLock distributedLock;
    private final LockConfiguration.LockProperties lockProperties;

    public WorkerFailureHandler(ExecutionOrchestrationService orchestrationService,
                                TaskDispatchService taskDispatchService,
                                DistributedLock distributedLock,
                                LockConfiguration.LockProperties lockProperties) {
        this.orchestrationService = orchestrationService;
        this.taskDispatchService = taskDispatchService;
        this.distributedLock = distributedLock;
        this.lockProperties = lockProperties;
    }

    /**
     * @return number of tasks requeued
     */
    public int handleWorkerLost(String workerId) {
        List<TaskExecution> requeued = orchestrationService.requeueTasksOfWorker(workerId);
        if (requeued.isEmpty()) {
            return 0;
        }

        Set<String> executionIds = new LinkedHashSet<>();
        for (TaskExecution task : requeued) {
            executionIds.add(task.getExecutionId());
            // Workers lock "{executionId}:{taskId}" with tokens of the form "{workerId}:{uuid}"
            String lockKey = lockProperties.buildTaskLockKey(task.getExecutionId() + ":" + task.getTaskId());
            if (distributedLock.releaseIfOwnedBy(lockKey, workerId + ":")) {
                log.info("Released task lock of lost worker: workerId={}, lockKey={}", workerId, lockKey);
            }
        }

        executionIds.forEach(taskDispatchService::dispatchReadyTasks);

        log.warn("Recovered {} task(s) of lost worker {} in {} execution(s)",
                requeued.size(), workerId, executionIds.size());
        return requeued.size();
    }
}
