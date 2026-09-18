# Failure Recovery and Delivery Guarantees

## Overview

Chronos provides production-grade fault-tolerant workflow orchestration with automatic recovery from worker failures, network issues, and transient errors. This document describes the failure semantics, recovery mechanisms, and delivery guarantees.

## Delivery Guarantees

### At-Least-Once Execution

**Chronos provides at-least-once execution semantics for tasks.**

This means:
- 🟢 Every task will be executed **at least once**
- 🟡 Tasks **may be executed multiple times** under failure conditions
- 🟢 Failed tasks will be **retried automatically** (up to maxRetries)
- 🟢 Tasks will eventually complete or move to **Dead Letter Queue (DLQ)**

**Why not exactly-once?**

Exactly-once execution is extremely difficult to achieve in distributed systems without significant complexity and performance tradeoffs. Instead, Chronos uses at-least-once with **idempotency requirements**:

1. **Task implementations must be idempotent** - executing the same task multiple times with the same input produces the same result
2. **Use external idempotency keys** - for non-idempotent operations (payments, emails), use unique request IDs
3. **Optimistic locking** - TaskExecution uses `@Version` to prevent lost updates from concurrent modifications

### Kafka Event Delivery

- **Kafka provides at-least-once delivery** - the same event may be delivered multiple times
- **EventIdempotencyService prevents duplicate processing** - tracks processed event IDs in Redis with 7-day TTL
- **Manual commit** - Kafka offsets are committed only after successful processing

## Failure Scenarios and Recovery

### 1. Worker Crash During Task Execution

**Scenario:**
```
1. Worker-1 claims TASK-100 and starts execution
2. Worker-1 process crashes (JVM crash, container killed, network partition)
3. Task is stuck - Worker-1 is not responding
```

**Recovery:**
1. **Worker heartbeat expires** (30 seconds without heartbeat)
   - WorkerMonitorService detects expired heartbeat (runs every 15s)
   - Worker marked as UNAVAILABLE
   
2. **Automatic recovery (scheduler leader):**
   - Tasks RUNNING on the lost worker are put back to PENDING (same attempt number)
   - Task locks still held by the lost worker are released (`WorkerFailureHandler`)
   - The scheduler dispatches the tasks again through the outbox

**Timeline:**
```
T+0s:    Worker crashes
T+30s:   Heartbeat expires
T+30-45s: WorkerMonitorService marks worker UNAVAILABLE, requeues its RUNNING tasks
          and releases their locks
T+31-46s: TaskReady re-dispatched via the outbox; another worker executes the task
```

**Guarantees:**
- 🟢 Task will be recovered within heartbeat TTL + monitor interval (~45s); if a TaskReady is lost, the stale-dispatch sweep re-dispatches it after the dispatch timeout (5 minutes)
- 🟡 Task may execute twice if worker crashed after completing but before releasing lock
- 🟢 No unsafe concurrent execution (locks prevent races)

### 2. Network Partition / Slow Worker

**Scenario:**
```
1. Worker-1 claims TASK-200
2. Network becomes slow (not completely partitioned)
3. Task execution takes 6 minutes (exceeds 5-minute lease)
```

**Recovery:**
1. Worker-1 keeps renewing its lease and lock while the task runs, so no other worker claims it
2. If Worker-1 stops heartbeating, the scheduler requeues the task and Worker-2 may run it
3. **Both workers may then be executing simultaneously**; the scheduler accepts the first result
   for the attempt and ignores the duplicate

**Mitigation:**
- **Task implementations must be idempotent** - duplicate execution is safe
- **Use external coordination** - for critical operations, use external locks or idempotency tokens
- **Extend lease for long tasks** - TaskLeaseService.extendLease() adds time for known long-running tasks

### 3. Transient Failures (Network, Service Unavailable)

**Scenario:**
```
1. Task calls external API
2. API returns 503 Service Unavailable (transient error)
3. Task fails with retriable error
```

**Recovery:**
1. **Error classification:**
   - `SocketTimeoutException`, `ConnectException`, `IOException` → retriable
   - `IllegalArgumentException`, `SecurityException` → non-retriable
   
2. **Retry with exponential backoff (owned by the scheduler):**
   - The worker reports the failed attempt (attempt number + retriable flag)
   - The scheduler puts the task back to PENDING with `nextRetryAt = now + delay`,
     where delay = `initialDelayMs * backoffMultiplier^(attempt-1)`, capped at `maxDelayMs`
     (from the task's `retryConfig`: defaults 5s, ×2, 5 minutes)
   - The retry sweep dispatches the next attempt once `nextRetryAt` has passed
   - Results from older attempts are ignored

3. **After `retryConfig.maxAttempts` attempts:**
   - Task marked FAILED, the workflow execution fails, unstarted tasks are cancelled
   - TaskFailedPermanentlyEvent published to `chronos.task.failed.permanently` (DLQ)

**Configuration (scheduler defaults, overridden per task by retryConfig):**
```yaml
chronos:
  scheduler:
    retry:
      initial-delay: 5s
      backoff-multiplier: 2.0
      max-delay: 5m
```

### 4. Permanent Failures (Logic Errors)

**Scenario:**
```
1. Task has invalid input (NullPointerException)
2. Error is non-retriable
3. Task should not be retried
```

**Recovery:**
1. Task fails immediately
2. Error marked as non-retriable
3. Task moved directly to DLQ (no retries)
4. Workflow marked as FAILED (depending on policy)

### 5. Duplicate Kafka Events

**Scenario:**
```
1. TaskReadyEvent published to Kafka
2. Consumer processes event
3. Consumer crashes before committing offset
4. Event redelivered on restart
```

**Prevention:**
1. **EventIdempotencyService checks event ID:**
   ```java
   if (!idempotencyService.checkAndMark(event.getEventId())) {
       log.info("Event already processed (idempotent skip)");
       return;
   }
   ```

2. **Redis SET NX atomic operation:**
   - Key: `chronos:event:processed:{eventId}`
   - TTL: 7 days (longer than Kafka retention)
   - Only first worker succeeds in marking

3. **Duplicate events are safely ignored**

### 6. Optimistic Locking Conflicts

**Scenario:**
```
1. Worker-1 reads TaskExecution (version=1)
2. Worker-2 reads TaskExecution (version=1)
3. Worker-1 updates and saves (version=2)
4. Worker-2 tries to update (fails - stale version)
```

**Handling:**
```java
@Version
private Long version;  // Automatically managed by Spring Data
```

Spring Data MongoDB throws `OptimisticLockingFailureException` on conflict. The operation should be retried:

```java
try {
    taskExecutionRepository.save(execution);
} catch (OptimisticLockingFailureException e) {
    // Refresh and retry
    execution = taskExecutionRepository.findById(id).get();
    // Apply changes again
    taskExecutionRepository.save(execution);
}
```

## Recovery Components

### 1. Worker Heartbeat Mechanism

**Purpose:** Detect unresponsive workers

**Implementation:**
- `WorkerHeartbeatScheduler` sends heartbeat every **10 seconds**
- Heartbeat stored in Redis: `worker:heartbeat:{workerId}` with **30s TTL**
- `WorkerMonitorService` (leader scheduler) scans for expired heartbeats every **15 seconds** and hands lost
  workers to `WorkerFailureHandler` (requeue running tasks, release their locks, re-dispatch)

**Key:** `worker:heartbeat:{workerId}`  
**Value:** ISO-8601 timestamp  
**TTL:** 30 seconds

### 2. Distributed Locking

**Purpose:** Prevent concurrent task execution

**Implementation:**
- `TaskLockService` uses Redis SET NX EX
- Lock token format: `{workerId}:{UUID}`
- Atomic claim with TTL

**Key:** `chronos:lock:task:{executionId}:{taskId}` (task IDs are only unique within an execution)  
**Value:** `worker-001:a1b2c3d4-...`  
**TTL:** 5 minutes (default), renewed while the task runs

**Lua script for safe release:**
```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

### 3. Task Execution Leases

**Purpose:** Record which worker is executing which attempt (for diagnostics)

**Implementation:**
- `TaskLeaseService` stores execution metadata; `TaskLeaseRenewalService` extends leases (and locks)
  of running tasks every 2 minutes
- Includes: task key, executionId, workerId, lockToken, attemptNumber, expiresAt
- Crash recovery does not depend on leases: it is driven by worker heartbeats (see above)

**Key:** `chronos:lease:task:{executionId}:{taskId}`  
**Value:** JSON TaskLease object  
**TTL:** 5 minutes (default)

**Difference from locks:**
- **Lock:** Atomic claim mechanism that prevents concurrent execution
- **Lease:** Execution tracking metadata

### 4. Scheduler Recovery Sweeps

**Purpose:** Keep executions moving when events are lost, workers disappear or the scheduler restarts

**Implementation (`RestartRecoveryService`, leader only):**
- **Retry sweep** (every 2s): dispatches retry attempts whose `nextRetryAt` has passed
- **Recovery sweep** (every 60s, first run 15s after startup):
  1. Releases dispatch claims of attempts that no worker started within the dispatch timeout (5 minutes)
  2. Dispatches ready tasks of all PENDING/RUNNING executions
- **Worker loss:** `WorkerMonitorService` (heartbeat expiry) and `WorkerUnavailableEvent` requeue tasks RUNNING on that worker

Each task attempt is claimed with an atomic conditional update before its TaskReady event is
written to the outbox, so the sweeps and event consumers never dispatch the same attempt twice.

**Configuration:**
```yaml
chronos:
  scheduler:
    restart-recovery:
      enabled: true
      dispatch-timeout: 5m
      retry-sweep-interval: 2000   # ms
      sweep-interval: 60000        # ms
      initial-delay: 15000         # ms
```

### 5. Event Idempotency Service

**Purpose:** Prevent duplicate event processing

**Implementation:**
- Tracks processed event IDs in Redis
- SET NX with TTL (atomic check-and-set)
- TTL: **7 days** (must exceed Kafka retention)

**Key:** `chronos:event:processed:{eventId}`  
**Value:** "1"  
**TTL:** 7 days

### 6. Dead Letter Queue (DLQ)

**Purpose:** Store permanently failed tasks for manual investigation

**Topic:** `chronos.task.failed.permanently`

**Event structure:**
```json
{
  "eventId": "uuid",
  "taskId": "task-123",
  "executionId": "exec-456",
  "totalAttempts": 3,
  "maxAttempts": 3,
  "lastErrorMessage": "Connection refused",
  "lastErrorType": "ConnectException",
  "configuration": { ... },
  "lastAttemptAt": "2026-09-15T19:22:00Z"
}
```

**DLQ consumers can:**
- Alert operators
- Log to external monitoring
- Store in long-term storage
- Trigger manual intervention workflows

## Configuration Reference

The defaults below come from the services' `application.yml` files (the full list is in
[README.md](README.md#configuration-defaults)).

```yaml
# Worker service
worker:
  heartbeat.interval: 10000             # ms; the heartbeat key expires after 30s
  supported-task-types: IMAGE_RESIZE,IMAGE_COMPRESS,DATA_PROCESSING,DATA_VALIDATION
chronos:
  lock.task-lock-ttl: 5m                # per {executionId}:{taskId}
  task:
    lease-duration: 5m
    lease-renewal-interval: 120000      # ms; locks and leases of running tasks are extended
  event.idempotency-ttl: 7d

# Scheduler service
chronos:
  scheduler:
    retry:                              # used when a task has no retryConfig
      initial-delay: 5s
      backoff-multiplier: 2.0
      max-delay: 5m
    restart-recovery:
      dispatch-timeout: 5m              # re-dispatch attempts no worker picked up
      retry-sweep-interval: 2000        # ms
      sweep-interval: 60000             # ms
worker.monitor.interval: 15000          # ms; heartbeat expiry check (leader only)
```

Per-task settings come from the workflow definition: `retryConfig` (maxAttempts, initialDelayMs,
backoffMultiplier, maxDelayMs) and `timeoutMs`.

## Monitoring and Observability

The metrics, dashboard and alerts are described in [observability-architecture.md](observability-architecture.md).
The ones most relevant to failure handling:

| Signal | Metric / alert |
|---|---|
| Workers alive | `count(up{job="worker-service"} == 1)`, alert `NoWorkersAvailable` |
| Scheduler leadership | `chronos_scheduler_leader`, alert `NoSchedulerLeader` |
| Retries and requeues | `chronos_tasks_total{outcome="retried"}`, `{outcome="requeued"}` |
| Permanent failures | `chronos_tasks_total{outcome="failed"}`, `chronos_task_dlq_total`, alert `TasksDeadLettered` |
| Undelivered dispatches | `chronos_outbox_pending`, alert `OutboxBacklog` |
| Execution failure rate | `chronos_workflow_executions_total{status="FAILED"}`, alert `HighWorkflowFailureRate` |

Recovery actions are logged by the scheduler (`Marking worker as unavailable`, `Released task lock of lost
worker`, `Recovered N task(s)`, `Released stale dispatch`).

## Best Practices

### 1. Design Idempotent Tasks

**Bad (not idempotent):**
```java
public void processOrder(Order order) {
    payment.charge(order.amount);        // 🔴 May charge twice
    email.send(order.customer.email);    // 🔴 May send duplicate emails
    inventory.decrement(order.item);     // 🔴 May decrement twice
}
```

**Good (idempotent):**
```java
public void processOrder(Order order) {
    String idempotencyKey = order.id;
    
    // Use external idempotency
    if (!payment.isCharged(idempotencyKey)) {
        payment.chargeWithKey(order.amount, idempotencyKey);
    }
    
    // Check before acting
    if (!email.wasSent(order.id)) {
        email.sendWithKey(order.customer.email, order.id);
    }
    
    // Use database constraints
    inventory.decrementIfAvailable(order.item, order.id);
}
```

### 2. Set Appropriate Timeouts

Give every task a `timeoutMs` a little above its normal duration: an attempt that exceeds it is aborted
and retried (timeouts count as retriable failures). Locks and leases of running tasks are renewed
automatically, so long tasks are not reassigned while their worker is alive.

### 3. Handle Non-Retriable Errors

**Fail fast for logic errors:**
```java
public void executeTask(TaskReadyEvent event) {
    // Validate early
    if (event.getConfiguration() == null) {
        throw new IllegalArgumentException("Configuration required");  // Non-retriable
    }
    
    try {
        externalService.call();
    } catch (TimeoutException e) {
        throw e;  // Retriable
    } catch (ValidationException e) {
        throw new IllegalArgumentException("Invalid input", e);  // Non-retriable
    }
}
```

### 4. Monitor DLQ

The `TasksDeadLettered` alert fires when any task is dead-lettered within 15 minutes. Inspect the events:

```bash
docker exec chronos-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic chronos.task.failed.permanently --from-beginning
```

### 5. Test Failure Scenarios

**Integration tests should cover:**
- Worker crash mid-execution
- Task timeout
- Retry exhaustion
- Duplicate events
- Optimistic locking conflicts

## Limitations and Tradeoffs

### 1. At-Least-Once vs Exactly-Once

**Limitation:** Tasks may execute multiple times

**Mitigation:** Design idempotent tasks

**Why:** Exactly-once would require:
- Two-phase commit across services (slow, complex)
- Perfect failure detection (impossible in async distributed systems)
- Significant performance overhead

### 2. False Positives in Failure Detection

**Limitation:** A worker that stops heartbeating (e.g. a long GC pause or a network partition) is treated
as dead; its tasks are requeued and may run twice.

**Mitigation:**
- The first result reported for an attempt wins; duplicates are ignored
- Heartbeat TTL (30s) is three times the heartbeat interval (10s)
- Tasks should be idempotent

**Why:** Distinguishing a slow worker from a crashed one is impossible in an asynchronous system.

### 3. Recovery Delay

**Limitation:** A crashed worker's tasks are reassigned after 30–45 seconds (heartbeat TTL plus the
15-second monitor interval). A lost `TaskReady` event is re-sent after the 5-minute dispatch timeout.

**Mitigation:** Lower `worker.monitor.interval`, the heartbeat TTL or `dispatch-timeout` for
latency-sensitive workloads.

**Why:** Shorter timeouts increase the risk of false positives (above).

### 4. Redis as Single Point of Failure

**Limitation:** Redis failure blocks all operations

**Mitigation:**
- Use Redis Sentinel or Redis Cluster
- Enable persistence (AOF + RDB)
- Monitor Redis health
- Consider graceful degradation (allow processing without locks)

**Why:** External coordinator required for distributed locking

## Appendix: State Machines

### Worker State Machine

```
REGISTERING → AVAILABLE → BUSY → AVAILABLE
                ↓           ↓
            UNAVAILABLE   STOPPED
```

States:
- **REGISTERING:** Initial state, sending registration
- **AVAILABLE:** Ready to accept tasks
- **BUSY:** Currently executing a task
- **UNAVAILABLE:** Heartbeat expired, not responding
- **STOPPED:** Graceful shutdown

### Task Execution State Machine

```
PENDING → RUNNING → COMPLETED
   ↓         ↓
   └────→ FAILED ──→ PENDING (retry) or DLQ
   ↓
CANCELLED
```

States:
- **PENDING:** Waiting for execution or retry
- **RUNNING:** Currently being executed
- **COMPLETED:** Executed successfully
- **FAILED:** Execution failed (may retry)
- **CANCELLED:** Manually cancelled

## Summary

Chronos provides **at-least-once execution** with comprehensive failure recovery:

🟢 **Automatic recovery** from worker crashes via heartbeat expiry (tasks requeued, locks released)  
🟢 **Retry with exponential backoff** for transient failures, and **timeouts** for hung attempts  
🟢 **Dead letter queue** for permanent failures  
🟢 **Event idempotency** prevents duplicate processing  
🟢 **Optimistic locking** prevents lost updates  
🟢 **Distributed locking** prevents concurrent execution  

🟡 **Tasks must be idempotent** to handle at-least-once semantics  
🟡 **Recovery delay** of 30–45 seconds for crashed workers  
🟡 **Redis dependency** for coordination and state  

This design provides production-grade fault tolerance while maintaining simplicity and acceptable performance.
