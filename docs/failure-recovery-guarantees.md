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
   
2. **Task lease expires** (5 minutes without activity)
   - TaskRecoveryService scans for expired leases (runs every 30s)
   - Detects TASK-100 lease expired
   
3. **Automatic recovery:**
   - Force-release distributed lock (allow new claims)
   - Delete expired lease (clean up metadata)
   - Republish TaskReadyEvent to Kafka
   - Available worker claims and executes task

**Timeline:**
```
T+0s:    Worker crashes
T+30s:   Heartbeat expires, worker marked UNAVAILABLE
T+300s:  Lease expires
T+330s:  Recovery service detects expired lease
T+330s:  Task reassigned via Kafka
T+331s:  New worker claims and executes task
```

**Guarantees:**
- 🟢 Task will be recovered within (lease TTL + scan interval) = ~5.5 minutes
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
1. Lease expires after 5 minutes
2. TaskRecoveryService republishes task
3. Worker-2 claims TASK-200
4. **Both workers may be executing simultaneously**

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
   
2. **Retry with exponential backoff:**
   - Attempt 1 fails → wait 5 seconds
   - Attempt 2 fails → wait 30 seconds (5 * 6^1)
   - Attempt 3 fails → wait 3 minutes (5 * 6^2 = 180s)
   - Attempt 4+ → capped at 5 minutes

3. **After maxRetries exhausted:**
   - Task moved to Dead Letter Queue (DLQ)
   - TaskFailedPermanentlyEvent published to `chronos.task.failed.permanently`

**Configuration:**
```yaml
chronos:
  retry:
    max-attempts: 3
    base-delay: 5s
    backoff-multiplier: 6.0
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
- `WorkerMonitorService` scans for expired heartbeats every **15 seconds**

**Key:** `worker:heartbeat:{workerId}`  
**Value:** ISO-8601 timestamp  
**TTL:** 30 seconds

### 2. Distributed Locking

**Purpose:** Prevent concurrent task execution

**Implementation:**
- `TaskLockService` uses Redis SET NX EX
- Lock token format: `{workerId}:{UUID}`
- Atomic claim with TTL

**Key:** `chronos:lock:task:{taskId}`  
**Value:** `worker-001:a1b2c3d4-...`  
**TTL:** 5 minutes (default)

**Lua script for safe release:**
```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

### 3. Task Execution Leases

**Purpose:** Track task ownership and enable recovery

**Implementation:**
- `TaskLeaseService` stores execution metadata
- Includes: taskId, executionId, workerId, lockToken, attemptNumber, expiresAt

**Key:** `chronos:lease:task:{taskId}`  
**Value:** JSON TaskLease object  
**TTL:** 5 minutes (default)

**Difference from locks:**
- **Lock:** Short-lived, atomic claim mechanism
- **Lease:** Longer-lived, execution tracking + recovery metadata

### 4. Task Recovery Service

**Purpose:** Detect and recover abandoned tasks

**Implementation:**
- `TaskRecoveryService` scans for expired leases
- Runs every **30 seconds** (configurable)
- Initial delay: **60 seconds** (give workers time to start)

**Recovery process:**
1. Find all expired leases: `taskLeaseService.findExpiredLeases()`
2. For each expired lease:
   - Force-release lock: `taskLockService.forceRelease(taskId)`
   - Delete lease: `taskLeaseService.forceReleaseLease(taskId)`
   - Republish event: `kafkaTemplate.send(TASK_READY, taskReadyEvent)`

**Configuration:**
```yaml
chronos:
  recovery:
    scan-interval: 30000      # 30 seconds
    initial-delay: 60000      # 60 seconds
    enabled: true
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

### Worker Service Configuration

```yaml
# Worker identification
worker:
  id: ${WORKER_ID:worker-001}
  supported-task-types: IMAGE_RESIZE,DATA_PROCESSING,EMAIL_SEND

# Heartbeat
chronos:
  heartbeat:
    interval: 10000          # 10 seconds
    ttl: 30000              # 30 seconds
    
  # Lock and lease
  lock:
    task-lock-ttl: 5m       # 5 minutes
    
  task:
    lease-duration: 5m      # 5 minutes
    
  # Retry policy
  retry:
    max-attempts: 3
    base-delay: 5s
    backoff-multiplier: 6.0
    max-delay: 5m
    
  # Recovery
  recovery:
    scan-interval: 30000    # 30 seconds
    initial-delay: 60000    # 60 seconds
    enabled: true
    
  # Event idempotency
  event:
    idempotency-ttl: 7d     # 7 days

# Kafka
spring:
  kafka:
    consumer:
      group-id: chronos-worker-group
      enable-auto-commit: false
      auto-offset-reset: earliest
```

## Monitoring and Observability

### Key Metrics to Monitor

1. **Worker Health:**
   - Active workers count
   - Heartbeat failure rate
   - Worker AVAILABLE vs BUSY ratio

2. **Task Execution:**
   - Tasks in progress (lease count)
   - Average task duration
   - Task success vs failure rate

3. **Recovery:**
   - Expired lease count
   - Recovery trigger rate
   - Time to recovery (lease expiry to reassignment)

4. **Retry:**
   - Retry attempt distribution (attempt 1, 2, 3)
   - Backoff delay effectiveness
   - Permanent failure rate

5. **DLQ:**
   - DLQ event rate
   - Top failure reasons
   - DLQ message age

### Logging

All components use structured logging with correlation IDs:

```
INFO  [worker-001] Task claimed: taskId=task-123, workerId=worker-001
INFO  [worker-001] Task completed: taskId=task-123, attempt=1, duration=1234ms
ERROR [worker-001] Task failed: taskId=task-123, attempt=1/3, retriable=true
WARN  [recovery]   Task recovery triggered: taskId=task-123, expiredWorker=worker-001
ERROR [recovery]   Task permanently failed: taskId=task-123, attempts=3
```

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

**Lease TTL should be 2-3x expected task duration:**
- Short tasks (< 1 min): 5-minute lease is safe
- Long tasks (5-10 min): extend lease or increase default
- Very long tasks (> 15 min): consider breaking into smaller tasks

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

**Set up alerts for DLQ activity:**
```yaml
alerts:
  - name: high-dlq-rate
    condition: rate(dlq_messages_total[5m]) > 10
    severity: warning
    
  - name: dlq-message-aging
    condition: max(dlq_message_age_seconds) > 3600
    severity: critical
```

### 5. Test Failure Scenarios

**Integration tests should cover:**
- Worker crash mid-execution
- Network timeout
- Lease expiration
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

### 2. Lease Expiry During Execution

**Limitation:** Lease may expire while worker is still processing

**Mitigation:**
- Set lease TTL 2-3x expected duration
- Extend lease for long-running tasks
- Design tasks to complete quickly

**Why:** Perfect lease renewal requires:
- Continuous communication with coordinator
- Differentiate between slow and crashed workers (hard)
- May still fail in network partition

### 3. Recovery Delay

**Limitation:** 5-6 minute delay before task reassignment

**Components:**
- Lease TTL: 5 minutes
- Recovery scan interval: 30 seconds
- Total: ~5.5 minutes

**Mitigation:**
- Reduce lease TTL for time-critical tasks
- Increase recovery scan frequency
- Monitor expired lease count

**Why:** Shorter TTL increases risk of premature reassignment

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

🟢 **Automatic recovery** from worker crashes via lease expiration  
🟢 **Retry with exponential backoff** for transient failures  
🟢 **Dead letter queue** for permanent failures  
🟢 **Event idempotency** prevents duplicate processing  
🟢 **Optimistic locking** prevents lost updates  
🟢 **Distributed locking** prevents concurrent execution  

🟡 **Tasks must be idempotent** to handle at-least-once semantics  
🟡 **Recovery delay** of ~5.5 minutes for crashed workers  
🟡 **Redis dependency** for coordination and state  

This design provides production-grade fault tolerance while maintaining simplicity and acceptable performance.
