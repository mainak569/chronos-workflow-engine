# Distributed Locking for Task Claiming

## Overview

Chronos uses Redis-based distributed locking to ensure that only one worker can claim and execute a task at any given time. This prevents duplicate task execution when multiple workers receive the same `TaskReady` event.

## The Problem

In a distributed system with multiple worker instances:
1. Kafka may deliver the same `TaskReady` event to multiple consumers
2. Multiple workers may simultaneously attempt to execute the same task
3. Without coordination, the task could be executed multiple times

## The Solution

### Atomic Task Claiming

Workers use Redis `SET NX EX` (Set if Not eXists with EXpiration) to atomically claim a task:

```
Key: chronos:lock:task:{taskId}
Value: {workerId}:{uuid}
TTL: 5 minutes (configurable)
```

**The atomic operation guarantees:**
- Only ONE worker's `SET NX` will succeed
- All other workers' attempts will fail immediately
- The lock automatically expires after TTL

### Lock Token Format

Each lock attempt creates a unique token:
```
{workerId}:{uuid}
```

**Example:** `worker-123:550e8400-e29b-41d4-a716-446655440000`

**Why include a UUID?**
- Prevents a worker from accidentally releasing another worker's lock
- Handles edge cases where a worker tries to claim the same task twice
- Provides unique identity for every lock acquisition attempt

## Locking Algorithm

### 1. Task Claiming Flow

```
Worker receives TaskReadyEvent
    ↓
Check if task type is supported
    ↓
Attempt atomic claim: SET NX chronos:lock:task:{taskId} {workerId}:{uuid} EX 300
    ↓
    ├─ SUCCESS (returned true) ──→ Execute task
    │                               ↓
    │                           Complete task
    │                               ↓
    │                           Release lock (Lua script)
    │
    └─ FAILURE (returned false) ──→ Another worker claimed it
                                    ↓
                                Acknowledge Kafka message
                                (do not execute)
```

### 2. Safe Lock Release

Release uses a Lua script to ensure atomic check-and-delete:

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
```

**This prevents:**
- Worker A's lock expires
- Worker B claims the lock
- Worker A finishes late and tries to release
- **Lua script checks**: token doesn't match → release fails
- Worker B's lock remains intact ✓

### 3. Lock Extension

For long-running tasks, the lock can be extended:

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
else
    return 0
end
```

Only the token owner can extend the lock.

## Guarantees

### 🟢 What This System GUARANTEES

1. **At-most-once task claiming**: Only one worker can successfully claim a task
2. **Atomic lock acquisition**: Redis `SET NX` is atomic
3. **Safe lock release**: Only the lock owner can release it
4. **Automatic expiration**: Locks expire to prevent deadlock
5. **Protection against accidental release**: UUID in token prevents cross-worker interference

### 🟡 What This System DOES NOT Guarantee

1. **Exactly-once execution**: This is NOT provided
2. **Protection against clock skew**: Redis TTL depends on Redis server clock
3. **Protection against network partitions**: Worker may lose connectivity while holding lock
4. **Protection against worker crashes**: Lock will hold until TTL expires
5. **Protection against very slow tasks**: If task exceeds TTL, lock expires and another worker may claim it

## Execution Semantics

**This system provides AT-LEAST-ONCE execution semantics:**

- Normal case: Task executes exactly once
- Worker crashes: Task may not execute (at-least-once with retry)
- Lock expires during execution: Task may execute twice

**Why not exactly-once?**
- Exactly-once is extremely difficult in distributed systems
- Requires distributed transactions or complex 2PC protocols
- Trade-off: We prioritize availability over strict once-and-only-once

## Handling Edge Cases

### Case 1: Lock Expires During Task Execution

```
Worker A claims task-123 (lock TTL: 5 min)
    ↓
Task takes 7 minutes to execute
    ↓
After 5 minutes: Lock expires
    ↓
Worker B claims task-123 (new lock)
    ↓
Worker B starts executing (DUPLICATE EXECUTION)
    ↓
Worker A finishes, tries to release
    ↓
Release fails (token mismatch)
```

**Mitigation:**
1. Set TTL longer than expected task duration
2. Implement task execution timeouts
3. Use idempotent task handlers
4. Consider lock extension for long tasks

### Case 2: Worker Crashes While Holding Lock

```
Worker A claims task-123
    ↓
Worker A crashes (network issue, OOM, etc.)
    ↓
Lock remains in Redis
    ↓
After TTL expires: Lock auto-releases
    ↓
Scheduler may retry TaskReady event
    ↓
Worker B claims task-123
```

**Result:** Task eventually executes (at-least-once)

### Case 3: Redis Becomes Unavailable

```
Worker receives TaskReadyEvent
    ↓
Attempts to claim task
    ↓
Redis connection fails
    ↓
Claim returns false
    ↓
Worker acknowledges Kafka message (does not execute)
```

**Result:** Task not executed until Redis recovers

**Mitigation:**
- Redis cluster for high availability
- Health checks and circuit breakers
- Dead letter queue for failed claims

### Case 4: Network Partition

```
Worker A claims task-123
    ↓
Network partition separates Worker A from Redis
    ↓
Worker A continues executing (thinks it holds lock)
    ↓
After TTL: Lock expires in Redis
    ↓
Worker B claims task-123
    ↓
Both workers executing (SPLIT BRAIN)
```

**Mitigation:**
1. Idempotent task handlers (duplicate execution is safe)
2. Task state tracking in database
3. Optimistic locking on task state updates
4. Monitor for duplicate executions

## Implementation Details

### Lock Key Naming

```
chronos:lock:task:{taskId}
```

**Benefits:**
- Namespace isolation: `chronos:lock:*`
- Easy to identify lock purpose: `task:{taskId}`
- Supports cleanup queries: `KEYS chronos:lock:task:*`

### Lock TTL Configuration

Default: **5 minutes**

Configurable via:
```yaml
chronos:
  lock:
    task-lock-ttl: 5m
```

**Choosing TTL:**
- Too short: Locks expire during normal execution
- Too long: Crashed workers delay retry
- **Rule of thumb**: 2-3x expected task duration

### Token Generation

```java
LockToken token = LockToken.create(workerId);
// Creates: {workerId}:{UUID.randomUUID()}
```

**UUID v4 properties:**
- 128-bit random number
- Collision probability: negligible for our use case
- No coordination needed

## Idempotency

**The locking system prevents duplicate claims, but NOT duplicate execution.**

Task handlers MUST be idempotent:

```java
@Transactional
public void executeTask(Task task) {
    // Check if already completed
    if (taskRepository.isCompleted(task.getId())) {
        log.info("Task already completed: {}", task.getId());
        return;
    }
    
    // Execute with optimistic locking
    task.setStatus(TaskStatus.RUNNING);
    task.setVersion(task.getVersion() + 1);
    taskRepository.save(task);  // Will fail if version changed
    
    // Actual work
    processTask(task);
    
    // Mark complete
    task.setStatus(TaskStatus.COMPLETED);
    taskRepository.save(task);
}
```

## Monitoring

### Metrics to Track

1. **Lock acquisition success rate**
   - `task_lock_acquired_total` (counter)
   - `task_lock_failed_total` (counter)

2. **Lock contention**
   - `task_lock_conflicts_total` (counter)
   - Indicates multiple workers competing for same task

3. **Lock expiration while held**
   - `task_lock_expired_during_execution_total` (counter)
   - Warning sign: TTL too short or tasks too slow

4. **Lock release failures**
   - `task_lock_release_failed_total` (counter)
   - Could indicate expired locks or bugs

### Alerts

- **High lock contention**: May indicate duplicate events or hot tasks
- **Frequent lock expiration**: Increase TTL or optimize task execution
- **Release failures**: Investigate for bugs or timing issues

## Comparison with Alternatives

### vs. Kafka Consumer Groups

**Kafka guarantees:**
- Each partition assigned to one consumer
- BUT: Multiple consumers in group may process DIFFERENT messages simultaneously
- Does NOT prevent: Same message delivered to multiple consumers after rebalance

**Our locking:**
- Ensures only one worker per TASK (not message)
- Works across consumer groups
- Protects against all duplicate scenarios

### vs. Database Locks

**Database row locks:**
- 🟢 Strong consistency
- 🟢 ACID guarantees
- 🔴 Higher latency
- 🔴 Database load
- 🔴 Lock held during entire transaction

**Redis locks:**
- 🟢 Very low latency (<1ms)
- 🟢 Minimal overhead
- 🟢 TTL-based expiration
- 🟡 Eventual consistency
- 🟡 No transaction support

### vs. Zookeeper/Consul Locks

**Zookeeper:**
- 🟢 Strong consistency (linearizable)
- 🟢 Ephemeral nodes
- 🔴 More complex
- 🔴 Higher latency
- 🔴 Heavier infrastructure

**Redis:**
- 🟢 Simpler
- 🟢 Faster
- 🟢 Already in our stack
- 🟡 Weaker consistency model

## Testing

See `TaskLockServiceConcurrencyTest` for comprehensive concurrency tests:

1. **Multiple workers compete for same task** → Only one succeeds
2. **Lock release allows reclaim** → Second worker succeeds after first releases
3. **Wrong token cannot release** → Protection against accidents
4. **Lock expiration** → New worker can claim after TTL
5. **Multiple tasks** → Workers can claim different tasks simultaneously
6. **Lock extension** → Owner can extend, others cannot

## Best Practices

### 1. Set Appropriate TTL

```yaml
# For fast tasks (< 1 min)
chronos.lock.task-lock-ttl: 2m

# For slow tasks (< 10 min)
chronos.lock.task-lock-ttl: 15m

# For very slow tasks: Consider lock extension
```

### 2. Implement Idempotent Handlers

```java
// 🟢 Good: Idempotent
if (isAlreadyProcessed(task)) {
    return;
}
processTask(task);
markAsProcessed(task);

// 🔴 Bad: Not idempotent
counter.increment();
sendEmail(user);
```

### 3. Handle Lock Failures Gracefully

```java
Optional<LockToken> token = taskLockService.claimTask(taskId, workerId);
if (token.isEmpty()) {
    log.info("Task already claimed by another worker");
    // Acknowledge message, don't retry
    return;
}
```

### 4. Always Release in Finally Block

```java
Optional<LockToken> token = taskLockService.claimTask(taskId, workerId);
if (token.isPresent()) {
    try {
        executeTask(task);
    } finally {
        taskLockService.releaseTask(taskId, token.get());
    }
}
```

### 5. Monitor Lock Metrics

```java
if (lockAcquired) {
    metrics.counter("task_lock_acquired").increment();
} else {
    metrics.counter("task_lock_failed").increment();
    metrics.counter("task_lock_conflicts").increment();
}
```

## Limitations and Future Improvements

### Current Limitations

1. **Single Redis instance is SPOF**
   - Mitigation: Use Redis Sentinel or Cluster

2. **No fencing tokens**
   - Can't prevent zombie workers from completing tasks
   - Mitigation: Implement task versioning

3. **No lock renewal during execution**
   - Long tasks risk lock expiration
   - Mitigation: Implement heartbeat-based extension

4. **In-memory idempotency tracking**
   - Lost on worker restart
   - Mitigation: Store in Redis with TTL

### Future Improvements

1. **Redlock algorithm**
   - Use multiple Redis instances
   - Quorum-based locking
   - Better safety guarantees

2. **Fencing tokens**
   - Monotonic counter with each lock
   - Database rejects operations with old tokens
   - Prevents split-brain execution

3. **Automatic lock extension**
   - Background thread extends lock periodically
   - Stops extension when task completes
   - Prevents expiration during normal execution

4. **Distributed idempotency**
   - Store processed event IDs in Redis
   - TTL-based cleanup
   - Survives worker restarts

## References

- [Redis SET command](https://redis.io/commands/set)
- [Distributed locks with Redis](https://redis.io/topics/distlock)
- [Martin Kleppmann's analysis of Redlock](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- [How to do distributed locking (Redis docs)](https://redis.io/docs/manual/patterns/distributed-locks/)

## Conclusion

**This distributed locking implementation:**
- 🟢 Prevents duplicate task claims (atomic)
- 🟢 Provides safe lock release (Lua scripts)
- 🟢 Handles worker crashes (TTL expiration)
- 🟢 Simple and fast (Redis)
- 🟡 Does NOT guarantee exactly-once execution
- 🟡 Requires idempotent task handlers

**Use this when:**
- You need to prevent multiple workers from claiming the same task
- You can make your task handlers idempotent
- You're okay with at-least-once execution semantics

**Consider alternatives when:**
- You need exactly-once guarantees (use distributed transactions)
- You need linearizable consistency (use ZooKeeper)
- You need stronger safety in face of network partitions (use Raft/Paxos)
