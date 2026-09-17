# Scheduler Service Architecture

## Overview

The Scheduler Service is responsible for orchestrating workflow executions in Chronos. It identifies scheduled workflows, creates workflow executions, determines runnable tasks, and publishes TaskReady events to Kafka for worker consumption.

**Key Features:**
- ✅ **Leader Election** - Only one scheduler instance actively schedules at a time
- ✅ **Persistent State** - All state stored in MongoDB/Redis, no local memory
- ✅ **Duplicate Prevention** - Idempotency ensures workflows aren't scheduled twice
- ✅ **Restart Recovery** - Resumes in-progress executions after crashes
- ✅ **Multiple Instances** - Supports running multiple schedulers for high availability

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                   Scheduler Instances                    │
├─────────────────────┬───────────────────────────────────┤
│   Scheduler-1       │        Scheduler-2                │
│   (LEADER)          │        (STANDBY)                  │
│                     │                                   │
│   ┌─────────────┐   │   ┌─────────────┐                │
│   │  Scheduling │   │   │  Monitoring │                │
│   │    Loop     │   │   │  Leadership │                │
│   │  (Active)   │   │   │  (Waiting)  │                │
│   └──────┬──────┘   │   └─────────────┘                │
│          │          │                                   │
└──────────┼──────────┴───────────────────────────────────┘
           │
           ↓
    ┌──────────────┐
    │    Redis     │  ← Leader Election
    │ chronos:     │     (SET NX EX with renewal)
    │ scheduler:   │
    │ leader       │
    └──────────────┘
           │
           ↓
    ┌──────────────┐
    │   MongoDB    │  ← Persistent State
    │ - Workflows  │     (SchedulerState collection)
    │ - Executions │
    │ - State      │
    └──────────────┘
           │
           ↓
    ┌──────────────┐
    │    Kafka     │  ← Event Publishing
    │ TaskReady    │     (Only by leader)
    │ Events       │
    └──────────────┘
```

## Components

### 1. LeaderElectionService

**Purpose:** Ensure only one scheduler instance actively schedules workflows at any time.

**Implementation:**
- Uses Redis SET NX EX for atomic leader election
- Leader holds lock with 30-second TTL
- Automatic renewal every 10 seconds
- Tracks leadership state in Redis (not JVM memory)
- Graceful leadership transfer on shutdown

**Redis Key:**
```
chronos:scheduler:leader → {schedulerId}:{timestamp}
TTL: 30 seconds
```

**Leader Election Algorithm:**
```java
1. Try to acquire leadership: SET NX chronos:scheduler:leader {myId} EX 30
2. If acquired:
   - Start scheduling loop
   - Renew leadership every 10 seconds
3. If not acquired:
   - Wait and retry
   - Monitor current leader
4. On shutdown:
   - Release leadership explicitly
```

**Guarantees:**
- At most one leader at a time
- Leadership automatically expires if instance crashes
- New leader elected within 30 seconds of crash
- No split-brain scenarios

### 2. SchedulerState (MongoDB)

**Purpose:** Track scheduled workflows and execution history persistently.

**Schema:**
```json
{
  "_id": "workflow-123",
  "workflowId": "workflow-123",
  "enabled": true,
  "schedule": "*/5 * * * *",  // Cron expression
  "lastExecutionTime": "2026-09-15T10:00:00Z",
  "lastExecutionId": "exec-456",
  "lastExecutionStatus": "COMPLETED",
  "nextScheduledTime": "2026-09-15T10:05:00Z",
  "executionCount": 42,
  "version": 3,  // Optimistic locking
  "createdAt": "2026-01-01T00:00:00Z",
  "updatedAt": "2026-09-15T10:00:05Z"
}
```

**Indexes:**
```
- workflowId: unique
- enabled: 1, nextScheduledTime: 1  (for scanning ready workflows)
- lastExecutionTime: 1  (for monitoring)
```

**Operations:**
- `findSchedulableWorkflows(now)` - Get workflows ready to execute
- `markExecuted(workflowId, executionId)` - Update after execution
- `calculateNextScheduleTime(cron, lastExecution)` - Compute next run

### 3. WorkflowScheduler

**Purpose:** Main scheduling loop that identifies and triggers workflow executions.

**Scheduling Loop:**
```java
@Scheduled(fixedDelay = 5000)  // Every 5 seconds
public void scheduleWorkflows() {
    // Step 1: Check if I am the leader
    if (!leaderElectionService.isLeader()) {
        log.debug("Not the leader, skipping scheduling");
        return;
    }
    
    // Step 2: Find workflows ready to execute
    List<SchedulerState> readyWorkflows = schedulerStateRepository
        .findByEnabledTrueAndNextScheduledTimeBefore(Instant.now());
    
    // Step 3: For each ready workflow
    for (SchedulerState state : readyWorkflows) {
        try {
            // Step 4: Create workflow execution (idempotent)
            WorkflowExecution execution = createExecution(state.getWorkflowId());
            
            // Step 5: Determine runnable tasks (no dependencies)
            List<TaskExecution> runnableTasks = findRunnableTasks(execution);
            
            // Step 6: Publish TaskReady events
            for (TaskExecution task : runnableTasks) {
                publishTaskReadyEvent(task);
            }
            
            // Step 7: Update scheduler state
            state.setLastExecutionTime(Instant.now());
            state.setLastExecutionId(execution.getId());
            state.setNextScheduledTime(calculateNextScheduleTime(state.getSchedule()));
            schedulerStateRepository.save(state);
            
        } catch (OptimisticLockingFailureException e) {
            // Another scheduler already processed this
            log.debug("Workflow {} already scheduled by another instance", state.getWorkflowId());
        } catch (Exception e) {
            log.error("Failed to schedule workflow {}", state.getWorkflowId(), e);
        }
    }
}
```

**Key Features:**
- Leader-only execution
- Optimistic locking prevents duplicates
- Idempotent execution creation
- Graceful error handling
- Per-workflow transaction boundaries

### 4. ExecutionIdempotencyService

**Purpose:** Prevent duplicate workflow executions from concurrent schedulers or retries.

**Implementation:**
- Uses Redis SET NX with TTL
- Key format: `chronos:execution:idempotency:{workflowId}:{timestamp}`
- TTL: 1 hour (longer than max execution time)

**Key Structure:**
```
chronos:execution:idempotency:workflow-123:2026-09-15T10:00 → exec-456
TTL: 1 hour
```

**Usage:**
```java
public String getOrCreateExecution(String workflowId, Instant scheduledTime) {
    String idempotencyKey = buildKey(workflowId, scheduledTime);
    
    // Try to get existing execution
    String existingExecution = redisTemplate.opsForValue().get(idempotencyKey);
    if (existingExecution != null) {
        return existingExecution;
    }
    
    // Create new execution
    WorkflowExecution execution = createWorkflowExecution(workflowId);
    
    // Store in Redis for idempotency
    redisTemplate.opsForValue().set(
        idempotencyKey,
        execution.getId(),
        Duration.ofHours(1)
    );
    
    return execution.getId();
}
```

### 5. RestartRecoveryService

**Purpose:** Resume interrupted workflow executions after scheduler restart or crash.

**Recovery Process:**
```java
@EventListener(ApplicationReadyEvent.class)
public void recoverOnStartup() {
    log.info("Starting restart recovery");
    
    // Step 1: Find in-progress executions
    List<WorkflowExecution> inProgress = executionRepository
        .findByStatusIn(RUNNING, PENDING);
    
    // Step 2: For each execution
    for (WorkflowExecution execution : inProgress) {
        try {
            // Step 3: Find tasks that should be running but aren't
            List<TaskExecution> pendingTasks = taskRepository
                .findByExecutionIdAndStatusIn(execution.getId(), PENDING, RUNNING);
            
            // Step 4: Determine which tasks are actually runnable
            List<TaskExecution> runnableTasks = filterRunnableTasks(
                execution, 
                pendingTasks
            );
            
            // Step 5: Republish TaskReady events
            for (TaskExecution task : runnableTasks) {
                if (shouldRepublish(task)) {
                    publishTaskReadyEvent(task);
                    log.info("Republished task {} after restart", task.getTaskId());
                }
            }
            
        } catch (Exception e) {
            log.error("Failed to recover execution {}", execution.getId(), e);
        }
    }
    
    log.info("Restart recovery completed");
}

private boolean shouldRepublish(TaskExecution task) {
    // Don't republish if task has active lease (worker still processing)
    TaskLease lease = taskLeaseService.getLease(task.getTaskId());
    if (lease != null && !lease.isExpired()) {
        return false;
    }
    
    // Don't republish if task recently started (within 1 minute)
    if (task.getStartedAt() != null && 
        Duration.between(task.getStartedAt(), Instant.now()).toMinutes() < 1) {
        return false;
    }
    
    return true;
}
```

**Guarantees:**
- In-progress workflows resume after restart
- Tasks are republished if not actively executing
- No duplicate task execution (lease checking)
- Graceful handling of partial executions

## Scheduling Flow

### Normal Execution Flow

```
1. Scheduler Loop Triggers (every 5 seconds)
   ↓
2. Leader Check: am I the leader?
   ↓ Yes
3. Find Ready Workflows (nextScheduledTime <= now)
   ↓
4. For Each Workflow:
   a. Check idempotency (already executed this slot?)
   b. Create WorkflowExecution (if not exists)
   c. Find runnable tasks (no unsatisfied dependencies)
   d. Publish TaskReady events to Kafka
   e. Update SchedulerState (lastExecutionTime, nextScheduledTime)
   ↓
5. Workers consume TaskReady events and execute
   ↓
6. Workers publish TaskCompleted/TaskFailed events
   ↓
7. ExecutionOrchestrationService updates state
   ↓
8. If all tasks complete, workflow marked COMPLETED
```

### Leader Election Flow

```
Scheduler-1 Starts:
T=0s:   Try to acquire leadership → SET NX chronos:scheduler:leader "sched-1" EX 30
T=0s:   Success! I am the leader
T=0s:   Start scheduling loop
T=10s:  Renew leadership → SET chronos:scheduler:leader "sched-1" EX 30
T=20s:  Renew leadership → SET chronos:scheduler:leader "sched-1" EX 30
...

Scheduler-1 Crashes:
T=100s: Scheduler-1 crashes, stops renewing
T=130s: Leadership lock expires (30s TTL)

Scheduler-2 Detects:
T=135s: Try to acquire leadership → SET NX chronos:scheduler:leader "sched-2" EX 30
T=135s: Success! I am the new leader
T=135s: Run restart recovery (check in-progress executions)
T=135s: Start scheduling loop
```

### Duplicate Prevention Flow

```
Scenario: Two schedulers run simultaneously (split-brain)

Scheduler-1 (thinks it's leader):
T=0s: Find workflow-A ready to execute
T=1s: Create execution-1 for workflow-A
T=2s: Try to set idempotency key → SET NX chronos:execution:idempotency:workflow-A:10:00 "exec-1"
T=2s: Success! Mark as scheduled
T=3s: Publish TaskReady events

Scheduler-2 (also thinks it's leader):
T=0.5s: Find workflow-A ready to execute
T=1.5s: Try to get idempotency key → GET chronos:execution:idempotency:workflow-A:10:00
T=1.5s: Found "exec-1" - execution already created
T=1.5s: Use existing execution-1 (don't create execution-2)
T=2.5s: Try to update SchedulerState with version=5
T=2.5s: OptimisticLockingFailureException (Scheduler-1 already updated to version=6)
T=2.5s: Skip this workflow (already scheduled)

Result: Only one execution created, no duplicate TaskReady events
```

## Failure Scenarios

### 1. Leader Crashes Mid-Schedule

**Scenario:**
```
T=0:   Scheduler-1 (leader) starts processing workflow-A
T=1:   Creates execution-1
T=2:   Publishes TaskReady for task-1
T=3:   Crashes before publishing TaskReady for task-2
```

**Recovery:**
```
T=33:  Scheduler-2 becomes leader (after 30s TTL)
T=34:  RestartRecoveryService scans in-progress executions
T=34:  Finds execution-1 with task-1 RUNNING, task-2 PENDING
T=35:  Checks if task-2 should be republished
T=35:  Republishes TaskReady for task-2
T=36:  Execution continues normally
```

**Outcome:** Workflow completes successfully, task-2 republished

### 2. Network Partition (Split-Brain)

**Scenario:**
```
T=0:   Scheduler-1 is leader
T=10:  Network partition: Scheduler-1 can't reach Redis
T=10:  Scheduler-1 thinks it's still leader (local state would be wrong)
T=30:  Leadership expires in Redis
T=31:  Scheduler-2 becomes leader
T=32:  Scheduler-1 tries to schedule → Redis operation fails
T=32:  Scheduler-1 detects it's not leader anymore
```

**Key:** Never use local `boolean isLeader` - always check Redis

### 3. Optimistic Locking Conflict

**Scenario:**
```
T=0:   Scheduler-1 reads SchedulerState (version=5)
T=0:   Scheduler-2 reads SchedulerState (version=5)
T=1:   Scheduler-1 saves SchedulerState (version=6)
T=2:   Scheduler-2 tries to save SchedulerState (version=5)
T=2:   OptimisticLockingFailureException thrown
T=2:   Scheduler-2 skips this workflow (already scheduled)
```

**Outcome:** Only one execution created, safe duplicate prevention

### 4. Scheduler Restart During Execution

**Scenario:**
```
T=0:   Execution-1 running: task-1 COMPLETED, task-2 RUNNING, task-3 PENDING
T=10:  Scheduler restarts
T=11:  RestartRecoveryService runs
T=11:  Checks task-2: has active lease, worker processing → don't republish
T=11:  Checks task-3: PENDING, no dependencies, no lease → republish TaskReady
T=12:  Worker-2 picks up task-3
T=15:  task-2 completes
T=20:  Execution-1 completes
```

**Outcome:** No duplicate execution, workflow resumes correctly

## Configuration

```yaml
chronos:
  scheduler:
    # Leader election
    leader-election:
      ttl: 30s                    # Leadership lock TTL
      renewal-interval: 10s       # How often to renew
      acquisition-retry: 5s       # Retry interval if not leader
      
    # Scheduling
    scheduling:
      interval: 5s                # How often to scan for ready workflows
      batch-size: 100             # Max workflows per scan
      
    # Idempotency
    execution-idempotency:
      ttl: 1h                     # Idempotency key TTL
      
    # Recovery
    restart-recovery:
      enabled: true
      task-republish-delay: 1m   # Don't republish if task started < 1min ago
      
spring:
  data:
    mongodb:
      database: chronos
      uri: mongodb://localhost:27017
      
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

## Redis Keys Reference

### Leader Election
```
chronos:scheduler:leader → {schedulerId}:{timestamp}
TTL: 30 seconds
Value: "scheduler-1:2026-09-15T10:00:00Z"
```

### Execution Idempotency
```
chronos:execution:idempotency:{workflowId}:{timeSlot} → {executionId}
TTL: 1 hour
Example: chronos:execution:idempotency:workflow-123:2026-09-15T10:00 → "exec-456"
```

### Scheduler Metrics
```
chronos:scheduler:metrics:{schedulerId}
TTL: 5 minutes
Value: JSON with last scheduling timestamp, executions created, etc.
```

## MongoDB Collections

### scheduler_state
```json
{
  "_id": "workflow-123",
  "workflowId": "workflow-123",
  "enabled": true,
  "schedule": "0 * * * *",
  "timezone": "UTC",
  "lastExecutionTime": "2026-09-15T10:00:00Z",
  "nextScheduledTime": "2026-09-15T11:00:00Z",
  "version": 5
}
```

### workflow_executions
```json
{
  "_id": "exec-456",
  "workflowId": "workflow-123",
  "status": "RUNNING",
  "createdAt": "2026-09-15T10:00:00Z",
  "scheduledBy": "scheduler-1"
}
```

## Monitoring & Metrics

### Key Metrics

**Leadership:**
- `scheduler.leadership.active` (gauge: 0 or 1)
- `scheduler.leadership.transitions` (counter)
- `scheduler.leadership.renewal.failures` (counter)

**Scheduling:**
- `scheduler.workflows.scanned` (counter)
- `scheduler.workflows.scheduled` (counter)
- `scheduler.workflows.skipped` (counter with reason)
- `scheduler.executions.created` (counter)

**Recovery:**
- `scheduler.recovery.executions.resumed` (counter)
- `scheduler.recovery.tasks.republished` (counter)

**Errors:**
- `scheduler.errors.optimistic_locking` (counter)
- `scheduler.errors.kafka_publish` (counter)
- `scheduler.errors.database` (counter)

### Health Checks

```java
@Component
public class SchedulerHealthIndicator implements HealthIndicator {
    
    @Override
    public Health health() {
        boolean isLeader = leaderElectionService.isLeader();
        String leaderInfo = leaderElectionService.getCurrentLeader();
        
        return Health.up()
            .withDetail("isLeader", isLeader)
            .withDetail("currentLeader", leaderInfo)
            .withDetail("lastSchedulingRun", lastSchedulingTime)
            .withDetail("workflowsScheduled", workflowsScheduledCount)
            .build();
    }
}
```

## Best Practices

### ✅ DO

1. **Always check leadership before scheduling** - Never schedule if not leader
2. **Use optimistic locking** - Prevent duplicate scheduling
3. **Implement idempotency** - Same time slot should create same execution
4. **Store all state in database** - Never rely on local memory
5. **Renew leadership frequently** - Keep TTL short, renew often
6. **Handle OptimisticLockingFailureException gracefully** - Log and skip
7. **Monitor leadership transitions** - Alert if changing too frequently

### ❌ DON'T

1. **Don't use local boolean isLeader** - Always check Redis
2. **Don't schedule without idempotency** - Prevent duplicates
3. **Don't ignore restart recovery** - Resume in-progress executions
4. **Don't hardcode timeouts** - Make all timings configurable
5. **Don't log at INFO for every scan** - Use DEBUG for normal operations
6. **Don't forget transaction boundaries** - One workflow per transaction

## Testing Strategy

### Unit Tests
- LeaderElectionService: acquire, renew, release
- SchedulerState: CRUD operations, next schedule calculation
- ExecutionIdempotencyService: duplicate prevention
- WorkflowScheduler: runnable task determination

### Integration Tests
- Multiple scheduler instances
- Leader failover
- Duplicate prevention
- Restart recovery
- Optimistic locking conflicts

### Chaos Tests
- Random scheduler crashes
- Network partitions
- Redis temporary unavailability
- MongoDB connection loss

## Summary

The Chronos Scheduler Service provides:

✅ **Leader Election** - Only one active scheduler using Redis  
✅ **Persistent State** - All state in MongoDB/Redis, no local memory  
✅ **Duplicate Prevention** - Idempotency and optimistic locking  
✅ **Restart Recovery** - Resumes in-progress executions  
✅ **High Availability** - Multiple instances with automatic failover  
✅ **Safe Concurrency** - Handles split-brain scenarios  

The design ensures workflows are scheduled reliably even with multiple scheduler instances, crashes, and network issues.
