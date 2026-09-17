# Chronos Architecture

## 1. Final System Architecture

### 1.1 Overview

Chronos is a distributed workflow orchestration engine built with a minimalist service-oriented architecture. The system consists of **4 application services** and **5 infrastructure components**, designed to be realistic for a single developer while demonstrating genuine distributed-system concepts.

```
┌─────────────────────────────────────────────────────────────┐
│                         CLIENT                               │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  API Gateway    │
                    │  (Port 8080)    │
                    └────────┬────────┘
                             │
                             │ HTTP
                             ▼
                    ┌─────────────────┐
                    │ Workflow Service│
                    │  (Port 8081)    │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
         ┌─────────┐   ┌─────────┐   ┌─────────┐
         │ MongoDB │   │  Kafka  │   │  Redis  │
         │ (27017) │   │ (9092)  │   │ (6379)  │
         └─────────┘   └────┬────┘   └─────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
      ┌──────────┐   ┌──────────┐   ┌──────────┐
      │Scheduler │   │ Worker-1 │   │ Worker-N │
      │ (8082)   │   │ (8083+)  │   │ (8083+N) │
      └──────────┘   └──────────┘   └──────────┘
              │             │             │
              └─────────────┼─────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │  Prometheus   │
                    │    (9090)     │
                    └───────┬───────┘
                            │
                            ▼
                    ┌───────────────┐
                    │   Grafana     │
                    │    (3000)     │
                    └───────────────┘
```

### 1.2 Architecture Principles

1. **Minimize Services, Maximize Learning**: Four services (not 10+) to keep the system manageable
2. **Event-Driven Core**: Kafka as the backbone for asynchronous communication
3. **Durable State in MongoDB**: Single source of truth for workflow execution state
4. **Ephemeral State in Redis**: Distributed coordination, locks, and heartbeats
5. **Horizontal Worker Scaling**: The only service designed for multi-instance deployment
6. **No Over-Engineering**: Every component must solve a real distributed-system problem

---

## 2. Service Boundaries

### 2.1 API Gateway (Port 8080)

**Purpose**: Single entry point for all external HTTP requests

**Scope**:
- Client authentication (JWT validation)
- Request authorization
- Request routing to internal services
- API-level rate limiting
- Correlation ID injection
- Consistent error response formatting

**Anti-patterns to avoid**:
- Business logic
- Direct database access
- Workflow state management
- Kafka event production/consumption

**Technology**: Spring Boot + Spring Security + Spring Cloud Gateway (or RestTemplate/WebClient for routing)

---

### 2.2 Workflow Service (Port 8081)

**Purpose**: Owns workflow definitions and execution state

**Scope**:
- Create and validate workflow definitions
- Store workflow schemas in MongoDB
- Validate workflow DAG (detect cycles)
- Start workflow executions
- Create execution records
- Determine initial runnable tasks
- Publish `WorkflowStarted` events to Kafka
- Query workflow and execution status
- Update execution state on completion/failure

**Anti-patterns to avoid**:
- Task scheduling logic (that's Scheduler's job)
- Task execution (that's Worker's job)
- Worker health monitoring

**Data Ownership**:
- `workflows` collection
- `executions` collection

**Kafka Interaction**:
- **Producer**: `workflow-events` topic
  - `WorkflowStarted`
  - `WorkflowCompleted`
  - `WorkflowFailed`

---

### 2.3 Scheduler Service (Port 8082)

**Purpose**: Determine which tasks are ready to run and publish TaskReady events

**Scope**:
- Listen to Kafka events:
  - `WorkflowStarted` → identify initial tasks
  - `TaskCompleted` → identify newly satisfied dependencies
- Compute task dependencies
- Determine which tasks are in PENDING state with satisfied dependencies
- Transition tasks from PENDING → READY
- Publish `TaskReady` events to Kafka
- Coordinate across multiple scheduler instances using Redis leader election
- Handle scheduler restart gracefully (recover in-progress workflows)

**Anti-patterns to avoid**:
- Task execution (that's Worker's job)
- Modifying workflow definitions
- Creating new workflows

**Data Access**:
- Reads from `tasks` collection
- Updates task status (PENDING → READY)

**Kafka Interaction**:
- **Consumer**: `workflow-events`, `task-events`
- **Producer**: `task-events` topic
  - `TaskReady`

**Coordination**:
- Uses Redis for leader election
- Only the leader actively schedules tasks
- Lock key: `scheduler:leader:lock` with TTL and renewal

---

### 2.4 Worker Service (Port 8083+)

**Purpose**: Execute tasks and report results

**Scope**:
- Register worker on startup with unique worker ID
- Publish heartbeats to Redis every 10 seconds
- Consume `TaskReady` events from Kafka
- Attempt to claim task using Redis distributed lock
- Execute task logic (simulate: resize image, process data, etc.)
- Report task status:
  - `TaskStarted`
  - `TaskCompleted`
  - `TaskFailed`
- Handle graceful shutdown
- Support horizontal scaling (multiple instances)

**Anti-patterns to avoid**:
- Scheduling decisions
- Dependency resolution
- Workflow creation

**Data Access**:
- Updates `tasks` collection
- Updates `workers` collection

**Kafka Interaction**:
- **Consumer**: `task-events` topic (consumer group: `worker-group`)
  - `TaskReady`
- **Producer**: `task-events` topic
  - `TaskStarted`
  - `TaskCompleted`
  - `TaskFailed`

**Coordination**:
- Uses Redis for:
  - Task locking (`task:{taskId}:lock`)
  - Heartbeat (`worker:{workerId}:heartbeat` with 30s TTL)
  - Worker registration (`worker:{workerId}:metadata`)

---

## 3. Responsibilities Matrix

| Responsibility | API Gateway | Workflow Service | Scheduler Service | Worker Service |
|---|---|---|---|---|
| **Authentication** | Y | N | N | N |
| **Authorization** | Y | N | N | N |
| **Create Workflow** | N | Y | N | N |
| **Validate DAG** | N | Y | N | N |
| **Start Execution** | N | Y | N | N |
| **Determine Runnable Tasks** | N | N | Y | N |
| **Publish TaskReady** | N | N | Y | N |
| **Claim Task** | N | N | N | Y |
| **Execute Task** | N | N | N | Y |
| **Update Task Status** | N | N | N | Y |
| **Heartbeat** | N | N | N | Y |
| **Retry Logic** | N | N | N | Y |
| **Leader Election** | N | N | Y | N |
| **Metrics Exposure** | Y | Y | Y | Y |

---

## 4. MongoDB Collections and Indexes

### 4.1 Users Collection

```javascript
{
  _id: ObjectId("..."),
  email: "user@example.com",
  name: "John Doe",
  passwordHash: "$2a$10$...",  // BCrypt
  createdAt: ISODate("2026-01-01T10:00:00Z"),
  updatedAt: ISODate("2026-01-01T10:00:00Z")
}
```

**Indexes**:
```javascript
db.users.createIndex({ email: 1 }, { unique: true })
```

**Rationale**: Email is the primary lookup key for authentication.

---

### 4.2 Workflows Collection

```javascript
{
  _id: "workflow-123",
  ownerId: "user-456",
  name: "image-processing-pipeline",
  description: "Resize, compress, and upload images",
  tasks: [
    {
      taskId: "resize-image",
      taskType: "IMAGE_RESIZE",
      dependencies: [],
      configuration: { width: 1280, height: 720 },
      retryConfig: {
        maxAttempts: 3,
        initialDelayMs: 5000,
        backoffMultiplier: 2.0,
        maxDelayMs: 60000
      },
      timeoutMs: 300000
    },
    {
      taskId: "compress-image",
      taskType: "IMAGE_COMPRESS",
      dependencies: ["resize-image"],
      configuration: { quality: 85 },
      retryConfig: { maxAttempts: 3, initialDelayMs: 5000, backoffMultiplier: 2.0, maxDelayMs: 60000 },
      timeoutMs: 300000
    }
  ],
  createdAt: ISODate("2026-01-01T10:00:00Z"),
  updatedAt: ISODate("2026-01-01T10:00:00Z")
}
```

**Indexes**:
```javascript
db.workflows.createIndex({ ownerId: 1 })
db.workflows.createIndex({ name: 1, ownerId: 1 })
db.workflows.createIndex({ createdAt: -1 })
```

**Rationale**:
- `ownerId`: Fetch all workflows for a user
- `name + ownerId`: Lookup workflow by name for a user
- `createdAt`: List recent workflows

---

### 4.3 Executions Collection

```javascript
{
  _id: "execution-789",
  workflowId: "workflow-123",
  ownerId: "user-456",
  status: "RUNNING",  // CREATED, RUNNING, COMPLETED, FAILED
  startedAt: ISODate("2026-01-01T10:00:00Z"),
  completedAt: null,
  failedAt: null,
  correlationId: "req-abc-123",
  metadata: {}
}
```

**Indexes**:
```javascript
db.executions.createIndex({ workflowId: 1 })
db.executions.createIndex({ ownerId: 1, createdAt: -1 })
db.executions.createIndex({ status: 1 })
db.executions.createIndex({ correlationId: 1 })
```

**Rationale**:
- `workflowId`: Find all executions of a workflow
- `ownerId + createdAt`: List user's recent executions
- `status`: Find all in-progress executions (for recovery)
- `correlationId`: Trace requests across services

---

### 4.4 Tasks Collection

**Note**: This collection stores **task execution instances**, not task definitions. Task definitions live in the `workflows.tasks` array.

```javascript
{
  _id: "task-execution-001",
  workflowId: "workflow-123",
  executionId: "execution-789",
  taskId: "resize-image",  // References workflows.tasks[].taskId
  taskType: "IMAGE_RESIZE",
  status: "RUNNING",  // PENDING, READY, RUNNING, SUCCESS, FAILED, RETRYING, DEAD
  workerId: "worker-01",
  attempt: 1,
  maxAttempts: 3,
  
  claimedAt: ISODate("2026-01-01T10:00:10Z"),
  startedAt: ISODate("2026-01-01T10:00:11Z"),
  completedAt: null,
  
  nextRetryAt: null,
  lastError: null,
  
  configuration: { width: 1280, height: 720 },
  result: null,
  
  dependencies: [],
  dependenciesMet: true
}
```

**Indexes**:
```javascript
db.tasks.createIndex({ executionId: 1 })
db.tasks.createIndex({ executionId: 1, taskId: 1 }, { unique: true })
db.tasks.createIndex({ status: 1 })
db.tasks.createIndex({ workerId: 1 })
db.tasks.createIndex({ nextRetryAt: 1 }, { sparse: true })
db.tasks.createIndex({ status: 1, dependenciesMet: 1 })
```

**Rationale**:
- `executionId`: Fetch all tasks for an execution
- `executionId + taskId`: Unique task instance per execution
- `status`: Find tasks in specific states (PENDING, READY, etc.)
- `workerId`: Find tasks owned by a worker (for failure recovery)
- `nextRetryAt`: Scheduler can poll for tasks ready to retry
- `status + dependenciesMet`: Efficiently find PENDING tasks with satisfied dependencies

---

### 4.5 Workers Collection

```javascript
{
  _id: "worker-01",
  status: "AVAILABLE",  // AVAILABLE, BUSY, UNAVAILABLE
  supportedTaskTypes: ["IMAGE_RESIZE", "IMAGE_COMPRESS", "DATA_PROCESSING"],
  registeredAt: ISODate("2026-01-01T09:00:00Z"),
  lastHeartbeatAt: ISODate("2026-01-01T10:00:00Z"),
  metadata: {
    hostname: "worker-pod-1",
    version: "1.0.0"
  }
}
```

**Indexes**:
```javascript
db.workers.createIndex({ status: 1 })
db.workers.createIndex({ lastHeartbeatAt: -1 })
```

**Rationale**:
- `status`: Find available workers
- `lastHeartbeatAt`: Detect stale workers (though Redis is primary heartbeat mechanism)

---

## 5. Kafka Topics, Partitions, Keys, Producers and Consumers

### 5.1 Topic: `workflow-events`

**Purpose**: Workflow lifecycle events

**Partitions**: 3

**Message Key**: `workflowId`

**Rationale**: All events for the same workflow go to the same partition, preserving order for that workflow.

**Events**:
- `WorkflowStarted`
- `WorkflowCompleted`
- `WorkflowFailed`

**Producers**:
- Workflow Service

**Consumers**:
- Scheduler Service (consumer group: `scheduler-group`)

**Event Schema**:
```json
{
  "eventId": "evt-001",
  "eventType": "WorkflowStarted",
  "workflowId": "workflow-123",
  "executionId": "execution-789",
  "timestamp": "2026-01-01T10:00:00Z",
  "correlationId": "req-abc-123",
  "payload": {
    "ownerId": "user-456",
    "tasks": [...]
  }
}
```

---

### 5.2 Topic: `task-events`

**Purpose**: Task lifecycle events

**Partitions**: 6

**Message Key**: `executionId`

**Rationale**: All tasks within the same execution go to the same partition. This ensures ordered processing of tasks within a workflow execution while allowing different executions to be processed in parallel.

**Alternative Considered**: Key by `taskId` → Problem: tasks from different executions would be interleaved, making dependency tracking harder.

**Events**:
- `TaskReady`
- `TaskStarted`
- `TaskCompleted`
- `TaskFailed`

**Producers**:
- Scheduler Service: `TaskReady`
- Worker Service: `TaskStarted`, `TaskCompleted`, `TaskFailed`

**Consumers**:
- Scheduler Service (consumer group: `scheduler-group`): consumes `TaskCompleted`, `TaskFailed` to determine next ready tasks
- Worker Service (consumer group: `worker-group`): consumes `TaskReady` to claim and execute tasks

**Event Schema**:
```json
{
  "eventId": "evt-002",
  "eventType": "TaskReady",
  "workflowId": "workflow-123",
  "executionId": "execution-789",
  "taskId": "resize-image",
  "taskType": "IMAGE_RESIZE",
  "timestamp": "2026-01-01T10:00:05Z",
  "correlationId": "req-abc-123",
  "payload": {
    "configuration": { "width": 1280, "height": 720 },
    "attempt": 1,
    "maxAttempts": 3
  }
}
```

---

### 5.3 Topic: `worker-events`

**Purpose**: Worker lifecycle and health events

**Partitions**: 1

**Message Key**: `workerId`

**Events**:
- `WorkerRegistered`
- `WorkerDeregistered`
- `WorkerHeartbeat` (optional, mainly handled by Redis)

**Producers**:
- Worker Service

**Consumers**:
- None initially (future: monitoring service)

**Rationale**: Low volume, informational. Primarily for observability.

---

### 5.4 Topic: `dead-letter-queue`

**Purpose**: Tasks that exceeded retry limits

**Partitions**: 1

**Message Key**: `taskId`

**Events**:
- `TaskDead`

**Producers**:
- Worker Service (after final retry failure)

**Consumers**:
- None initially (future: alerting service, manual intervention UI)

**Event Schema**:
```json
{
  "eventId": "evt-dlq-001",
  "eventType": "TaskDead",
  "workflowId": "workflow-123",
  "executionId": "execution-789",
  "taskId": "resize-image",
  "taskType": "IMAGE_RESIZE",
  "timestamp": "2026-01-01T10:10:00Z",
  "correlationId": "req-abc-123",
  "payload": {
    "attempts": 3,
    "lastError": "OutOfMemoryError: Java heap space",
    "workerId": "worker-02",
    "failedAt": "2026-01-01T10:09:55Z"
  }
}
```

---

### 5.5 Kafka Ordering Guarantees

**What Kafka Guarantees**:
- Messages with the same key go to the same partition
- Messages within a partition are ordered
- Consumers in the same consumer group read from non-overlapping partitions

**What Kafka Does NOT Guarantee**:
- Global ordering across partitions
- Exactly-once delivery (configured for at-least-once)

**Chronos Strategy**:
- Use `executionId` as key for task events → all tasks in an execution are ordered
- Idempotent event handlers ensure duplicate events don't corrupt state

---

## 6. Redis Data Structures and TTL Strategy

### 6.1 Task Locks

**Purpose**: Prevent multiple workers from executing the same task

**Key Pattern**: `task:{executionId}:{taskId}:lock`

**Data Type**: String (stores worker ID)

**TTL**: 5 minutes (300 seconds)

**Operations**:
```java
// Acquire lock
SET task:execution-789:resize-image:lock worker-01 NX EX 300

// Release lock (only if owned)
Lua script:
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  else
    return 0
  end
```

**Rationale**: 
- 5 minutes is generous for most tasks
- If worker crashes, lock expires automatically
- Prevents indefinite task stalling

---

### 6.2 Worker Heartbeats

**Key Pattern**: `worker:{workerId}:heartbeat`

**Data Type**: String (stores timestamp)

**TTL**: 30 seconds

**Operations**:
```java
// Update heartbeat
SETEX worker:worker-01:heartbeat 30 "2026-01-01T10:00:00Z"

// Check if worker is alive
EXISTS worker:worker-01:heartbeat
```

**Background Job**: Scheduler service polls for expired workers every 15 seconds

**Rationale**:
- 30s TTL means worker must heartbeat every ~10s
- If 3 consecutive heartbeats fail, worker is considered dead

---

### 6.3 Worker Metadata

**Key Pattern**: `worker:{workerId}:metadata`

**Data Type**: Hash

**TTL**: None (persistent until worker deregisters)

**Fields**:
```
supportedTaskTypes: "IMAGE_RESIZE,IMAGE_COMPRESS"
status: "AVAILABLE"
registeredAt: "2026-01-01T09:00:00Z"
```

**Operations**:
```java
HSET worker:worker-01:metadata supportedTaskTypes "IMAGE_RESIZE,IMAGE_COMPRESS"
HGET worker:worker-01:metadata status
```

---

### 6.4 Scheduler Leader Lock

**Key Pattern**: `scheduler:leader:lock`

**Data Type**: String (stores scheduler instance ID)

**TTL**: 10 seconds

**Operations**:
```java
// Acquire leadership
SET scheduler:leader:lock scheduler-01 NX EX 10

// Renew leadership (every 5 seconds if still leader)
SET scheduler:leader:lock scheduler-01 XX EX 10
```

**Rationale**:
- Short TTL ensures fast failover (max 10s delay)
- Leader must renew every 5s
- If leader crashes, another instance can acquire leadership within 10s

---

### 6.5 Rate Limiting (Optional)

**Key Pattern**: `ratelimit:user:{userId}:{endpoint}:{window}`

**Data Type**: String (counter)

**TTL**: 60 seconds

**Operations**:
```java
INCR ratelimit:user:user-123:/api/workflows:2026-01-01-10:00
EXPIRE ratelimit:user:user-123:/api/workflows:2026-01-01-10:00 60
```

---

### 6.6 TTL Strategy Summary

| Key Type | TTL | Rationale |
|---|---|---|
| Task Lock | 5 minutes | Long enough for task execution, short enough to recover quickly |
| Worker Heartbeat | 30 seconds | Fast failure detection, worker updates every 10s |
| Scheduler Leader | 10 seconds | Fast failover, leader renews every 5s |
| Worker Metadata | None | Persistent until explicit deregistration |
| Rate Limit | 60 seconds | Sliding window for API rate limiting |

---

## 7. Workflow Execution Model

### 7.1 Execution Lifecycle

```
Client Request
      |
      v
POST /api/v1/workflows/{id}/execute
      |
      v
API Gateway (authenticate)
      |
      v
Workflow Service
      |
      +---> Validate workflow exists
      +---> Create execution record (status: CREATED)
      +---> Create task execution instances (status: PENDING)
      +---> Update execution status: RUNNING
      +---> Publish WorkflowStarted event
      |
      v
   Kafka: workflow-events
      |
      v
Scheduler Service (listens to WorkflowStarted)
      |
      +---> Find tasks with no dependencies
      +---> Update task status: READY
      +---> Publish TaskReady events
      |
      v
   Kafka: task-events
      |
      v
Worker Service (listens to TaskReady)
      |
      +---> Attempt to acquire lock
      +---> If success: claim task
      +---> Execute task
      +---> Publish TaskCompleted or TaskFailed
      |
      v
   Kafka: task-events
      |
      v
Scheduler Service (listens to TaskCompleted)
      |
      +---> Find dependent tasks
      +---> Check if dependencies satisfied
      +---> If yes: update status to READY, publish TaskReady
      |
      v
Repeat until all tasks complete
      |
      v
Workflow Service
      |
      +---> Update execution status: COMPLETED or FAILED
```

### 7.2 Task Dependency Resolution

Example workflow:
```
     A
    / \
   B   C
    \ /
     D
```

**Execution Flow**:
1. Workflow starts → Task A is READY (no dependencies)
2. Task A completes → Tasks B and C become READY (dependency satisfied)
3. Tasks B and C execute in parallel
4. Both B and C complete → Task D becomes READY (all dependencies satisfied)
5. Task D completes → Workflow completes

**Implementation**:
```java
// Pseudo-code in Scheduler
@KafkaListener(topics = "task-events")
void handleTaskCompleted(TaskCompletedEvent event) {
    // Find tasks that depend on this task
    List<Task> dependentTasks = taskRepository.findByExecutionIdAndDependenciesContaining(
        event.getExecutionId(),
        event.getTaskId()
    );
    
    for (Task task : dependentTasks) {
        // Check if ALL dependencies are satisfied
        boolean allDependenciesMet = checkAllDependenciesMet(task);
        
        if (allDependenciesMet && task.getStatus() == TaskStatus.PENDING) {
            task.setStatus(TaskStatus.READY);
            task.setDependenciesMet(true);
            taskRepository.save(task);
            
            kafkaProducer.send("task-events", TaskReadyEvent.from(task));
        }
    }
}
```

---

## 8. Task State Machine

```
         ┌─────────────┐
         │   PENDING   │ (created, waiting for dependencies)
         └──────┬──────┘
                │
                │ (dependencies satisfied)
                v
         ┌─────────────┐
         │    READY    │ (ready to be claimed)
         └──────┬──────┘
                │
                │ (worker claims task)
                v
         ┌─────────────┐
         │   RUNNING   │
         └──────┬──────┘
                │
        ┌───────┴───────┐
        │               │
        v               v
  ┌──────────┐    ┌──────────┐
  │ SUCCESS  │    │  FAILED  │
  └──────────┘    └─────┬────┘
                        │
                        │ (attempt < maxAttempts)
                        v
                  ┌──────────┐
                  │RETRYING  │
                  └─────┬────┘
                        │
                        │ (after backoff delay)
                        v
                  ┌──────────┐
                  │  READY   │ (back to ready, attempt++)
                  └──────────┘
                        
                  (attempt >= maxAttempts)
                        │
                        v
                  ┌──────────┐
                  │   DEAD   │ (moved to DLQ)
                  └──────────┘
```

### Valid State Transitions

| From | To | Condition |
|---|---|---|
| PENDING | READY | Dependencies satisfied |
| READY | RUNNING | Worker claimed task |
| RUNNING | SUCCESS | Task completed successfully |
| RUNNING | FAILED | Task failed |
| FAILED | RETRYING | attempt < maxAttempts |
| RETRYING | READY | After backoff delay |
| FAILED | DEAD | attempt >= maxAttempts |

### Invalid Transitions (Must be Rejected)

- PENDING → RUNNING (must go through READY)
- SUCCESS → FAILED (cannot fail after success)
- DEAD → RETRYING (dead is terminal)
- RUNNING → PENDING (cannot go backwards)

**Implementation**: Use state machine pattern with validation in domain layer.

---

## 9. Worker Lifecycle and Heartbeat Mechanism

### 9.1 Worker Startup

```java
@Component
public class WorkerLifecycleManager {
    
    @PostConstruct
    public void startup() {
        String workerId = generateWorkerId(); // e.g., worker-<hostname>-<uuid>
        
        // 1. Register in MongoDB
        Worker worker = new Worker();
        worker.setId(workerId);
        worker.setStatus(WorkerStatus.AVAILABLE);
        worker.setSupportedTaskTypes(Arrays.asList("IMAGE_RESIZE", "DATA_PROCESSING"));
        worker.setRegisteredAt(Instant.now());
        workerRepository.save(worker);
        
        // 2. Register in Redis
        redisTemplate.opsForHash().put("worker:" + workerId + ":metadata", 
            "supportedTaskTypes", "IMAGE_RESIZE,DATA_PROCESSING");
        redisTemplate.opsForHash().put("worker:" + workerId + ":metadata", 
            "status", "AVAILABLE");
        
        // 3. Start heartbeat thread
        startHeartbeat(workerId);
        
        // 4. Publish WorkerRegistered event
        kafkaProducer.send("worker-events", new WorkerRegisteredEvent(workerId));
        
        log.info("Worker {} registered successfully", workerId);
    }
}
```

### 9.2 Heartbeat Mechanism

```java
@Scheduled(fixedDelay = 10000) // Every 10 seconds
public void sendHeartbeat() {
    String workerId = getWorkerId();
    String timestamp = Instant.now().toString();
    
    // Update Redis heartbeat with 30s TTL
    redisTemplate.opsForValue().set(
        "worker:" + workerId + ":heartbeat",
        timestamp,
        30,
        TimeUnit.SECONDS
    );
    
    // Update MongoDB (optional, for historical record)
    workerRepository.updateLastHeartbeat(workerId, Instant.now());
    
    log.debug("Heartbeat sent for worker {}", workerId);
}
```

### 9.3 Worker Shutdown

```java
@PreDestroy
public void shutdown() {
    String workerId = getWorkerId();
    
    // 1. Mark as unavailable
    workerRepository.updateStatus(workerId, WorkerStatus.UNAVAILABLE);
    
    // 2. Stop accepting new tasks
    kafkaConsumer.pause();
    
    // 3. Wait for in-progress tasks to complete (graceful shutdown)
    awaitTaskCompletion(60, TimeUnit.SECONDS);
    
    // 4. Release any held locks
    releaseAllLocks(workerId);
    
    // 5. Remove from Redis
    redisTemplate.delete("worker:" + workerId + ":heartbeat");
    redisTemplate.delete("worker:" + workerId + ":metadata");
    
    // 6. Publish WorkerDeregistered event
    kafkaProducer.send("worker-events", new WorkerDeregisteredEvent(workerId));
    
    log.info("Worker {} shut down gracefully", workerId);
}
```

### 9.4 Heartbeat Monitoring (Scheduler Service)

```java
@Scheduled(fixedDelay = 15000) // Every 15 seconds
public void detectFailedWorkers() {
    List<Worker> workers = workerRepository.findByStatus(WorkerStatus.AVAILABLE);
    
    for (Worker worker : workers) {
        String heartbeatKey = "worker:" + worker.getId() + ":heartbeat";
        Boolean exists = redisTemplate.hasKey(heartbeatKey);
        
        if (!exists) {
            log.warn("Worker {} heartbeat expired, marking as UNAVAILABLE", worker.getId());
            
            // Mark worker as unavailable
            workerRepository.updateStatus(worker.getId(), WorkerStatus.UNAVAILABLE);
            
            // Recover tasks owned by this worker
            recoverTasksFromWorker(worker.getId());
        }
    }
}
```

---

## 10. Failure Recovery Strategy

### 10.1 Worker Crash Scenario

**Problem**: Worker-01 claims a task, starts execution, then crashes before completion.

**Detection**:
1. Worker-01 stops sending heartbeats
2. After 30 seconds, Redis key `worker:worker-01:heartbeat` expires
3. Scheduler's background job detects missing heartbeat
4. Marks worker as UNAVAILABLE

**Recovery**:
```java
void recoverTasksFromWorker(String workerId) {
    // Find all tasks claimed by this worker that are still RUNNING
    List<Task> runningTasks = taskRepository.findByWorkerIdAndStatus(
        workerId, 
        TaskStatus.RUNNING
    );
    
    for (Task task : runningTasks) {
        log.info("Recovering task {} from failed worker {}", task.getId(), workerId);
        
        // Check if lock is still held
        String lockKey = "task:" + task.getExecutionId() + ":" + task.getTaskId() + ":lock";
        String lockHolder = redisTemplate.opsForValue().get(lockKey);
        
        if (workerId.equals(lockHolder)) {
            // Force release lock
            redisTemplate.delete(lockKey);
        }
        
        // Reset task to READY status
        task.setStatus(TaskStatus.READY);
        task.setWorkerId(null);
        task.setClaimedAt(null);
        taskRepository.save(task);
        
        // Republish TaskReady event
        kafkaProducer.send("task-events", TaskReadyEvent.from(task));
    }
}
```

**Timeline**:
```
T=0s:   Worker-01 crashes
T=10s:  Last heartbeat expires
T=30s:  Redis key expires
T=45s:  Scheduler detects missing worker (next poll cycle)
T=46s:  Tasks recovered, TaskReady events published
T=47s:  Worker-02 claims task, continues execution
```

**Max Recovery Time**: ~45 seconds (worst case)

---

### 10.2 Kafka Temporary Failure

**Problem**: Kafka broker becomes unreachable

**Strategy**:
1. Spring Kafka auto-retries with exponential backoff
2. Services log errors but don't crash
3. MongoDB state remains consistent (source of truth)
4. When Kafka recovers, services reconnect automatically
5. Use Kafka's consumer offset mechanism to resume from last committed offset

**Configuration**:
```yaml
spring:
  kafka:
    producer:
      retries: 3
      acks: all
      properties:
        retry.backoff.ms: 1000
    consumer:
      enable-auto-commit: false
      properties:
        session.timeout.ms: 30000
```

**Trade-off**: During Kafka downtime, no new tasks can be scheduled. Workflow execution pauses but doesn't corrupt.

---

### 10.3 MongoDB Temporary Failure

**Problem**: MongoDB becomes unreachable

**Strategy**:
1. Services fail fast on write operations
2. Return HTTP 503 (Service Unavailable) to clients
3. Do NOT cache state in memory (would create inconsistency)
4. When MongoDB recovers, services resume normal operation
5. Use MongoDB replica sets in production (out of scope for local dev)

**Anti-pattern**: Do NOT implement in-memory fallback state. This would create split-brain scenarios.

---

### 10.4 Redis Failure

**Problem**: Redis becomes unreachable

**Impact**:
- Cannot acquire task locks → workers cannot safely claim tasks
- Cannot detect worker heartbeats → cannot detect failures
- Scheduler leader election fails

**Strategy**:
- **Workers**: Fail fast, do not execute tasks without acquiring lock
- **Scheduler**: Stop scheduling until Redis is available
- **System**: Enters safe degraded mode (no new work, but no corruption)

**Why NOT continue without Redis?**
```
Worker-1: Claims task (no lock check) ──┐
                                         ├──> BOTH EXECUTE SAME TASK (corruption)
Worker-2: Claims task (no lock check) ──┘
```

**Trade-off**: Availability is sacrificed to preserve correctness (CAP theorem: choosing CP over AP).

---

### 10.5 Duplicate Event Handling (Idempotency)

**Problem**: Kafka delivers the same TaskCompleted event twice

**Solution**: Idempotent state transitions

```java
@Transactional
void handleTaskCompleted(TaskCompletedEvent event) {
    Task task = taskRepository.findById(event.getTaskId())
        .orElseThrow();
    
    // Idempotent check: only transition from RUNNING to SUCCESS
    if (task.getStatus() != TaskStatus.RUNNING) {
        log.warn("Ignoring duplicate TaskCompleted event for task {} (current status: {})",
            task.getId(), task.getStatus());
        return;
    }
    
    task.setStatus(TaskStatus.SUCCESS);
    task.setCompletedAt(Instant.now());
    taskRepository.save(task);
    
    // Continue with dependency resolution...
}
```

**Key Principle**: State transitions are validated based on current state, not blindly applied.

---

## 11. Distributed Locking Strategy

### 11.1 Lock Acquisition

```java
public boolean tryAcquireTaskLock(String executionId, String taskId, String workerId) {
    String lockKey = "task:" + executionId + ":" + taskId + ":lock";
    
    // SET NX EX: Set if Not eXists with EXpiration
    Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
        lockKey,
        workerId,
        Duration.ofMinutes(5)
    );
    
    return Boolean.TRUE.equals(acquired);
}
```

**Atomicity**: `SET NX EX` is atomic in Redis (single command)

---

### 11.2 Lock Release

```java
public void releaseTaskLock(String executionId, String taskId, String workerId) {
    String lockKey = "task:" + executionId + ":" + taskId + ":lock";
    
    // Lua script ensures atomicity: only release if current worker owns the lock
    String luaScript = 
        "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
        "    return redis.call('DEL', KEYS[1]) " +
        "else " +
        "    return 0 " +
        "end";
    
    redisTemplate.execute(
        RedisScript.of(luaScript, Long.class),
        Collections.singletonList(lockKey),
        workerId
    );
}
```

**Why Lua Script?** Prevents this race condition:
```
Worker-1: GET lock → returns "worker-1" ─────┐
Worker-1 crashes                             │
Lock expires                                 │
Worker-2: SET lock "worker-2" ───────────────┤
Worker-1: DEL lock ←─────────────────────────┘  (deletes Worker-2's lock!)
```

---

### 11.3 Lock Expiration (Lease)

**Problem**: Worker acquires lock, then hangs (not crashed, just slow)

**Solution**: Lock expires after 5 minutes automatically

**Scenario**:
```
T=0:    Worker-1 acquires lock, starts task
T=2min: Task is still running (normal)
T=5min: Lock expires (TTL reached)
T=5min: Worker-2 can now acquire lock, starts task
T=6min: Worker-1 finishes, tries to save result
        → Fails because task is already SUCCESS (Worker-2 completed it)
```

**Handling in Worker**:
```java
void executeTask(Task task) {
    try {
        // Periodically check if we still own the lock
        result = longRunningOperation(task, () -> {
            if (!stillHoldLock(task)) {
                throw new LockLostException("Lock expired during execution");
            }
        });
        
        // Save result only if we still own the lock
        if (stillHoldLock(task)) {
            saveTaskResult(task, result);
        } else {
            log.warn("Lost lock ownership for task {}, discarding result", task.getId());
        }
    } catch (LockLostException e) {
        log.error("Lock lost during execution for task {}", task.getId());
        // Don't save result, another worker will retry
    }
}
```

---

### 11.4 Lock Expiration vs Worker Health

**Question**: Why have both lock expiration (5 min) and heartbeat (30s)?

**Answer**: Different failure modes

| Scenario | Heartbeat | Lock |
|---|---|---|
| Worker crashes | Expires in 30s | Expires in 5min |
| Worker hangs on single task | Still beating | Expires in 5min |
| Network partition | Expires in 30s | Expires in 5min |
| Worker slow but working | Still beating | Still valid |

**Heartbeat**: Detects worker-level failure
**Lock**: Protects individual task execution

---

## 12. Retry and Dead-Letter Strategy

### 12.1 Retry Configuration

```java
@Data
public class RetryConfig {
    private int maxAttempts = 3;
    private long initialDelayMs = 5000;      // 5 seconds
    private double backoffMultiplier = 2.0;
    private long maxDelayMs = 300000;        // 5 minutes
}
```

### 12.2 Exponential Backoff Calculation

```java
public long calculateNextRetryDelay(int attempt, RetryConfig config) {
    long delay = config.getInitialDelayMs() * 
                 (long) Math.pow(config.getBackoffMultiplier(), attempt - 1);
    
    return Math.min(delay, config.getMaxDelayMs());
}

// Example:
// Attempt 1: 5s
// Attempt 2: 10s
// Attempt 3: 20s
// Attempt 4: 40s
// ...
// Attempt 8: 300s (capped at maxDelayMs)
```

### 12.3 Retry Flow

```java
@Transactional
void handleTaskFailed(TaskFailedEvent event) {
    Task task = taskRepository.findById(event.getTaskId()).orElseThrow();
    
    task.setStatus(TaskStatus.FAILED);
    task.setLastError(event.getError());
    
    if (task.getAttempt() < task.getMaxAttempts()) {
        // Schedule retry
        task.setStatus(TaskStatus.RETRYING);
        
        long delayMs = calculateNextRetryDelay(task.getAttempt() + 1, task.getRetryConfig());
        Instant nextRetryAt = Instant.now().plusMillis(delayMs);
        task.setNextRetryAt(nextRetryAt);
        
        taskRepository.save(task);
        
        log.info("Task {} will retry in {}ms (attempt {}/{})",
            task.getId(), delayMs, task.getAttempt() + 1, task.getMaxAttempts());
        
        // Scheduler will poll for tasks with nextRetryAt <= now
    } else {
        // Send to dead-letter queue
        task.setStatus(TaskStatus.DEAD);
        taskRepository.save(task);
        
        kafkaProducer.send("dead-letter-queue", TaskDeadEvent.from(task, event.getError()));
        
        log.error("Task {} exceeded max retries, moved to DLQ", task.getId());
    }
}
```

### 12.4 Retry Polling (Scheduler)

```java
@Scheduled(fixedDelay = 5000) // Every 5 seconds
public void scheduleRetries() {
    Instant now = Instant.now();
    
    List<Task> tasksToRetry = taskRepository.findByStatusAndNextRetryAtBefore(
        TaskStatus.RETRYING,
        now
    );
    
    for (Task task : tasksToRetry) {
        task.setStatus(TaskStatus.READY);
        task.setAttempt(task.getAttempt() + 1);
        task.setNextRetryAt(null);
        taskRepository.save(task);
        
        kafkaProducer.send("task-events", TaskReadyEvent.from(task));
    }
}
```

---

## 13. API Design

### 13.1 Authentication APIs

**POST /api/v1/auth/register**

Request:
```json
{
  "email": "user@example.com",
  "name": "John Doe",
  "password": "SecurePass123!"
}
```

Response (201 Created):
```json
{
  "userId": "user-123",
  "email": "user@example.com",
  "name": "John Doe",
  "createdAt": "2026-01-01T10:00:00Z"
}
```

---

**POST /api/v1/auth/login**

Request:
```json
{
  "email": "user@example.com",
  "password": "SecurePass123!"
}
```

Response (200 OK):
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenType": "Bearer",
  "expiresIn": 3600
}
```

---

### 13.2 Workflow APIs

**POST /api/v1/workflows**

Headers:
```
Authorization: Bearer <token>
```

Request:
```json
{
  "name": "image-processing-pipeline",
  "description": "Resize, compress, and upload images",
  "tasks": [
    {
      "taskId": "resize",
      "taskType": "IMAGE_RESIZE",
      "dependencies": [],
      "configuration": {
        "width": 1280,
        "height": 720
      },
      "retryConfig": {
        "maxAttempts": 3,
        "initialDelayMs": 5000,
        "backoffMultiplier": 2.0,
        "maxDelayMs": 60000
      },
      "timeoutMs": 300000
    },
    {
      "taskId": "compress",
      "taskType": "IMAGE_COMPRESS",
      "dependencies": ["resize"],
      "configuration": {
        "quality": 85
      }
    }
  ]
}
```

Response (201 Created):
```json
{
  "workflowId": "workflow-123",
  "name": "image-processing-pipeline",
  "ownerId": "user-456",
  "createdAt": "2026-01-01T10:00:00Z"
}
```

---

**GET /api/v1/workflows/{workflowId}**

Response (200 OK):
```json
{
  "workflowId": "workflow-123",
  "name": "image-processing-pipeline",
  "description": "Resize, compress, and upload images",
  "ownerId": "user-456",
  "tasks": [...],
  "createdAt": "2026-01-01T10:00:00Z",
  "updatedAt": "2026-01-01T10:00:00Z"
}
```

---

**POST /api/v1/workflows/{workflowId}/execute**

Response (202 Accepted):
```json
{
  "executionId": "execution-789",
  "workflowId": "workflow-123",
  "status": "RUNNING",
  "startedAt": "2026-01-01T10:05:00Z"
}
```

---

### 13.3 Execution APIs

**GET /api/v1/executions/{executionId}**

Response (200 OK):
```json
{
  "executionId": "execution-789",
  "workflowId": "workflow-123",
  "status": "RUNNING",
  "startedAt": "2026-01-01T10:05:00Z",
  "completedAt": null,
  "correlationId": "req-abc-123"
}
```

---

**GET /api/v1/executions/{executionId}/tasks**

Response (200 OK):
```json
{
  "executionId": "execution-789",
  "tasks": [
    {
      "taskId": "resize",
      "status": "SUCCESS",
      "attempt": 1,
      "startedAt": "2026-01-01T10:05:10Z",
      "completedAt": "2026-01-01T10:05:15Z"
    },
    {
      "taskId": "compress",
      "status": "RUNNING",
      "attempt": 1,
      "startedAt": "2026-01-01T10:05:20Z",
      "completedAt": null
    }
  ]
}
```

---

## 14. Docker Compose Architecture

```yaml
version: '3.8'

services:
  # Infrastructure
  
  mongodb:
    image: mongo:7.0
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: chronos
      MONGO_INITDB_ROOT_PASSWORD: chronos123
    volumes:
      - mongodb_data:/data/db
    networks:
      - chronos-network

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - chronos-network

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - chronos-network

  redis:
    image: redis:7.2-alpine
    ports:
      - "6379:6379"
    networks:
      - chronos-network

  # Application Services
  
  api-gateway:
    build:
      context: ./services/api-gateway
    ports:
      - "8080:8080"
    environment:
      WORKFLOW_SERVICE_URL: http://workflow-service:8081
      JWT_SECRET: ${JWT_SECRET}
    depends_on:
      - workflow-service
    networks:
      - chronos-network

  workflow-service:
    build:
      context: ./services/workflow-service
    ports:
      - "8081:8081"
    environment:
      MONGODB_URI: mongodb://chronos:chronos123@mongodb:27017/chronos
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      - mongodb
      - kafka
    networks:
      - chronos-network

  scheduler-service:
    build:
      context: ./services/scheduler-service
    ports:
      - "8082:8082"
    environment:
      MONGODB_URI: mongodb://chronos:chronos123@mongodb:27017/chronos
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      REDIS_HOST: redis
      REDIS_PORT: 6379
    depends_on:
      - mongodb
      - kafka
      - redis
    networks:
      - chronos-network

  worker-service:
    build:
      context: ./services/worker-service
    ports:
      - "8083-8093:8083"  # Support up to 10 workers
    environment:
      MONGODB_URI: mongodb://chronos:chronos123@mongodb:27017/chronos
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      REDIS_HOST: redis
      REDIS_PORT: 6379
      WORKER_ID: ${HOSTNAME}
    depends_on:
      - mongodb
      - kafka
      - redis
    networks:
      - chronos-network
    deploy:
      replicas: 3  # Start with 3 workers

  # Observability
  
  prometheus:
    image: prom/prometheus:v2.47.0
    ports:
      - "9090:9090"
    volumes:
      - ./infrastructure/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
    networks:
      - chronos-network

  grafana:
    image: grafana/grafana:10.1.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./infrastructure/grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./infrastructure/grafana/datasources:/etc/grafana/provisioning/datasources
      - grafana_data:/var/lib/grafana
    depends_on:
      - prometheus
    networks:
      - chronos-network

networks:
  chronos-network:
    driver: bridge

volumes:
  mongodb_data:
  prometheus_data:
  grafana_data:
```

**Scaling Workers**:
```bash
docker compose up --scale worker-service=5
```

---

## 15. Testing Strategy

### 15.1 Unit Tests

**Target**: Business logic, state machines, validation

**Technologies**: JUnit 5, Mockito, AssertJ

**Examples**:

```java
@Test
void shouldRejectInvalidStateTransition() {
    Task task = new Task();
    task.setStatus(TaskStatus.SUCCESS);
    
    assertThatThrownBy(() -> task.transitionTo(TaskStatus.FAILED))
        .isInstanceOf(IllegalStateTransitionException.class)
        .hasMessage("Cannot transition from SUCCESS to FAILED");
}

@Test
void shouldCalculateExponentialBackoff() {
    RetryConfig config = new RetryConfig(3, 5000, 2.0, 300000);
    
    assertThat(retryService.calculateNextRetryDelay(1, config)).isEqualTo(5000);
    assertThat(retryService.calculateNextRetryDelay(2, config)).isEqualTo(10000);
    assertThat(retryService.calculateNextRetryDelay(3, config)).isEqualTo(20000);
}

@Test
void shouldDetectCyclicDependencies() {
    Workflow workflow = new Workflow();
    workflow.addTask(new Task("A", List.of("B")));
    workflow.addTask(new Task("B", List.of("C")));
    workflow.addTask(new Task("C", List.of("A")));
    
    assertThatThrownBy(() -> workflowValidator.validate(workflow))
        .isInstanceOf(CyclicDependencyException.class);
}
```

---

### 15.2 Integration Tests

**Target**: Service interaction with real infrastructure

**Technologies**: Spring Boot Test, Testcontainers, Awaitility

**Examples**:

```java
@SpringBootTest
@Testcontainers
class WorkflowExecutionIntegrationTest {
    
    @Container
    static MongoDBContainer mongodb = new MongoDBContainer("mongo:7.0");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );
    
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
        .withExposedPorts(6379);
    
    @Test
    void shouldExecuteSimpleWorkflow() {
        // Given
        Workflow workflow = createSimpleWorkflow();
        workflowService.createWorkflow(workflow);
        
        // When
        ExecutionResponse response = workflowService.executeWorkflow(workflow.getId());
        
        // Then
        await().atMost(30, SECONDS).until(() -> {
            Execution execution = executionRepository.findById(response.getExecutionId());
            return execution.getStatus() == ExecutionStatus.COMPLETED;
        });
    }
}
```

---

### 15.3 Failure Scenario Tests

```java
@Test
void shouldRecoverFromWorkerFailure() {
    // Start workflow
    String executionId = startWorkflow();
    
    // Wait for task to be claimed
    await().until(() -> taskIsClaimed(executionId));
    
    // Kill worker
    workerContainer.stop();
    
    // Verify task is recovered and reassigned
    await().atMost(60, SECONDS).until(() -> {
        Task task = getTask(executionId);
        return task.getStatus() == TaskStatus.SUCCESS;
    });
}

@Test
void shouldHandleDuplicateKafkaEvents() {
    // Publish TaskCompleted event twice
    kafkaProducer.send(taskCompletedEvent);
    kafkaProducer.send(taskCompletedEvent); // duplicate
    
    // Verify task is only marked complete once
    await().pollDelay(2, SECONDS).atMost(5, SECONDS).until(() -> {
        Task task = taskRepository.findById(taskId);
        return task.getStatus() == TaskStatus.SUCCESS;
    });
    
    // Verify no errors logged
    assertThat(getDuplicateEventWarnings()).hasSize(1);
}
```

---

### 15.4 Test Coverage Goals

| Component | Target Coverage |
|---|---|
| Domain models | 90%+ |
| State machines | 100% |
| Validators | 90%+ |
| Services | 80%+ |
| Controllers | 70%+ |
| Event handlers | 80%+ |

---

## Summary

This architecture balances **educational value** with **production-inspired design**:

- **4 services**: Manageable for a single developer
- **Event-driven**: Genuine asynchronous communication via Kafka
- **Distributed coordination**: Redis for locks, heartbeats, leader election
- **Fault tolerance**: Worker failure recovery, retry with backoff, idempotency
- **Observable**: Prometheus metrics, Grafana dashboards, structured logging
- **Testable**: Unit, integration, and failure scenario tests

Every technology serves a clear purpose in solving distributed-system problems, not just padding a resume.


---

## 9. Docker Deployment Architecture

### 9.1 Containerization Strategy

Chronos uses a production-ready Docker deployment with:
- **Multi-stage builds** for optimized image sizes
- **Non-root containers** for security
- **Health checks** for reliability
- **Graceful shutdown** for zero data loss
- **Horizontal scaling** for worker services

### 9.2 Container Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        DOCKER HOST                                 │
│                                                                    │
│  ┌───────────────────────────────────────────────────────────────┐ │
│  │              chronos-network (bridge)                         │ │
│  │                                                               │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │ │
│  │  │ MongoDB      │  │ Redis        │  │ Zookeeper    │         │ │
│  │  │ (mongo:7.0)  │  │ (redis:7.2)  │  │ (cp:7.5.0)   │         │ │
│  │  │ Port: 27017  │  │ Port: 6379   │  │ Port: 2181   │         │ │
│  │  │ Volume: data │  │ Volume: data │  │ Volume: data │         │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘         │ │
│  │         │                  │                  │               │ │
│  │  ┌──────▼──────────────────▼──────────────────▼──────┐        │ │
│  │  │                 Kafka                             │        │ │
│  │  │           (confluent/cp-kafka:7.5.0)              │        │ │
│  │  │              Ports: 9092, 9093                    │        │ │
│  │  │              Volume: kafka-data                   │        │ │
│  │  └───────────────────────┬───────────────────────────┘        │ │
│  │                          │                                    │ │
│  │  ┌───────────────────────┼──────────────────────────────┐     │ │
│  │  │                       │                              │     │ │
│  │  │  ┌────────────┐  ┌────▼──────┐  ┌────────────┐       │     │ │
│  │  │  │API Gateway │  │ Workflow  │  │ Scheduler  │       │     │ │
│  │  │  │(Port 8080) │  │(Port 8081)│  │(Port 8082) │       │     │ │
│  │  │  │JRE Alpine  │  │JRE Alpine │  │JRE Alpine  │       │     │ │
│  │  │  └─────┬──────┘  └─────┬─────┘  └─────┬──────┘       │     │ │
│  │  │        │               │              │              │     │ │
│  │  │        │         ┌─────▼──────────────▼────────┐     │     │ │
│  │  │        │         │  Worker-1 (Port 8083)       │     │     │ │
│  │  │        │         │  Worker-2 (Port 8084)       │     │     │ │
│  │  │        │         │  Worker-N (Port 8083+N)     │     │     │ │
│  │  │        │         │  (Horizontally Scalable)    │     │     │ │
│  │  │        │         └─────────────────────────────┘     │     │ │
│  │  │        │                                             │     │ │
│  │  │        └─────────────────┬──────────────────────┐    │     │ │
│  │  │                          │                      │    │     │ │
│  │  │                   ┌──────▼──────┐        ┌──────▼───┐│     │ │
│  │  │                   │ Prometheus  │        │ Grafana  ││     │ │
│  │  │                   │(Port 9090)  │───────▶│Port 3000 ││     │ │
│  │  │                   │Scrapes:/9080│        │Dashboard ││     │ │
│  │  │                   │     /9081   │        │          ││     │ │
│  │  │                   │     /9082   │        │          ││     │ │
│  │  │                   │     /9083   │        │          ││     │ │
│  │  │                   └─────────────┘        └──────────┘│     │ │
│  │  └──────────────────────────────────────────────────────┘     │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  Named Volumes:                                                    │
│  • chronos-mongodb-data, chronos-mongodb-config.                   │
│  • chronos-redis-data                                              │
│  • chronos-kafka-data                                              │
│  • chronos-zookeeper-data, chronos-zookeeper-logs                  │
│  • chronos-prometheus-data                                         │
│  • chronos-grafana-data                                            │
└────────────────────────────────────────────────────────────────────┘
```

### 9.3 Multi-Stage Build Process

Each application service uses a two-stage Docker build:

#### Stage 1: Builder (Maven + JDK 21)
```dockerfile
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B
```

**Benefits**:
- Cached dependency layer (rebuild only when pom.xml changes)
- Build artifacts created in isolated environment
- All build tools included

#### Stage 2: Runtime (JRE 21 Alpine)
```dockerfile
FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl wget tzdata
RUN addgroup -g 1001 chronos && adduser -D -u 1001 -G chronos chronos
WORKDIR /app
COPY --from=builder /build/target/*.jar app.jar
USER chronos:chronos
EXPOSE 8080 9080
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
```

**Benefits**:
- Small runtime image (~200MB vs 600MB+ with full JDK)
- Security: Non-root user (UID 1001)
- Health checks: Docker-native monitoring
- Optimized JVM settings for containers

### 9.4 Service Configuration

#### Port Allocation

| Service | Application Port | Management Port | Exposed |
|---------|------------------|-----------------|---------|
| API Gateway | 8080 | 9080 | ✓ |
| Workflow Service | 8081 | 9081 | ✓ |
| Scheduler Service | 8082 | 9082 | ✓ |
| Worker Service | 8083+ | 9083+ | ✓ (range) |
| MongoDB | 27017 | - | ✓ |
| Redis | 6379 | - | ✓ |
| Kafka | 9092 | - | ✓ |
| Prometheus | 9090 | - | ✓ |
| Grafana | 3000 | - | ✓ |

**Management Ports**: Used for:
- Spring Actuator endpoints (/actuator/health, /actuator/metrics)
- Prometheus scraping (/actuator/prometheus)
- Liveness/readiness probes
- Separated from application traffic for security

#### Environment-Based Configuration

Services use Spring profiles:
- **docker** (default): Container-optimized settings
- **development**: Local development with hot reload
- **production**: Production-ready configuration

Example configuration precedence:
```
application.yml (defaults)
  ↓
application-docker.yml (Docker overrides)
  ↓
Environment variables (.env file)
  ↓
Docker Compose overrides
```

### 9.5 Health Check Strategy

#### Infrastructure Services

**MongoDB**:
```yaml
healthcheck:
  test: mongosh --quiet --eval "db.adminCommand('ping')"
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 20s
```

**Redis**:
```yaml
healthcheck:
  test: redis-cli ping | grep PONG
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 10s
```

**Kafka**:
```yaml
healthcheck:
  test: kafka-broker-api-versions --bootstrap-server localhost:9092
  interval: 10s
  timeout: 10s
  retries: 5
  start_period: 30s
```

#### Application Services

All application services use Spring Boot Actuator:
```yaml
healthcheck:
  test: wget --quiet --tries=1 --spider http://localhost:8080/actuator/health
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 60s
```

**Kubernetes-Ready**:
```yaml
management:
  health:
    livenessState:
      enabled: true
    readinessState:
      enabled: true
```

Endpoints:
- `/actuator/health/liveness` - Container should restart if fails
- `/actuator/health/readiness` - Container should not receive traffic if fails

### 9.6 Dependency Management

#### Service Startup Order

```
1. Infrastructure Layer (parallel):
   - MongoDB (20s startup)
   - Redis (10s startup)
   - Zookeeper (10s startup)

2. Messaging Layer (depends on Zookeeper):
   - Kafka (30s startup, waits for Zookeeper healthy)

3. Observability Layer (parallel):
   - Prometheus (10s startup)
   - Grafana (20s startup, waits for Prometheus)

4. Application Layer (depends on infrastructure):
   - Workflow Service (40s startup, waits for MongoDB + Kafka)
   - Scheduler Service (40s startup, waits for MongoDB + Redis + Kafka)
   - Worker Service (40s startup, waits for MongoDB + Redis + Kafka)

5. Gateway Layer (depends on workflow service):
   - API Gateway (40s startup, waits for Workflow Service)

Total startup time: ~90-120 seconds
```

Docker Compose manages dependencies:
```yaml
scheduler-service:
  depends_on:
    mongodb:
      condition: service_healthy
    kafka:
      condition: service_healthy
    redis:
      condition: service_healthy
```

### 9.7 Volume Management

#### Persistent Volumes

All data is stored in named Docker volumes:

```yaml
volumes:
  chronos-mongodb-data:      # Workflow definitions, execution history
  chronos-mongodb-config:    # MongoDB configuration
  chronos-redis-data:        # Distributed locks, heartbeats, cache
  chronos-kafka-data:        # Event streams, consumer offsets
  chronos-zookeeper-data:    # Kafka coordination data
  chronos-zookeeper-logs:    # Zookeeper transaction logs
  chronos-prometheus-data:   # Metrics time series (15 days retention)
  chronos-grafana-data:      # Dashboard configurations
```

**Backup Strategy**:
```bash
# MongoDB backup
docker exec chronos-mongodb mongodump \
  --uri="mongodb://chronos:chronos123@localhost:27017/chronos?authSource=admin" \
  --out=/backup

# Copy from container
docker cp chronos-mongodb:/backup ./mongodb-backup-$(date +%Y%m%d).tar.gz
```

**Volume Lifecycle**:
- Created automatically on first `docker compose up`
- Persist across container restarts
- Removed only with `docker compose down -v`
- Can be backed up and restored

### 9.8 Network Architecture

#### Docker Bridge Network

```yaml
networks:
  chronos-network:
    driver: bridge
    name: chronos-network
```

**Internal DNS Resolution**:
- `mongodb` → MongoDB container
- `redis` → Redis container
- `kafka` → Kafka container
- `scheduler-service` → Scheduler container
- `worker-service` → Load-balanced across worker instances

**Security**:
- All inter-service communication uses internal network
- Only necessary ports exposed to host
- No direct access to infrastructure from outside

#### Service Discovery

Services use Docker DNS:
```yaml
# application-docker.yml
spring:
  data:
    mongodb:
      uri: mongodb://chronos:chronos123@mongodb:27017/chronos
    redis:
      host: redis
  kafka:
    bootstrap-servers: kafka:29092
```

**Note**: Kafka uses two listeners:
- `kafka:29092` - Internal (Docker network)
- `localhost:9092` - External (host machine)

### 9.9 Scaling Strategy

#### Horizontal Scaling

**Worker Service** (stateless, scales horizontally):
```bash
# Scale to 5 workers
docker compose up -d --scale worker-service=5

# Scale to 10 workers
docker compose up -d --scale worker-service=10

# Scale down to 2
docker compose up -d --scale worker-service=2
```

**Load Distribution**:
- Kafka consumer group ensures each task consumed by one worker
- Redis distributed locks prevent duplicate task execution
- Worker heartbeats enable detection of crashed workers
- Automatic rebalancing when workers join/leave

#### Vertical Scaling

Resource limits per service (docker-compose.yml):
```yaml
scheduler-service:
  deploy:
    resources:
      limits:
        cpus: '2.0'
        memory: 1G
      reservations:
        cpus: '0.5'
        memory: 256M
```

**JVM Tuning** (per service):
```env
JAVA_OPTS=-Xms256m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

### 9.10 Monitoring and Observability

#### Metrics Collection

Prometheus scrapes all services via management ports:
```yaml
scrape_configs:
  - job_name: 'api-gateway'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['api-gateway:9080']
  
  - job_name: 'scheduler-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['scheduler-service:9082']
  
  - job_name: 'worker-service'
    dns_sd_configs:
      - names: ['worker-service']
        port: 9083
```

**Key Metrics**:
- `chronos_workflow_executions_total` - Total workflow executions
- `chronos_task_completed_total{status="success|failure"}` - Task outcomes
- `chronos_worker_active_tasks` - Tasks in progress per worker
- `jvm_memory_used_bytes` - JVM heap usage
- `http_server_requests_seconds` - API latency

#### Grafana Dashboards

Pre-configured dashboards:
1. **Workflow Execution Dashboard**
   - Execution rate over time
   - Success/failure ratio
   - Average execution duration
   - Workflow status distribution

2. **Service Health Dashboard**
   - CPU and memory usage per service
   - JVM heap utilization
   - Garbage collection metrics
   - Request rates and latencies

3. **Infrastructure Dashboard**
   - MongoDB operations per second
   - Redis hit/miss ratio
   - Kafka lag and throughput
   - Container resource usage

#### Log Aggregation

All containers output JSON-formatted logs:
```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

**Log Viewing**:
```bash
# All services
./scripts/logs.sh --follow

# Specific service
./scripts/logs.sh scheduler --follow --lines 500

# Search logs
docker compose logs | grep "ERROR"
```

### 9.11 Security Considerations

#### Container Security

1. **Non-root Users**
   - All application containers run as UID 1001
   - Reduced attack surface
   - Complies with security best practices

2. **Read-only Filesystems** (optional enhancement):
   ```yaml
   scheduler-service:
     read_only: true
     tmpfs:
       - /tmp
       - /app/logs
   ```

3. **No Privileged Containers**
   - No containers run with `privileged: true`
   - No unnecessary capabilities added

#### Secrets Management

**Development** (.env file):
```env
JWT_SECRET=change-me-in-production
MONGODB_ROOT_PASSWORD=chronos123
```

**Production** (external secrets):
- Use Docker secrets
- Or AWS Secrets Manager
- Or HashiCorp Vault
- Never commit secrets to git

```yaml
scheduler-service:
  secrets:
    - jwt_secret
    - mongodb_password

secrets:
  jwt_secret:
    external: true
  mongodb_password:
    external: true
```

#### Network Security

1. **Expose only necessary ports**
   - API Gateway (8080) - public
   - Grafana (3000) - internal only
   - MongoDB, Redis, Kafka - internal only

2. **Use TLS in production**
   ```yaml
   api-gateway:
     environment:
       SERVER_SSL_ENABLED: "true"
       SERVER_SSL_KEY_STORE: "/certs/keystore.p12"
   ```

3. **Enable authentication**
   - MongoDB auth enabled by default
   - Redis password (optional)
   - Kafka SASL (production)

### 9.12 Deployment Workflow

#### Initial Deployment

```bash
# 1. Clone repository
git clone <repo-url> && cd chronos

# 2. Configure environment
cp .env.example .env
vim .env  # Update secrets and passwords

# 3. Start all services
./scripts/start.sh --build --detached

# 4. Verify health
./scripts/health-check.sh

# 5. Access services
open http://localhost:3000  # Grafana
open http://localhost:8080  # API Gateway
```

#### Rolling Updates

```bash
# 1. Build new version
docker compose build scheduler-service

# 2. Update with zero downtime
docker compose up -d --no-deps scheduler-service

# 3. Verify new version
curl http://localhost:8082/actuator/health

# 4. View logs
./scripts/logs.sh scheduler --follow
```

#### Rollback

```bash
# 1. Identify previous image
docker images | grep chronos-scheduler-service

# 2. Tag and rollback
docker tag chronos-scheduler-service:old chronos-scheduler-service:latest
docker compose up -d scheduler-service
```

### 9.13 Production Deployment Checklist

Before deploying to production:

**Security**:
- [ ] Generate strong JWT secret (`openssl rand -base64 32`)
- [ ] Change all default passwords
- [ ] Enable TLS/SSL for all external endpoints
- [ ] Configure MongoDB authentication properly
- [ ] Set up firewall rules (only API Gateway exposed)
- [ ] Enable audit logging

**Reliability**:
- [ ] Configure proper resource limits
- [ ] Set up automated backups (MongoDB, Redis)
- [ ] Configure log rotation and retention
- [ ] Set up Prometheus alerts
- [ ] Configure dead letter queues in Kafka
- [ ] Test disaster recovery procedures

**Scalability**:
- [ ] Benchmark system under load
- [ ] Configure horizontal scaling for workers
- [ ] Set up MongoDB replica set (3+ nodes)
- [ ] Configure Redis Sentinel or Cluster
- [ ] Set up Kafka cluster (3+ brokers)
- [ ] Implement rate limiting

**Monitoring**:
- [ ] Configure Grafana alert channels (email, Slack)
- [ ] Set up centralized logging (ELK, Splunk)
- [ ] Enable distributed tracing (Zipkin, Jaeger)
- [ ] Configure uptime monitoring (external)
- [ ] Set up on-call rotation

**Operations**:
- [ ] Document runbook procedures
- [ ] Test backup and restore
- [ ] Automate deployment pipeline
- [ ] Set up staging environment
- [ ] Configure blue-green or canary deployment
- [ ] Train operations team

---

## 10. Deployment Comparison

### 10.1 Docker Compose vs Kubernetes

| Aspect | Docker Compose | Kubernetes |
|--------|----------------|------------|
| Complexity | Low | High |
| Setup Time | Minutes | Hours/Days |
| Scaling | Manual (`--scale`) | Automatic (HPA) |
| High Availability | Single host | Multi-node cluster |
| Service Discovery | Docker DNS | CoreDNS + Services |
| Load Balancing | Round-robin | Configurable strategies |
| Rolling Updates | Manual | Built-in |
| Health Checks | Docker native | Liveness/Readiness probes |
| Secrets Management | .env or Docker secrets | Kubernetes Secrets |
| Monitoring | Prometheus + Grafana | + Kubernetes metrics |
| Best For | Development, small prod | Large-scale production |

**Recommendation**:
- **Development/Testing**: Docker Compose (this implementation)
- **Small Production**: Docker Swarm or single-node K8s
- **Large Production**: Full Kubernetes cluster

### 10.2 Migration Path to Kubernetes

Chronos is Kubernetes-ready:

1. **Convert Docker Compose to K8s**:
   ```bash
   kompose convert -f docker-compose.yml
   ```

2. **Helm Chart Structure**:
   ```
   chronos-helm/
   ├── Chart.yaml
   ├── values.yaml
   ├── templates/
   │   ├── api-gateway/
   │   ├── workflow-service/
   │   ├── scheduler-service/
   │   ├── worker-service/
   │   ├── mongodb/
   │   ├── redis/
   │   └── kafka/
   ```

3. **Key Changes Needed**:
   - StatefulSets for MongoDB, Kafka, Zookeeper
   - HorizontalPodAutoscaler for workers
   - Ingress for API Gateway
   - ConfigMaps for configuration
   - Secrets for sensitive data
   - PersistentVolumeClaims for data

4. **Advantages in Kubernetes**:
   - Auto-scaling based on metrics
   - Self-healing (automatic restarts)
   - Zero-downtime deployments
   - Multi-zone availability
   - Advanced networking (service mesh)

---

## 11. Summary

Chronos's Docker deployment architecture provides:

✓ **Production-Ready**: Multi-stage builds, health checks, graceful shutdown  
✓ **Secure**: Non-root containers, secrets management, network isolation  
✓ **Scalable**: Horizontal worker scaling, resource limits, monitoring  
✓ **Observable**: Comprehensive metrics, logs, dashboards  
✓ **Reliable**: Health checks, dependency management, data persistence  
✓ **Developer-Friendly**: Simple commands, quick startup, easy debugging  

**One-Command Deployment**:
```bash
docker compose up --build
```

**Full Stack Running**:
- ✓ 4 Application Services
- ✓ 5 Infrastructure Components
- ✓ 2 Observability Services
- ✓ Complete monitoring and metrics
- ✓ Production-grade reliability

For setup instructions, see [SETUP.md](../SETUP.md).  
For troubleshooting, see [TROUBLESHOOTING.md](./TROUBLESHOOTING.md).
