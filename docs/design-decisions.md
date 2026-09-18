# Design Decisions

This document explains **why** specific technologies and architectural patterns were chosen for Chronos, what problems they solve, and what trade-offs were made.

---

## 1. Why 4 Services (Not 1, Not 10)?

### Decision
Four application services: API Gateway, Workflow Service, Scheduler Service, Worker Service.

### Problem It Solves
- **Separation of concerns**: Each service has a distinct responsibility
- **Independent scaling**: Only Worker Service needs horizontal scaling
- **Failure isolation**: Scheduler crash doesn't affect workers
- **Clear boundaries**: Easy to reason about system behavior

### Why Not Fewer?
- **1 Monolith**: Would hide distributed-system complexity. Chronos is explicitly a distributed system exercise.
- **2 Services**: Cannot demonstrate event-driven architecture and service coordination properly.

### Why Not More?
- **10+ Microservices**: Over-engineering. Would add operational complexity without educational benefit.
  - Example of unnecessary services: Separate "Notification Service" when we're not actually sending notifications
  - Example of unnecessary services: "Metrics Service" when Prometheus already aggregates metrics

### Trade-Offs
- 🟢 **Realistic complexity**: Demonstrates distributed patterns without overwhelming a single developer
- 🟢 **Manageable**: Can run and debug locally
- 🔴 **Not production-scale**: Real systems like Temporal have 20+ services, but that's not the goal

---

## 2. Why Kafka (Not RabbitMQ, Not Simple Queues)?

### Decision
Apache Kafka as the event backbone.

### Problem It Solves
1. **Asynchronous communication**: Services don't wait for synchronous responses
2. **Scalability**: Multiple workers can consume from the same topic
3. **Event replay**: Can replay events for debugging or recovery
4. **Durability**: Events are persisted to disk
5. **Partitioning**: Allows parallel processing while maintaining order within a partition

### Why Not RabbitMQ?
RabbitMQ is a valid choice. Kafka was selected because:
- **Event streaming model** is more appropriate for workflow orchestration (workflows generate streams of events)
- **Partition ordering** is useful for maintaining task execution order within a workflow
- **Industry standard** for event-driven architectures (Temporal, Airflow use similar patterns)

### Why Not Simple In-Memory Queues?
- Would defeat the purpose of a distributed system
- No durability: events lost on service restart
- No multi-consumer support

### Why Not HTTP Webhooks?
- Requires services to be synchronously available
- No built-in retry or durability
- Couples producer and consumer

### Trade-Offs
- 🟢 **Distributed event-driven architecture**: Genuinely asynchronous
- 🟢 **Scalable**: Can add workers without code changes
- 🔴 **Operational complexity**: Requires Zookeeper, more moving parts
- 🔴 **At-least-once delivery**: Requires idempotent handlers (but this is a valuable learning)

---

## 3. Why MongoDB (Not PostgreSQL)?

### Decision
MongoDB for durable state storage.

### Problem It Solves
1. **Flexible schema**: Workflow definitions vary (different tasks, configurations)
2. **Embedded documents**: Tasks can be embedded in workflows naturally
3. **Document model**: Workflows are naturally document-shaped (JSON-like structure)
4. **Easy local setup**: Single Docker container, no schema migrations initially

### Why Not PostgreSQL?
PostgreSQL would also work well. MongoDB was chosen because:
- **Schema flexibility**: Workflows have variable task structures
- **JSON-native**: Task configurations are JSON blobs
- **No JOIN complexity**: Workflows and executions are mostly self-contained documents

### Trade-Offs
- 🟢 **Flexible schema**: Easy to add new task types
- 🟢 **Natural fit**: Workflow definitions are JSON-like
- 🔴 **No ACID across documents**: Must handle eventual consistency (but this is true in distributed systems anyway)
- 🔴 **No complex queries**: Harder to do analytical queries (but Chronos is operational, not analytical)

### What Would PostgreSQL Bring?
- Stronger ACID guarantees (transactions across tables)
- Better for analytics queries
- More mature tooling

**Conclusion**: Either would work. MongoDB was chosen for schema flexibility and JSON-native support.

---

## 4. Why Redis (Not Zookeeper, Not Database Locks)?

### Decision
Redis for distributed coordination (locks, heartbeats, leader election).

### Problem It Solves
1. **Fast distributed locks**: Sub-millisecond lock acquisition
2. **TTL support**: Automatic expiration for heartbeats and locks
3. **Atomic operations**: SET NX EX is atomic
4. **Low latency**: In-memory, critical for lock contention scenarios

### Why Not Database Locks (MongoDB)?
```sql
-- MongoDB advisory lock approach:
db.locks.findAndModify({
  query: { _id: "task-123", locked: false },
  update: { $set: { locked: true, ownerId: "worker-01" } }
})
```

**Problems**:
- Slower (network + disk)
- Harder to implement TTL (requires background job to clean up stale locks)
- Database becomes a bottleneck for high-frequency lock operations

### Why Not Zookeeper?
Zookeeper is actually designed for coordination, but:
- **Heavier**: More complex to set up and operate
- **Overkill**: Zookeeper provides stronger consistency than needed
- **Already have Kafka**: Kafka uses Zookeeper, but we don't need two coordination systems

### Why Not Redlock (Redis Cluster)?
Chronos uses **single Redis instance** with simple locks, not Redlock.

**Reason**: Local development simplicity. In production, Redlock or Zookeeper would be better.

### Trade-Offs
- 🟢 **Fast**: In-memory, low latency
- 🟢 **Simple**: SET NX EX is easy to understand
- 🟢 **TTL built-in**: Automatic cleanup
- 🔴 **Single point of failure**: If Redis dies, no locks available
- 🔴 **Not as strong as Zookeeper**: But sufficient for Chronos's needs

---

## 5. Why JWT (Not Sessions)?

### Decision
JWT (JSON Web Tokens) for authentication.

### Problem It Solves
1. **Stateless**: No session storage needed
2. **Scalable**: API Gateway doesn't need to query a session store
3. **Distributed-friendly**: Works across multiple service instances
4. **Standard**: Industry-standard auth mechanism

### Why Not Session Cookies?
- Requires shared session storage (Redis or database)
- Harder to scale (need sticky sessions or shared state)
- Not REST-friendly

### Trade-Offs
- 🟢 **Stateless**: No shared session storage
- 🟢 **Scalable**: Works with multiple API Gateway instances
- 🔴 **Cannot revoke**: Once issued, valid until expiration (can be mitigated with short TTL + refresh tokens)
- 🔴 **Larger**: JWT tokens are larger than session IDs

### Implementation Detail
```java
// JWT payload
{
  "sub": "user-123",
  "email": "user@example.com",
  "exp": 1735740000,
  "iat": 1735736400
}
```

**Security**: JWT secret is environment variable, never committed.

---

## 6. Why At-Least-Once Delivery (Not Exactly-Once)?

### Decision
Kafka configured for **at-least-once** delivery, handlers are **idempotent**.

### Problem It Solves
Exactly-once is extremely complex to implement correctly in distributed systems. At-least-once with idempotency is:
- Simpler to implement
- More realistic for educational purposes
- Still correct (idempotent handlers prevent duplicate side effects)

### Why Not Exactly-Once?
Kafka supports "exactly-once semantics" but only within Kafka itself (producer to broker to consumer). True end-to-end exactly-once requires:
- Transactional outbox pattern
- Two-phase commit
- Distributed transactions across Kafka + MongoDB

**This is overkill for Chronos.**

### How Idempotency Works

**Example: TaskCompleted event delivered twice**

```java
// First delivery
handleTaskCompleted(event) {
    task.status = RUNNING
    task.transitionTo(SUCCESS) // 🟢 Succeeds
}

// Second delivery (duplicate)
handleTaskCompleted(event) {
    task.status = SUCCESS
    task.transitionTo(SUCCESS) // 🔴 Rejected: "Already in SUCCESS state"
}
```

**Key principle**: State transitions validate current state.

### Trade-Offs
- 🟢 **Simpler**: No distributed transactions
- 🟢 **Realistic**: Matches real-world systems (AWS SQS, most message brokers)
- 🟢 **Correct**: Idempotency prevents corruption
- 🔴 **Duplicate processing**: Wasted work (but rare)

---

## 7. Why Exponential Backoff (Not Fixed Delay)?

### Decision
Failed tasks retry with exponential backoff: 5s, 10s, 20s, ...

### Problem It Solves
1. **Transient failures**: Give system time to recover
2. **Avoid thundering herd**: Don't retry all tasks simultaneously
3. **Adaptive**: Short retries for quick failures, longer for persistent issues

### Why Not Fixed Delay?
```text
Fixed 5s delay:
  Failure → 5s → Retry → Failure → 5s → Retry → Failure
  
Problem: If failure is due to downstream service being down for 30s,
         all retries happen during downtime (wasted).
```

### Why Not Linear Backoff?
```text
Linear: 5s, 10s, 15s, 20s
Exponential: 5s, 10s, 20s, 40s

Exponential grows faster, giving more time for recovery.
```

### Trade-Offs
- 🟢 **Handles transient failures**: Brief network glitch recovers quickly
- 🟢 **Prevents overload**: Doesn't hammer failing service
- 🔴 **Longer recovery time**: If service recovers quickly, exponential backoff wastes time (but we cap at 5 minutes)

---

## 8. Why Leader Election for Scheduler (Not All-Active)?

### Decision
Multiple scheduler instances use Redis leader election. Only the leader schedules tasks.

### Problem It Solves
**Duplicate scheduling**: Without coordination, all schedulers would process the same `WorkflowStarted` event.

**Scenario without leader election**:
```text
Scheduler-1: Sees WorkflowStarted → Publishes TaskReady
Scheduler-2: Sees WorkflowStarted → Publishes TaskReady (duplicate)
Scheduler-3: Sees WorkflowStarted → Publishes TaskReady (duplicate)
```

**Result**: 3x TaskReady events for the same task → workers claim same task multiple times (distributed lock helps, but wasteful).

### Why Not All-Active?
Could make all schedulers active with idempotency:
```java
// Each scheduler checks: "Did I already publish TaskReady for this task?"
if (!taskRepository.existsByExecutionIdAndTaskIdAndStatus(executionId, taskId, READY)) {
    publishTaskReady();
}
```

**Problem**: Race condition between check and publish. Would need distributed lock per task → same complexity as leader election, but more locks.

### Why Leader Election?
- Simpler: One active scheduler, others are warm standby
- Efficient: No lock contention
- Fast failover: If leader dies, new leader elected in ~10 seconds

### Trade-Offs
- 🟢 **Avoids duplicates**: Only one scheduler is active
- 🟢 **Efficient**: No lock contention
- 🔴 **Single bottleneck**: Only one scheduler is active (but scheduler is lightweight, not a real bottleneck)
- 🔴 **Failover delay**: ~10 seconds to elect new leader

---

## 9. Why Consumer Groups for Workers (Not Competing Consumers)?

### Decision
Workers belong to the same Kafka consumer group: `worker-group`.

### Problem It Solves
**Load balancing**: Kafka automatically distributes partitions across workers.

```text
task-events topic (6 partitions):

worker-1: consumes partitions 0, 1
worker-2: consumes partitions 2, 3
worker-3: consumes partitions 4, 5
```

### Why Not Separate Consumer Groups?
If each worker had its own consumer group, **all workers would receive all events** (broadcast).

```text
Worker-1 consumer group → Receives ALL TaskReady events
Worker-2 consumer group → Receives ALL TaskReady events (duplicate)
```

**Result**: Distributed lock prevents duplicate execution, but wastes resources.

### Trade-Offs
- 🟢 **Automatic load balancing**: Kafka distributes partitions
- 🟢 **Efficient**: Each event consumed once per consumer group
- 🔴 **Rebalancing**: Adding/removing workers triggers rebalance (brief pause)

---

## 10. Why `executionId` as Kafka Message Key (Not `taskId`)?

### Decision
`task-events` topic uses `executionId` as the message key.

### Problem It Solves
**Ordering within a workflow execution**: All tasks for the same execution go to the same partition, preserving order.

**Example workflow**:
```text
Execution-1: Task-A → Task-B → Task-C
Execution-2: Task-D → Task-E → Task-F
```

With `executionId` as key:
```text
Partition 0: Execution-1 events (A, B, C) in order
Partition 1: Execution-2 events (D, E, F) in order
```

### Why Not `taskId`?
Tasks from different executions would be interleaved:
```text
Partition 0: Task-A (exec-1), Task-A (exec-2), Task-A (exec-3)
```

Makes dependency tracking harder: "Has Task-A from exec-1 completed?" requires filtering by executionId.

### Why Not `workflowId`?
All executions of the same workflow would go to the same partition:
```text
Partition 0: Workflow-X, Exec-1, Exec-2, Exec-3, ...
```

**Problem**: Limits parallelism. If Workflow-X is popular, all its executions are processed serially.

### Trade-Offs
- 🟢 **Order within execution**: Task events for the same execution are ordered
- 🟢 **Parallelism across executions**: Different executions processed in parallel
- 🔴 **Partition skew**: Popular workflows (many executions) may overload partitions (acceptable for Chronos scale)

---

## 11. Why 5-Minute Task Lock TTL?

### Decision
Task locks expire after 5 minutes.

### Problem It Solves
**Worker hangs**: If a worker acquires a lock but hangs (not crashed, just slow), the task shouldn't be stuck forever.

**Scenario**:
```text
T=0:   Worker-1 claims task, starts execution
T=1m:  Task running...
T=3m:  Task running... (network glitch, slow API)
T=5m:  Lock expires
T=5m:  Worker-2 can now claim task, starts execution
T=6m:  Worker-1 finishes, tries to save result → Rejected (lost lock ownership)
```

### Why Not Shorter (30 seconds)?
Tasks might legitimately take 1-2 minutes (image processing, API calls).

### Why Not Longer (30 minutes)?
If worker hangs, task is stuck for too long. 5 minutes is a reasonable upper bound for most tasks.

### How It Interacts with Worker Heartbeat
- **Heartbeat TTL**: 30 seconds → Detects worker crash
- **Lock TTL**: 5 minutes → Handles worker hang on specific task

**Different failure modes**:
- Worker crash: Detected via heartbeat in 30s
- Worker hang: Detected via lock expiration in 5m

### Trade-Offs
- 🟢 **Prevents indefinite stalls**: Task recovers after 5m
- 🔴 **Duplicate execution**: If worker is slow (not hung), two workers may execute the same task (last-write-wins, or optimistic locking on save)

### Mitigation
Workers check lock ownership before saving results:
```java
if (stillHoldLock(task)) {
    saveResult(task);
} else {
    log.warn("Lost lock, discarding result");
}
```

---

## 12. Why Separate Workflow Service and Scheduler Service?

### Decision
Workflow Service owns workflow definitions and executions. Scheduler Service determines which tasks are ready.

### Problem It Solves
**Separation of concerns**:
- **Workflow Service**: Stateful CRUD operations (create workflow, start execution)
- **Scheduler Service**: Stateless event-driven logic (determine ready tasks)

### Why Not Combine Them?
Could have a single "Workflow + Scheduler" service. Separated because:
1. **Scalability**: Workflow Service is request-driven (HTTP). Scheduler is event-driven (Kafka). Different scaling characteristics.
2. **Failure isolation**: Scheduler crash doesn't prevent workflow creation.
3. **Clear boundaries**: Easier to reason about responsibilities.

### Why Not More Granular?
Could further split:
- Workflow Definition Service
- Workflow Execution Service
- Dependency Resolution Service

**Problem**: Over-engineering. Four services is the sweet spot for Chronos.

### Trade-Offs
- 🟢 **Clear responsibilities**: Easy to understand what each service does
- 🟢 **Independent scaling**: Scale scheduler if many workflows, scale workflow service if many API requests
- 🔴 **More services**: More deployment complexity (but manageable with Docker Compose)

---

## 13. Why Worker Heartbeat in Redis (Not MongoDB)?

### Decision
Workers update heartbeat in Redis with 30s TTL.

### Problem It Solves
**Fast failure detection**: Redis TTL expires automatically, no need for background cleanup job.

### Why Not MongoDB?
```javascript
// MongoDB approach
db.workers.updateOne(
  { _id: "worker-01" },
  { $set: { lastHeartbeat: new Date() } }
)

// Separate job checks for stale workers
setInterval(() => {
  const staleWorkers = db.workers.find({
    lastHeartbeat: { $lt: new Date(Date.now() - 30000) }
  });
  // Mark as unavailable
}, 15000);
```

**Problems**:
- Requires background job
- Slower (disk write)
- More complex (manual staleness detection)

### Redis Approach
```java
// Update heartbeat
redisTemplate.opsForValue().set("worker:worker-01:heartbeat", timestamp, 30, SECONDS);

// Check if alive
Boolean alive = redisTemplate.hasKey("worker:worker-01:heartbeat");
```

**Benefits**:
- Automatic expiration (no background job needed for cleanup)
- Fast (in-memory)
- Simple (TTL is built-in)

### Trade-Offs
- 🟢 **Automatic expiration**: Redis handles cleanup
- 🟢 **Fast**: In-memory, low latency
- 🔴 **Ephemeral**: No historical heartbeat data (but we can log to MongoDB for history if needed)

---

## 14. Why Testcontainers (Not Mocked Infrastructure)?

### Decision
Integration tests use Testcontainers (real MongoDB, Kafka, Redis in Docker).

### Problem It Solves
**Real infrastructure behavior**: Mocks don't catch:
- Serialization issues (Kafka)
- Index performance problems (MongoDB)
- Lock expiration race conditions (Redis)

### Why Not Mocks?
```java
// Mocked test: passes, but doesn't reflect reality
when(kafkaProducer.send(any())).thenReturn(CompletableFuture.completedFuture(null));

// Real test: catches serialization bug
kafkaProducer.send(event); // Throws SerializationException
```

### Why Not Embedded Versions (H2, Embedded Kafka)?
- Behavior differs from production (H2 is not MongoDB)
- Embedded Kafka is heavyweight, Testcontainers is cleaner

### Trade-Offs
- 🟢 **Realistic**: Tests use real infrastructure
- 🟢 **Catches bugs**: Serialization, network issues
- 🔴 **Slower**: Docker startup adds ~10s to test suite
- 🔴 **Requires Docker**: Developers need Docker installed

---

## 15. Why Structured Logging (Not Plain Text)?

### Decision
Logs are JSON-structured with correlation IDs, workflow IDs, execution IDs, etc.

### Problem It Solves
**Distributed tracing**: Easy to correlate logs across services.

**Example**:
```json
{
  "timestamp": "2026-01-01T10:00:00Z",
  "level": "INFO",
  "service": "worker-service",
  "workerId": "worker-01",
  "correlationId": "req-abc-123",
  "workflowId": "workflow-123",
  "executionId": "execution-789",
  "taskId": "resize-image",
  "message": "Task completed successfully"
}
```

**Query**: "Show me all logs for executionId=execution-789 across all services"

```bash
grep "execution-789" *.log | jq .
```

### Why Not Plain Text?
```text
2026-01-01 10:00:00 INFO Worker worker-01 completed task resize-image
```

**Problem**: Hard to parse, hard to correlate across services.

### Trade-Offs
- 🟢 **Easy to parse**: JSON is machine-readable
- 🟢 **Correlate across services**: Correlation ID traces requests
- 🔴 **Less human-readable**: JSON is verbose (but tools like `jq` help)

---

## 16. Why Prometheus + Grafana (Not ELK Stack)?

### Decision
Prometheus for metrics collection, Grafana for visualization.

### Problem It Solves
**Observability**: Monitor workflow execution, task failures, worker health, Kafka lag.

### Why Not ELK Stack (Elasticsearch, Logstash, Kibana)?
ELK is for **log aggregation**. Prometheus is for **metrics**.

**Different use cases**:
- **Logs**: "What happened?" (events, errors)
- **Metrics**: "How much?" (counters, gauges, histograms)

Chronos needs both, but:
- Prometheus is lighter (no Elasticsearch cluster)
- Grafana is simpler for metrics dashboards
- ELK is overkill for local development

### In Production
Would add ELK for centralized logging. But for Chronos, Prometheus + Grafana is sufficient.

### Trade-Offs
- 🟢 **Lightweight**: Single Prometheus + Grafana container
- 🟢 **Metrics-focused**: Perfect for monitoring counters, rates, latencies
- 🔴 **No centralized logs**: Logs are still in individual containers (acceptable for local dev)

---

## 17. Workflow Cancellation: Cooperative, Not Preemptive

### Decision
Cancellation marks the execution `CANCELLED` and stops scheduling; it does **not** interrupt tasks
that are already running.

### How It Works
1. Client: `POST /api/v1/workflows/executions/{id}/cancel` (only the owner; `409` if already finished)
2. Workflow service: execution → `CANCELLED`, every non-terminal task → `CANCELLED`
3. Scheduler: never dispatches tasks of a finished execution
4. A task that was already running finishes on its worker; its result is ignored because the task
   is already in a terminal state

### Why Not Interrupt Workers?
- Task code may not be interruptible (external calls, long-running work)
- Interrupting mid-task risks leaving side effects half-done; letting the attempt finish is simpler
  and safe because results are ignored
- Race conditions (a task completing just as the cancel arrives) are resolved by the task state
  machine: the first terminal status wins

### Trade-Offs
- 🟢 **Simple and race-free**: no cancel protocol between scheduler and workers
- 🔴 **Wasted work**: a running task keeps using its worker until it finishes

---

## 18. Cron Scheduling in the Scheduler Service

### Decision
Workflows can carry an optional cron `schedule` (Spring 6-field format) and `timezone`; the leader
scheduler starts executions automatically. No Quartz: the existing leader election, idempotency
store and outbox already provide what is needed.

### How It Works
1. Every 15s the leader syncs `scheduler_state` from workflows that have a schedule (new, changed and
   deleted schedules)
2. Every 5s it finds due entries (`nextScheduledTime <= now`)
3. For each due slot it reserves an execution ID in Redis keyed by workflow + scheduled second, so a
   retried or duplicated run of the same slot reuses the same execution
4. It inserts the task executions, then the workflow execution, and dispatches ready tasks through
   the outbox
5. It advances `nextScheduledTime` last (optimistic locking on `scheduler_state`)

### Restart Recovery
Because the state is advanced last and the slot's execution ID is reserved first, a crash at any
point re-runs the same slot idempotently. Missed slots while all schedulers were down are skipped:
the next run is computed from the current time.

### Trade-Offs
- 🟢 **No extra infrastructure**: reuses leader election, Redis and the outbox
- 🔴 **Minimum granularity** is bounded by the 5s scheduling scan

---

## 19. Why No Web UI (Initially)?

### Decision
Chronos is API-only (no web frontend).

### Problem
Web UI adds significant complexity:
- Frontend framework (React, Vue)
- Build pipeline (webpack, etc.)
- State management
- API integration
- Deployment complexity

### Why Deferred?
- Core distributed-system concepts are backend-only
- Postman/curl is sufficient for demonstration
- UI doesn't teach distributed-system principles
- Would double the project scope

### Demonstration
Use Postman collection + Grafana dashboards to demonstrate the system.

---

## 20. Why No Kubernetes (Initially)?

### Decision
Chronos uses Docker Compose, not Kubernetes.

### Problem
Kubernetes adds:
- YAML configurations (Deployments, Services, ConfigMaps)
- kubectl CLI
- Cluster setup (Minikube, kind, or cloud)
- Ingress controllers
- Persistent volumes
- Secrets management

### Why Docker Compose?
- **Local development**: Single `docker compose up` command
- **Simpler**: No cluster management
- **Sufficient**: Demonstrates horizontal scaling (`--scale worker-service=5`)
- **Educational value**: Focus on distributed-system concepts, not Kubernetes

### Trade-Offs
- 🟢 **Simple**: Easy to run locally
- 🟢 **Fast**: No cluster bootstrap time
- 🔴 **Not production-ready**: Real systems use Kubernetes (but that's not the goal)

### Future Enhancement
Kubernetes manifests can be added later to demonstrate production deployment.

---

## Summary of Key Trade-Offs

| Decision | Benefit | Trade-Off |
|---|---|---|
| 4 services | Realistic complexity | Not as scalable as 10+ services |
| Kafka | Event-driven architecture | At-least-once delivery |
| MongoDB | Flexible schema | Weaker ACID than PostgreSQL |
| Redis | Fast locks & TTL | Single point of failure |
| JWT | Stateless auth | Cannot revoke before expiration |
| At-least-once | Simpler than exactly-once | Duplicate events (handled via idempotency) |
| Leader election | Avoids duplicate scheduling | Single active scheduler |
| 5-min lock TTL | Prevents indefinite stalls | May duplicate work if worker is slow |
| Testcontainers | Real infrastructure in tests | Slower test suite |
| No web UI | Focus on backend | Less demo-friendly |
| Docker Compose | Simple local setup | Not production-ready |

Every decision prioritizes **correctness** and **learning value** over premature optimization or unnecessary features.
