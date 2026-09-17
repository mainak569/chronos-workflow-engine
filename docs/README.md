# Chronos Documentation

Welcome to the Chronos distributed workflow orchestration engine documentation.

## Quick Start

1. **[Architecture Overview](./architecture.md)** - System design, components, and data flow
2. **[Design Decisions](./design-decisions.md)** - Technology choices and tradeoffs
3. **[Failure Recovery & Guarantees](./failure-recovery-guarantees.md)** - Production fault tolerance ⭐

## Core Documentation

### System Design

- **[Architecture](./architecture.md)** - Microservices architecture, Kafka events, MongoDB persistence
- **[Design Decisions](./design-decisions.md)** - Why we chose Spring Boot, Kafka, MongoDB, Redis
- **[Distributed Locking](./distributed-locking.md)** - Redis-based locking for safe task claiming

### Fault Tolerance ⭐

- **[Failure Recovery & Guarantees](./failure-recovery-guarantees.md)** - **Start here for production operations**
  - At-least-once execution semantics
  - Worker crash recovery (leases, heartbeats)
  - Retry with exponential backoff
  - Dead letter queue (DLQ)
  - Event idempotency
  - Optimistic locking
  - Configuration reference
  - Monitoring and best practices

- **[Failure Scenarios](./failure-scenarios.md)** - Legacy detailed scenario catalog (see above doc instead)

## Implementation Status

### 🟢 Completed Components

**Foundation (100%)**
- Spring Boot microservices (4 services)
- Kafka event-driven architecture (7 event types)
- MongoDB persistence
- Redis distributed locking
- Docker Compose infrastructure

**Workflow Management (100%)**
- Workflow definition and validation
- Task dependency management
- REST API with OpenAPI docs
- Comprehensive test coverage

**Execution Engine (100%)**
- Workflow execution orchestration
- Task state machine (PENDING → RUNNING → COMPLETED/FAILED)
- Dependency resolution
- Status tracking

**Worker Architecture (100%)**
- Worker registration and discovery
- Heartbeat mechanism (10s interval, 30s TTL)
- Worker lifecycle management
- Worker monitoring service

**Fault Tolerance (100%)** ⭐
- **Distributed locking** - Atomic task claiming (Redis SET NX EX)
- **Task leases** - Execution tracking with 5-minute TTL
- **Task recovery** - Expired lease detection and reassignment (30s scan)
- **Retry policy** - Exponential backoff (5s, 30s, 3min, capped at 5min)
- **Dead letter queue** - Permanent failure handling
- **Event idempotency** - Duplicate prevention (7-day Redis TTL)
- **Optimistic locking** - TaskExecution versioning with @Version

### 🟡 In Progress / Future Work

**Testing**
- Integration tests for fault-tolerance scenarios
- End-to-end workflow tests
- Performance benchmarks
- Chaos engineering tests

**Observability**
- Metrics export (Prometheus)
- Distributed tracing (Jaeger)
- Structured logging enhancement
- Alerting rules

**Advanced Features**
- Workflow versioning
- Parallel task execution groups
- Conditional branching
- Sub-workflows
- Scheduled/cron workflows
- Workflow templates

## Key Concepts

### Execution Semantics

**At-Least-Once Execution**
- Tasks may execute multiple times under failure conditions
- **Task implementations must be idempotent**
- Use external idempotency keys for non-idempotent operations

### Recovery Timeline

When a worker crashes:
```
T+0s:    Worker crashes
T+30s:   Heartbeat expires → worker marked UNAVAILABLE  
T+300s:  Task lease expires
T+330s:  Recovery service detects expired lease
T+330s:  Task reassigned via Kafka
T+331s:  New worker executes task
```

Total recovery time: **~5.5 minutes** (configurable)

### Retry Policy

Default exponential backoff:
```
Attempt 1 fails → wait 5 seconds
Attempt 2 fails → wait 30 seconds (5 * 6^1)
Attempt 3 fails → wait 3 minutes (5 * 6^2)
Attempt 4+ → capped at 5 minutes
```

After maxRetries exhausted → **Dead Letter Queue**

### Configuration Defaults

```yaml
# Heartbeat
chronos.heartbeat.interval: 10s
chronos.heartbeat.ttl: 30s

# Locking & Leases
chronos.lock.task-lock-ttl: 5m
chronos.task.lease-duration: 5m

# Retry
chronos.retry.max-attempts: 3
chronos.retry.base-delay: 5s
chronos.retry.backoff-multiplier: 6.0
chronos.retry.max-delay: 5m

# Recovery
chronos.recovery.scan-interval: 30s
chronos.recovery.initial-delay: 60s

# Idempotency
chronos.event.idempotency-ttl: 7d
```

## Architecture Diagram

```
┌─────────────────┐
│  Workflow API   │ ← REST API for workflow management
└────────┬────────┘
         │
         ↓
┌─────────────────┐       ┌─────────────────┐
│  Scheduler      │──────→│    Kafka        │
│   Service       │       │  Event Bus      │
└─────────────────┘       └────────┬────────┘
                                   │
         ┌─────────────────────────┼─────────────┐
         ↓                         ↓             ↓
┌─────────────────┐       ┌─────────────────┐   │
│   Worker-1      │       │   Worker-2      │   │
│  (AVAILABLE)    │       │   (BUSY)        │   │
└────────┬────────┘       └────────┬────────┘   │
         │                         │             │
         └─────────────┬───────────┘             │
                       ↓                         ↓
              ┌─────────────────┐       ┌──────────────┐
              │     Redis       │       │   MongoDB    │
              │  (Locks+State)  │       │ (Workflows)  │
              └─────────────────┘       └──────────────┘
```

## Service Breakdown

### 1. Workflow Service (Port 8081)
- **REST API** for workflow definition CRUD
- **Validation** of task dependencies and configurations
- **Storage** in MongoDB

### 2. Scheduler Service (Port 8082)
- **Workflow execution** orchestration
- **Dependency resolution** and task scheduling
- **Event publishing** (TaskReady, WorkflowCompleted)

### 3. Worker Service (Port 8083+)
- **Task execution** with distributed locking
- **Heartbeat** mechanism (10s interval)
- **Fault tolerance** (leases, retries, DLQ)
- **Event consumption** (TaskReady)

### 4. Monitor Service (Port 8084) - Future
- **Dashboard** for execution monitoring
- **Metrics** aggregation
- **Alerting** for DLQ and failures

## Development

### Prerequisites
```bash
- Java 21
- Maven 3.9+
- Docker & Docker Compose
- Redis 7.x
- MongoDB 6.x
- Kafka 3.x
```

### Build
```bash
mvn clean install
```

### Run Infrastructure
```bash
docker-compose up -d redis mongodb kafka zookeeper
```

### Run Services
```bash
# Terminal 1: Workflow Service
cd services/workflow-service && mvn spring-boot:run

# Terminal 2: Scheduler Service  
cd services/scheduler-service && mvn spring-boot:run

# Terminal 3: Worker Service
cd services/worker-service && mvn spring-boot:run
```

## Monitoring

### Key Metrics

**Worker Health:**
- `worker.count.active` - Number of active workers
- `worker.count.by_status{status=AVAILABLE|BUSY|UNAVAILABLE}`
- `worker.heartbeat.failures` - Heartbeat failure rate

**Task Execution:**
- `task.execution.duration` - Task execution time histogram
- `task.execution.count{status=completed|failed}` - Task outcome counts
- `task.lease.active` - Current leases held
- `task.lease.expired` - Expired leases awaiting recovery

**Retry & Recovery:**
- `task.retry.attempts{attempt=1|2|3}` - Retry distribution
- `task.recovery.triggers` - Recovery actions taken
- `task.dlq.messages` - Permanent failures

**Events:**
- `event.idempotency.duplicates` - Duplicate events detected
- `event.processing.duration` - Event processing time

### Logging

Structured JSON logs with correlation IDs:
```json
{
  "timestamp": "2026-09-15T19:22:00.000Z",
  "level": "INFO",
  "logger": "TaskEventConsumer",
  "message": "Task completed",
  "taskId": "task-123",
  "executionId": "exec-456",
  "workerId": "worker-001",
  "attemptNumber": 1,
  "durationMs": 1234
}
```

## Best Practices

### DOs

1. **Design idempotent tasks** - Handle duplicate execution safely
2. **Use external idempotency keys** - For payments, emails, critical operations
3. **Set appropriate timeouts** - Lease TTL should be 2-3x task duration
4. **Monitor DLQ** - Alert on high DLQ rate or message aging
5. **Test failure scenarios** - Worker crashes, network issues, retries
6. **Use structured logging** - Include correlation IDs for tracing

### DON'Ts

1. **Don't assume exactly-once** - Chronos provides at-least-once
2. **Don't ignore non-retriable errors** - Fail fast for logic errors
3. **Don't set lease TTL too short** - Risk premature task reassignment
4. **Don't forget optimistic locking** - Use `@Version` for concurrent updates
5. **Don't skip idempotency checks** - Duplicate events can occur
6. **Don't hardcode timeouts** - Make them configurable

## Troubleshooting

### Tasks Stuck in RUNNING

**Symptoms:**
- Tasks remain RUNNING for > 5 minutes
- No worker activity

**Diagnosis:**
```bash
# Check worker heartbeats
redis-cli KEYS "worker:heartbeat:*"

# Check task leases
redis-cli KEYS "chronos:lease:task:*"

# Check recovery service logs
docker logs chronos-worker | grep TaskRecoveryService
```

**Resolution:**
- Restart worker if crashed
- Check network connectivity
- Verify recovery service is running
- Manually trigger recovery if needed

### High DLQ Rate

**Symptoms:**
- Many messages in `chronos.task.failed.permanently`
- Workflows failing consistently

**Diagnosis:**
```bash
# Check DLQ messages
kafka-console-consumer --topic chronos.task.failed.permanently

# Check error patterns
grep "permanently failed" worker.log | jq .errorType | sort | uniq -c
```

**Resolution:**
- Fix underlying issue (API down, invalid config)
- Determine if errors are retriable
- Adjust maxRetries if needed
- Reprocess DLQ messages after fix

### Duplicate Task Execution

**Symptoms:**
- Same task executes twice
- External side effects duplicated

**Diagnosis:**
```bash
# Check idempotency service
redis-cli KEYS "chronos:event:processed:*"

# Check task execution logs
grep "Task completed" worker.log | grep taskId=task-123
```

**Resolution:**
- Ensure tasks are idempotent
- Verify EventIdempotencyService is working
- Check if lease expired during execution
- Consider increasing lease TTL

## Contributing

1. Read this documentation thoroughly
2. Understand failure scenarios and recovery
3. Write tests for fault tolerance
4. Follow idempotency principles
5. Add structured logging with correlation IDs

## License

[Add license information]

## Support

- **Documentation Issues:** Create GitHub issue
- **Production Support:** [Add contact information]
- **Slack Channel:** [Add channel link]

---

**Last Updated:** September 15, 2026  
**Status:** Production-Ready (Fault Tolerance Complete)
