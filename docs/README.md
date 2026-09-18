# Chronos Documentation

Chronos is a distributed workflow orchestration engine: you define workflows as DAGs of tasks,
and Chronos executes them across a pool of workers with retries, crash recovery and cron scheduling.

## Documents

| Document | What it covers |
|---|---|
| [API reference](./api/workflow-api.md) | REST endpoints, request/response examples, error codes |
| [Architecture](./architecture.md) | Services, data model, Kafka topics, Redis keys, execution model |
| [Design decisions](./design-decisions.md) | Technology choices and trade-offs |
| [Failure recovery & guarantees](./failure-recovery-guarantees.md) | Failure scenarios and how the system recovers |
| [Distributed locking](./distributed-locking.md) | Redis locks and leases for task claiming |
| [Kafka event architecture](./kafka-event-architecture.md) | Topics, event schemas, partitioning |
| [Scheduler architecture](./scheduler-architecture.md) | Leader election, dispatch, outbox, cron |
| [Security architecture](./security-architecture.md) | JWT authentication, resource isolation |
| [Observability](./observability-architecture.md) | Metrics catalog, Grafana dashboard, alerts, logging |
| [Local development](./local-development.md) | Running the stack, useful commands, troubleshooting |

A Postman collection covering every endpoint is in [`../postman/`](../postman/chronos-api.postman_collection.json).

## What Is Implemented

**Services**
- **API gateway**: JWT validation, routing to the workflow service, per-client rate limiting, correlation IDs
- **Workflow service**: users and JWT issuing, workflow definitions (DAG validation, optional cron schedule),
  starting, inspecting and cancelling executions
- **Scheduler service**: turns task status events into execution state, dispatches ready tasks,
  owns retries, recovery sweeps and cron scheduling; one elected leader runs the background jobs
- **Worker service** (horizontally scalable): claims and executes tasks, heartbeats, reports results

**Reliability**
- At-least-once delivery with idempotent consumers on every Kafka topic
- Transactional outbox for TaskReady events; each task attempt is claimed atomically before dispatch
- Redis leader election for the scheduler (Lua compare-and-renew / compare-and-delete)
- Per-task retries with exponential backoff; permanent failures go to a dead letter topic
- Crash recovery: a lost worker's running tasks are requeued and its locks released
- Optimistic locking on executions and tasks; out-of-order and duplicate events are tolerated
- Dead-letter topics (`{topic}.dlt`) for records that keep failing

**Operations**
- Docker Compose stack with healthchecks and scalable workers
- Prometheus metrics for workflows, tasks, workers, Kafka lag, HTTP and JVM; a provisioned Grafana
  dashboard; alert rules
- 220+ automated tests, including Testcontainers integration tests against real MongoDB, Kafka and Redis

**Task execution is simulated**: workers sleep for `simulateDurationMs` (default 1s) and return a
result. Task configuration can simulate failures (`failUntilAttempt`, `failPermanently`) to exercise
retries and dead-lettering. Real task handlers would plug into `TaskEventConsumerEnhanced.executeTask`.

## Execution Semantics

- **At-least-once**: a task attempt can run more than once (e.g. its worker dies after finishing but
  before reporting). Task implementations should be idempotent.
- **No concurrent duplicates**: a Redis lock per `{executionId}:{taskId}` stops two workers running the
  same attempt at the same time.
- **Exactly one result per attempt**: the scheduler accepts the first terminal result for the current
  attempt and ignores duplicates and results of older attempts.

## Recovery Timeline (worker crash)

```
T+0s      Worker crashes while running a task
T+30s     Its heartbeat key expires
T+30-45s  Leader scheduler detects it (checks every 15s), requeues the worker's RUNNING tasks,
          releases their locks and dispatches them again
T+31-46s  Another worker picks the task up
```

If a TaskReady event is lost, the stale-dispatch sweep re-dispatches it after the dispatch timeout (5 minutes).

## Retry Policy

Per task, from the workflow definition's `retryConfig` (defaults shown):

```json
"retryConfig": { "maxAttempts": 3, "initialDelayMs": 5000, "backoffMultiplier": 2.0, "maxDelayMs": 300000 }
```

Delay before attempt *n+1* = `initialDelayMs * backoffMultiplier^(n-1)`, capped at `maxDelayMs`
(5s, 10s, 20s, ...). Non-retriable errors (`IllegalArgumentException`, `NullPointerException`,
`SecurityException`, unsupported task type) fail the task immediately.

## Configuration Defaults

```yaml
# Worker
worker.heartbeat.interval: 10000            # ms; heartbeat key TTL is 30s
worker.supported-task-types: IMAGE_RESIZE,IMAGE_COMPRESS,DATA_PROCESSING,DATA_VALIDATION
chronos.lock.task-lock-ttl: 5m
chronos.task.lease-duration: 5m
chronos.task.lease-renewal-interval: 120000 # ms
chronos.event.idempotency-ttl: 7d

# Scheduler
chronos.scheduler.leader-election.ttl: 30s
chronos.scheduler.leader-election.renewal-interval: 10000
chronos.scheduler.retry.initial-delay: 5s   # defaults when a task has no retryConfig
chronos.scheduler.retry.backoff-multiplier: 2.0
chronos.scheduler.retry.max-delay: 5m
chronos.scheduler.restart-recovery.dispatch-timeout: 5m
chronos.scheduler.restart-recovery.retry-sweep-interval: 2000
chronos.scheduler.restart-recovery.sweep-interval: 60000
chronos.scheduler.scheduling.interval: 5000       # cron scan
chronos.scheduler.scheduling.sync-interval: 15000 # cron schedule sync
worker.monitor.interval: 15000                    # heartbeat expiry check
chronos.outbox.poll-interval: 1000
chronos.admin.token: ""                            # enables /api/locks when set

# Gateway
gateway.rate-limit.requests-per-minute: 120
```

## Troubleshooting

**Execution stuck in PENDING/RUNNING**
```bash
# Task states of the execution
docker exec chronos-mongodb mongosh -u chronos -p chronos123 --authenticationDatabase admin chronos \
  --eval 'db.task_executions.find({executionId: "<id>"}, {taskId:1, status:1, attemptNumber:1, workerId:1, dispatchedAt:1})'

# Live workers and their heartbeats
docker exec chronos-redis redis-cli --scan --pattern 'worker:heartbeat:*'

# Scheduler recovery activity
docker compose logs scheduler-service | grep -E "Recovered|Released stale|Marking worker"

# Records that could not be processed
docker exec chronos-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic chronos.task.ready.dlt --from-beginning --property print.headers=true
```

**Tasks failing permanently**: check the task's `errorMessage` via
`GET /api/v1/workflows/executions/{id}/tasks`, and the dead letter topic `chronos.task.failed.permanently`.

**No scheduler leader** (`sum(chronos_scheduler_leader) == 0`): check Redis connectivity from the
scheduler; leadership is re-acquired within 30s.

## Limitations and Future Work

- Real task executors (tasks are simulated)
- Distributed tracing (correlation IDs are propagated, but there is no tracing backend)
- Redis-backed rate limiting across multiple gateway instances
- Separate topics or consumer groups per task type (today every worker must support every task type in use)
- Web UI
