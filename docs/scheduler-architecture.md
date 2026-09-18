# Scheduler Service Architecture

## Overview

The scheduler service drives every workflow execution. It turns task status events from workers into
execution state, dispatches tasks whose dependencies are complete, owns retries, recovers from lost
workers and lost events, and starts cron-scheduled workflows.

Several scheduler instances can run at once. Event consumption is shared through Kafka consumer groups;
background jobs run only on the **leader**, elected through Redis.

| Responsibility | Component | Runs on |
|---|---|---|
| Apply task started/completed/failed events | `TaskStatusEventConsumer` → `ExecutionOrchestrationService` | every instance (Kafka group) |
| Dispatch initial tasks of new executions | `WorkflowEventConsumer` → `TaskDispatchService` | every instance |
| React to workers shutting down | `WorkerStatusEventConsumer` → `WorkerFailureHandler` | every instance |
| Publish outbox messages to Kafka | `OutboxPublisherService` | leader |
| Dispatch due retries, recover stale dispatches | `RestartRecoveryService` | leader |
| Detect dead workers (heartbeat expiry) | `WorkerMonitorService` → `WorkerFailureHandler` | leader |
| Cron schedules | `WorkflowScheduler` | leader |
| Leadership | `LeaderElectionService` | every instance |

## Architecture Diagram

```
  chronos.workflow.created ──┐        ┌── chronos.task.started / completed / failed
                             ▼        ▼
                 ┌────────────────────────────────┐
                 │  Event consumers (all nodes)    │
                 │  ExecutionOrchestrationService  │──── MongoDB: workflow_executions,
                 │  TaskDispatchService            │              task_executions
                 └───────────────┬────────────────┘
                                 │ claim attempt (atomic update) + write TaskReady
                                 ▼
                        MongoDB: outbox_messages
                                 │
                 ┌───────────────▼────────────────┐
                 │  Leader-only jobs               │
                 │  OutboxPublisherService ────────┼──▶ chronos.task.ready ──▶ workers
                 │  RestartRecoveryService         │
                 │  WorkerMonitorService           │◀── Redis: worker heartbeats
                 │  WorkflowScheduler (cron)       │
                 └───────────────┬────────────────┘
                                 │
                   Redis: chronos:scheduler:leader
```

## Components

### 1. LeaderElectionService

- Key `chronos:scheduler:leader`, value `{schedulerId}:{timestamp}`, TTL 30s
- Acquire: `SET NX PX 30000`; renew every 10s with a Lua script that only renews if the value still
  starts with this scheduler's ID; if leadership was lost it tries to re-acquire
- Release on shutdown (`ContextClosedEvent`, before Redis connections close) with a Lua compare-and-delete
- `isLeader()` always reads Redis — no cached local flag
- The scheduler ID is resolved once per process (`SchedulerIdEnvironmentPostProcessor`); default
  `scheduler-{hostname}` in Docker, a random ID otherwise

### 2. ExecutionOrchestrationService

Applies task events to MongoDB. Kafka delivers at least once and the three result topics are consumed
independently, so every update is idempotent and order-tolerant:

- duplicates of an already applied result are ignored
- events for an older attempt (the task was retried or requeued since) are ignored
- a completion or failure that arrives before its `started` event implicitly starts the task
- concurrent updates are detected with `@Version` optimistic locking and retried

It also derives the execution status: `PENDING → RUNNING` when a task leaves PENDING, `COMPLETED` when
all tasks completed, `FAILED` (and unstarted tasks cancelled) when a task failed permanently.

**Retries** are decided here: a failed attempt that is retriable and has attempts left goes back to
`PENDING` with `attemptNumber + 1` and `nextRetryAt = now + initialDelay × multiplier^(attempt−1)`
(capped), using the task's `retryConfig`.

### 3. TaskDispatchService

Dispatches every task that is `PENDING`, not yet dispatched for its current attempt, past `nextRetryAt`
and whose dependencies are `COMPLETED`:

1. **Claim** the attempt with a conditional update (`status = PENDING, attemptNumber = n,
   dispatchedAt = null` → set `dispatchedAt`). Only one caller can win, so consumers and sweeps never
   dispatch the same attempt twice.
2. **Write** the `TaskReady` event (with `attemptNumber`, `maxRetries`, `timeoutMs`) to the outbox,
   keyed by `executionId` so an execution's tasks stay on one partition.

### 4. OutboxPublisherService (leader)

Polls `outbox_messages` every second, oldest first, publishes each to Kafka and marks it published
after the broker acknowledges. Failed sends are retried; after 5 attempts a message is marked `FAILED`
and re-queued by a job every 5 minutes. Published messages are deleted after 7 days.

MongoDB runs standalone (no multi-document transactions), so a crash between claiming an attempt and
writing its outbox message is handled by the stale-dispatch sweep below.

### 5. RestartRecoveryService (leader)

- **Retry sweep** (every 2s): dispatches retries whose `nextRetryAt` has passed
- **Recovery sweep** (every 60s, first run 15s after startup):
  1. releases the claim of attempts dispatched more than 5 minutes ago that no worker started
     (lost event, worker skipped it, or a crash before the outbox write)
  2. dispatches ready tasks of all `PENDING`/`RUNNING` executions (e.g. a lost `WorkflowCreated` event)

### 6. WorkerMonitorService and WorkerFailureHandler

The leader checks every 15s for workers whose `worker:heartbeat:{workerId}` key expired (30s TTL). For
each lost worker — or on a `chronos.worker.unavailable` event — `WorkerFailureHandler`:
1. puts the worker's `RUNNING` tasks back to `PENDING` (same attempt; a lost worker doesn't use up a retry)
2. releases their task locks if still held by that worker (Lua compare-prefix-delete)
3. dispatches them again

### 7. WorkflowScheduler (leader) — cron

- Every 15s: synchronises `scheduler_state` with workflows that have a `schedule` (adds new ones,
  updates changed schedules, removes deleted ones)
- Every 5s: for each due entry (`enabled`, `nextScheduledTime <= now`):
  1. reserves an execution ID for the slot in Redis (`chronos:execution:idempotency:{workflowId}:{slot}`,
     TTL 1h) — a retried or duplicated run of the same slot reuses it
  2. inserts task executions, then the workflow execution (duplicates ignored)
  3. dispatches ready tasks
  4. advances `nextScheduledTime` from the current time (missed slots are skipped) with optimistic locking

Because the state is advanced last and the execution ID reserved first, a crash at any step re-runs the
same slot idempotently.

## Failure Scenarios

### Leader crashes
Its key expires within 30s and a standby acquires leadership on its next renewal attempt (every 10s).
Background jobs resume on the new leader; all their state is in MongoDB/Redis. Event consumption was
never interrupted because every instance consumes.

### Two schedulers think they are leader
Only possible if the old leader pauses beyond the TTL. Everything the leader does is idempotent: attempt
claims are atomic, outbox messages are published at least once and consumers de-duplicate, cron slots
are reserved in Redis and `scheduler_state` updates use optimistic locking.

### Scheduler crashes mid-dispatch
An attempt claimed but not written to the outbox is released by the recovery sweep after the dispatch
timeout and dispatched again.

### Duplicate or reordered task events
Ignored or applied idempotently by `ExecutionOrchestrationService` (see above). Records that keep
failing are sent to `{topic}.dlt` after 3 retries.

## Configuration

```yaml
scheduler:
  id: ${SCHEDULER_ID:scheduler-${random.uuid}}     # resolved once per process

chronos:
  scheduler:
    leader-election:
      ttl: 30s
      renewal-interval: 10000          # ms
    scheduling:
      enabled: true
      interval: 5000                   # cron scan, ms
      sync-interval: 15000             # cron schedule sync, ms
    execution-idempotency:
      ttl: 1h
    retry:                             # defaults when a task has no retryConfig
      initial-delay: 5s
      backoff-multiplier: 2.0
      max-delay: 5m
    restart-recovery:
      enabled: true
      dispatch-timeout: 5m
      retry-sweep-interval: 2000
      sweep-interval: 60000
      initial-delay: 15000
  outbox:
    poll-interval: 1000
    batch-size: 100
    max-attempts: 5
    cleanup-after: 7d
  admin:
    token: ${CHRONOS_ADMIN_TOKEN:}     # enables /api/locks

worker.monitor.interval: 15000         # heartbeat expiry check, ms
```

## Storage Reference

| Store | Key / collection | Content |
|---|---|---|
| Redis | `chronos:scheduler:leader` | `{schedulerId}:{timestamp}`, TTL 30s |
| Redis | `chronos:execution:idempotency:{workflowId}:{slot}` | execution ID of a cron slot, TTL 1h |
| MongoDB | `workflow_executions` | execution status, input/output, owner, `@Version` |
| MongoDB | `task_executions` | task status, attempt, `dispatchedAt`, `nextRetryAt`, worker, `@Version` |
| MongoDB | `outbox_messages` | pending/published/failed TaskReady messages |
| MongoDB | `scheduler_state` | cron schedule, next run, last execution, `@Version` |
| MongoDB | `workflows` | read-only view of workflow definitions (for cron runs) |

## Monitoring

Scheduler metrics (see [observability-architecture.md](observability-architecture.md)):
`chronos_scheduler_leader`, `chronos_outbox_pending`, `chronos_workflow_active`,
`chronos_workflow_executions_total{status}`, `chronos_workflow_duration_seconds`,
`chronos_tasks_total{outcome}`, `chronos_tasks_dispatched_total`.

Operations API (requires `X-Admin-Token`):

```bash
curl -H "X-Admin-Token: $CHRONOS_ADMIN_TOKEN" http://localhost:8082/api/locks/statistics
```

## Testing

- **Unit**: `ExecutionOrchestrationServiceTest` (out-of-order and duplicate events, retries, backoff,
  requeue, metrics), `LeaderElectionServiceTest`, `ExecutionIdempotencyServiceTest`, `WorkerFailureHandlerTest`
- **Integration** (Testcontainers): `SchedulerIntegrationTest` (cron execution, slot idempotency,
  leader-only scheduling, schedule sync), `DistributedSystemsIntegrationTest` (leader election races,
  concurrent scheduling, optimistic locking under contention), `FailureScenarioIntegrationTest`
  (MongoDB/Redis paused and restarted mid-operation)
