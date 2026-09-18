# Kafka Event-Driven Architecture

## Overview

Chronos uses Kafka as the backbone for event-driven communication between services. This document describes the event types, topic design, partitioning strategy, consumer groups, and delivery semantics.

---

## Event Types

### 1. WorkflowCreatedEvent
**Published by:** workflow-service  
**Consumed by:** scheduler-service

Triggered when a user creates a new workflow definition.

**Fields:**
- `eventId` (String): Unique event identifier (UUID)
- `correlationId` (String): Execution ID for tracing
- `timestamp` (Instant): Event creation time
- `workflowId` (String): Workflow identifier
- `ownerId` (String): User who created the workflow
- `workflowName` (String): Human-readable name
- `tasks` (List<TaskDefinition>): Task definitions with dependencies

**Purpose:** Initiates workflow execution planning in the scheduler.

---

### 2. TaskReadyEvent
**Published by:** scheduler-service  
**Consumed by:** worker-service

Triggered when a task is ready to execute (dependencies satisfied).

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Workflow ID for tracing
- `timestamp` (Instant): Event creation time
- `workflowId` (String): Workflow identifier
- `executionId` (String): Execution instance identifier
- `taskId` (String): Task identifier within workflow
- `taskType` (String): Task type for worker matching
- `configuration` (Map<String, Object>): Task-specific config
- `timeoutMs` (Long): Maximum execution time of this attempt (enforced by the worker)
- `maxRetries` (Integer): Maximum number of attempts
- `attemptNumber` (Integer): Attempt being dispatched (1-based); results for older attempts are ignored

**Purpose:** Signals workers that a task attempt is available for execution. Each attempt is dispatched
once (atomic claim in MongoDB) through the transactional outbox.

---

### 3. TaskStartedEvent
**Published by:** worker-service  
**Consumed by:** scheduler-service (for tracking)

Triggered when a worker begins executing a task.

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Workflow ID for tracing
- `timestamp` (Instant): Event creation time
- `workflowId` (String): Workflow identifier
- `executionId` (String): Execution instance identifier
- `taskId` (String): Task identifier
- `workerId` (String): Worker executing the task
- `attemptNumber` (Integer): Retry attempt number (1-based)

**Purpose:** Tracks task execution start for monitoring and timeout detection.

---

### 4. TaskCompletedEvent
**Published by:** worker-service  
**Consumed by:** scheduler-service

Triggered when a task completes successfully.

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Workflow ID for tracing
- `timestamp` (Instant): Event creation time
- `workflowId` (String): Workflow identifier
- `executionId` (String): Execution instance identifier
- `taskId` (String): Task identifier
- `workerId` (String): Worker that executed the task
- `attemptNumber` (Integer): Retry attempt number
- `result` (Map<String, Object>): Task execution result
- `durationMs` (Long): Execution duration in milliseconds

**Purpose:** Updates execution state and triggers dependent tasks.

---

### 5. TaskFailedEvent
**Published by:** worker-service  
**Consumed by:** scheduler-service

Triggered when a task execution fails.

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Workflow ID for tracing
- `timestamp` (Instant): Event creation time
- `workflowId` (String): Workflow identifier
- `executionId` (String): Execution instance identifier
- `taskId` (String): Task identifier
- `workerId` (String): Worker that attempted the task
- `attemptNumber` (Integer): Retry attempt number
- `errorMessage` (String): Error description
- `errorType` (String): Error class name
- `retriable` (Boolean): Whether task can be retried
- `durationMs` (Long): Execution duration before failure

**Purpose:** Handles task failures, determines retry logic or workflow failure.

---

### 6. WorkerRegisteredEvent
**Published by:** worker-service  
**Consumed by:** scheduler-service

Triggered when a worker registers itself on startup.

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Worker ID for tracing
- `timestamp` (Instant): Event creation time
- `workerId` (String): Unique worker identifier
- `supportedTaskTypes` (List<String>): Task types worker can handle
- `workerHostname` (String): Worker hostname
- `workerPort` (Integer): Worker port

**Purpose:** Maintains available worker registry for task assignment.

---

### 7. WorkerUnavailableEvent
**Published by:** worker-service  
**Consumed by:** scheduler-service

Triggered when a worker shuts down or becomes unresponsive.

**Fields:**
- `eventId` (String): Unique event identifier
- `correlationId` (String): Worker ID for tracing
- `timestamp` (Instant): Event creation time
- `workerId` (String): Worker identifier
- `reason` (String): Unavailability reason (shutdown, timeout, crash)

**Purpose:** Published by a worker on graceful shutdown; the scheduler requeues any tasks still RUNNING on it.

---

## Topic Design

### Topic Naming Convention
`chronos.<domain>.<event-type>`

### Main Topics

Topics are auto-created by the broker. In the local Docker stack every topic has **6 partitions,
replication factor 1 and 24h retention** (`KAFKA_NUM_PARTITIONS`, `KAFKA_LOG_RETENTION_HOURS` in
`docker-compose.yml`). The last column is a suggested production setting.

| Topic Name | Producer → Consumer | Key | Suggested production retention |
|------------|---------------------|-----|------------------|
| `chronos.workflow.created` | workflow service → scheduler | executionId | 7 days |
| `chronos.task.ready` | scheduler (outbox) → workers | executionId | 3 days |
| `chronos.task.started` | worker → scheduler | executionId | 3 days |
| `chronos.task.completed` | worker → scheduler | executionId | 7 days |
| `chronos.task.failed` | worker → scheduler | executionId | 30 days |
| `chronos.task.failed.permanently` | worker → (operators) | executionId | 30 days — the task dead letter queue |
| `chronos.worker.registered` | worker → scheduler | workerId | 1 day |
| `chronos.worker.unavailable` | worker → scheduler | workerId | 7 days |

In production use replication factor 3 and `min.insync.replicas=2` (producers already use `acks=all`
and idempotence).

### Dead Letter Topics (DLT)

When a consumer keeps failing on a record (3 retries, 1s apart) or cannot deserialize it, the record is
published to **`<original topic>.dlt`** with the exception in its headers
(`kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace`), and the partition moves on. For example
`chronos.task.ready.dlt` (worker side) or `chronos.task.completed.dlt` (scheduler side).

A dead-lettered `TaskReady` leaves its task dispatched but not started; the scheduler's stale-dispatch
sweep re-dispatches it after the dispatch timeout (5 minutes).

---

## Partitioning Strategy

### Message Keys

**Consistent partitioning ensures:**
1. Events for the same entity are processed in order
2. Load is distributed evenly across partitions
3. Consumer affinity (same consumer handles related events)

| Event Type | Message Key | Rationale |
|------------|-------------|-----------|
| WorkflowCreatedEvent | `workflowId` | All events for a workflow go to same partition |
| TaskReadyEvent | `executionId` | Tasks from same execution maintain order |
| TaskStartedEvent | `executionId` | Task lifecycle events stay together |
| TaskCompletedEvent | `executionId` | Task lifecycle events stay together |
| TaskFailedEvent | `executionId` | Task lifecycle events stay together |
| WorkerRegisteredEvent | `workerId` | Worker events stay together |
| WorkerUnavailableEvent | `workerId` | Worker events stay together |

### Partition Count Selection

- **Local stack:** every topic is auto-created with 6 partitions
- **Production suggestion:** workflow topics 6 partitions (lower volume), task topics 12 (task-level parallelism)
- **Worker topics (3 partitions):** Lower volume, limited by worker count

**Scaling considerations:**
- Can increase partitions as load grows
- Cannot decrease without recreating topic
- Choose based on peak throughput estimates

---

## Consumer Groups

### scheduler-group
**Service:** scheduler-service  
**Consumes:**
- `chronos.workflow.created`
- `chronos.task.started`
- `chronos.task.completed`
- `chronos.task.failed`
- `chronos.worker.registered`
- `chronos.worker.unavailable`

**Concurrency:** 3 consumer threads per task/workflow topic, 2 per worker topic  
**Purpose:** All scheduler instances share the work through the group; handlers are idempotent and
use optimistic locking, so any instance can process any event (only background jobs are leader-only)

**Configuration:**
- `group.id`: `scheduler-group`
- `auto.offset.reset`: `earliest`
- `enable.auto.commit`: `false` (manual acknowledgment)
- `max.poll.records`: `100`
- `max.poll.interval.ms`: `300000` (5 minutes)

---

### worker-group
**Service:** worker-service  
**Consumes:**
- `chronos.task.ready`

**Concurrency:** 4 consumers per worker instance  
**Purpose:** Multiple workers compete for available tasks

**Configuration:**
- `group.id`: `worker-group`
- `auto.offset.reset`: `earliest`
- `enable.auto.commit`: `false` (manual acknowledgment)
- `max.poll.records`: `10` (limited to prevent overload)
- `max.poll.interval.ms`: `600000` (10 minutes for long tasks)

**Task Assignment:**
- Workers filter by supported task types
- First worker to acquire distributed lock executes task
- Idempotency prevents duplicate execution

---

## Delivery Semantics

### At-Least-Once Delivery

**Implementation:**
- Producers: `enable.idempotence=true`, `acks=all`, `retries=3`
- Consumers: Manual acknowledgment after processing
- Messages may be delivered multiple times

**Idempotency Strategy:**
1. **Event-level:** Check `eventId` in Redis/MongoDB before processing
2. **Entity-level:** Check entity state (e.g., task already completed)
3. **Deduplicate window:** 24-hour TTL on processed eventIds

**Example (Worker):**
```java
if (processedEventIds.contains(event.getEventId())) {
    log.info("Event already processed (idempotent skip)");
    acknowledgment.acknowledge();
    return;
}
// Process event
processedEventIds.add(event.getEventId());
acknowledgment.acknowledge();
```

---

### Retry Behavior

**Producer retries:**
- Automatic: 3 retries with 1s backoff
- Failure: Logged, returned to caller

**Consumer retries:**
- Don't acknowledge on processing failure
- Kafka redelivers after commit timeout
- After multiple redeliveries, message goes to DLT
- DLT messages require manual intervention

**Retry limits (recommended):**
- Consumer: 3-5 redeliveries
- DLT: Manual replay after fix

---

## Error Handling

### Malformed Messages

**Protection:** `ErrorHandlingDeserializer` wraps `JsonDeserializer`

**Behavior:**
1. Deserialization fails → error logged, null value passed
2. Consumer checks for null, logs error, acknowledges
3. Message does not block partition processing

### Processing Errors

**Strategy:**
1. **Transient errors** (network, DB timeout): Don't acknowledge, retry
2. **Permanent errors** (validation, logic): Log, send to DLT, acknowledge
3. **Unknown errors**: Don't acknowledge, retry up to limit

### Dead Letter Queue Flow

```
Message → Consumer → Processing Error
                ↓
         Retry (3-5 times)
                ↓
         Still Failing?
                ↓
         Send to DLT → Manual Review
```

---

## Monitoring & Observability

### Key Metrics

**Producer metrics:**
- `kafka.producer.record-send-rate`: Messages/sec sent
- `kafka.producer.record-error-rate`: Failed sends/sec
- `kafka.producer.request-latency-avg`: Send latency

**Consumer metrics:**
- `kafka.consumer.records-consumed-rate`: Messages/sec consumed
- `kafka.consumer.records-lag`: Unconsumed messages
- `kafka.consumer.fetch-latency-avg`: Fetch latency

**Application metrics:**
- Event processing duration (by event type)
- Idempotent skips (duplicate events detected)
- DLT message count
- Task execution success/failure rates

### Logging

All events log:
- `eventId`, `correlationId`, `timestamp`
- Partition and offset (for troubleshooting)
- Processing success/failure
- Duration (for performance tracking)

**Example:**
```
Received TaskReadyEvent: executionId=abc-123, taskId=task1, partition=5, offset=1234
Successfully processed TaskReadyEvent: executionId=abc-123, taskId=task1
```

---

## Operational Procedures

### Topic Creation

```bash
# Create workflow topic
kafka-topics.sh --create \
  --topic chronos.workflow.created \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000

# Create task topic
kafka-topics.sh --create \
  --topic chronos.task.ready \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=259200000
```

### Consumer Lag Monitoring

```bash
# Check consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group scheduler-group

# Expected output shows partition, offset, lag
```

### DLT Message Replay

```bash
# 1. Fix the issue causing failures
# 2. Consume DLT messages
# 3. Re-publish to main topic for reprocessing
```

---

## Future Enhancements

### Planned Improvements

1. **Exactly-Once Semantics**
   - Use Kafka transactions
   - Requires idempotent producers + transactional consumers
   - Higher complexity, better guarantees

2. **Schema Registry**
   - Use Avro/Protobuf for event schemas
   - Version management and compatibility checks
   - Better backward/forward compatibility

3. **Distributed Tracing**
   - Propagate traceId via Kafka headers
   - End-to-end workflow execution tracing
   - Integration with Zipkin/Jaeger

4. **Event Replay**
   - Store events in long-term storage (S3)
   - Replay capability for debugging/recovery
   - Time-travel queries for workflow history

5. **Consumer Auto-scaling**
   - Scale workers based on `chronos.task.ready` lag
   - Dynamic partition assignment
   - Kubernetes HPA integration

---

## References

- Kafka Producer Config: https://kafka.apache.org/documentation/#producerconfigs
- Kafka Consumer Config: https://kafka.apache.org/documentation/#consumerconfigs
- Spring Kafka Documentation: https://docs.spring.io/spring-kafka/reference/html/
- Chronos Architecture: `docs/architecture.md`
- Failure Scenarios: `docs/failure-recovery-guarantees.md`
