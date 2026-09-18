# Chronos — Distributed Workflow Orchestration Engine

> Define workflows as DAGs of tasks; Chronos runs them across a pool of workers with retries,
> timeouts, crash recovery and cron scheduling.

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-green.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Overview

Chronos is an event-driven workflow engine built as four Spring Boot services communicating through
Kafka, with MongoDB for state and Redis for coordination. It was built to explore the problems real
orchestrators (Temporal, Airflow, Step Functions) have to solve:

- **At-least-once execution** with idempotent consumers, a transactional outbox and atomic per-attempt dispatch
- **Leader election** so exactly one scheduler runs background work, with fast failover
- **Fault tolerance**: retries with exponential backoff, task timeouts, dead-letter queues, and
  requeueing of tasks whose worker crashed
- **Horizontal scaling** of workers, with distributed locks preventing duplicate concurrent execution
- **Observability**: Prometheus metrics, a Grafana dashboard and alert rules

> **Task execution is simulated.** Workers sleep for a configurable time and return a result; the
> orchestration around it (dispatch, retries, timeouts, recovery, scheduling) is real. Task
> configuration can simulate failures to exercise it — see [Try the failure handling](#-try-the-failure-handling).

---

## 🏗️ Architecture

```
                 Client (REST + JWT)
                        │
                ┌───────▼───────┐
                │  API Gateway  │  JWT validation, rate limiting, correlation IDs
                │     :8080     │
                └───────┬───────┘
                        │ HTTP
                ┌───────▼───────┐        ┌───────────┐
                │   Workflow    │───────▶│  MongoDB  │◀──────────────────┐
                │   Service     │        └───────────┘                   │
                │     :8081     │  users, workflows, executions, tasks   │
                └───────┬───────┘                                        │
                        │ chronos.workflow.created                       │
                ┌───────▼─────────────────────────── Kafka ─────────┐    │
                │                                                   │    │
        ┌───────▼───────┐   chronos.task.ready   ┌───────────────┐  │    │
        │   Scheduler   │───────────────────────▶│ Worker × N    │  │    │
        │   Service     │◀───────────────────────│  :8083+       │  │    │
        │     :8082     │ task.started/completed │               │  │    │
        └───┬───────┬───┘ /failed, worker events └──────┬────────┘  │    │
            │       └───────────────────────────────────┼───────────┘    │
            │                                           │                │
            │            ┌─────────┐                    │                │
            └───────────▶│  Redis  │◀───────────────────┘                │
             leader lock │         │ task locks, leases, heartbeats      │
                         └─────────┘                                     │
            └────────────────────────────────────────────────────────────┘
               scheduler reads/updates execution state

        Prometheus :9090 scrapes all services  ──▶  Grafana :3000
```

| Service | Port | Responsibilities |
|---|---|---|
| **API Gateway** | 8080 | Validates JWTs, routes `/api/**` to the workflow service, per-client rate limiting, correlation IDs |
| **Workflow Service** | 8081 | Users and JWT issuing, workflow definitions (DAG + cron validation), starting/inspecting/cancelling executions |
| **Scheduler Service** | 8082 | Applies task events to execution state, dispatches ready tasks, retries, recovery sweeps, cron scheduling (leader-elected) |
| **Worker Service** | 8083+ | Claims tasks with Redis locks, executes them with timeouts, heartbeats, reports results; scale with `--scale` |

### How an execution runs

1. `POST /workflows/{id}/execute` stores the execution and its task executions in MongoDB and publishes
   `chronos.workflow.created`.
2. The scheduler finds tasks whose dependencies are complete, **claims each attempt atomically** and writes a
   `TaskReady` event to the **outbox**; the leader publishes it to `chronos.task.ready`.
3. A worker takes a Redis lock on `{executionId}:{taskId}`, runs the task under its `timeoutMs`, and publishes
   `chronos.task.started` / `completed` / `failed`.
4. The scheduler records the result (ignoring duplicates and results of old attempts), dispatches newly
   ready tasks, and completes or fails the execution.
5. A failed attempt is retried with exponential backoff while attempts remain; otherwise the execution fails
   and the task goes to the dead-letter topic.
6. If a worker dies, its heartbeat expires; the leader requeues its running tasks, releases their locks and
   dispatches them to other workers.

---

## ✨ Features

- **Workflows as DAGs**: dependencies, parallel fan-out/fan-in, validation (unique IDs, unknown dependencies, cycles)
- **Execution control**: start with input, inspect execution/task state and statistics, cancel
- **Retries**: per-task `retryConfig` (max attempts, exponential backoff with cap); non-retriable errors fail fast
- **Timeouts**: per-task `timeoutMs`; attempts exceeding it are aborted and retried
- **Cron scheduling**: optional `schedule` + `timezone` per workflow, idempotent per scheduled slot
- **Crash recovery**: worker heartbeats; lost workers' tasks are requeued within ~45s
- **Exactly-once dispatch per attempt**: atomic claims + transactional outbox; stale dispatches re-sent by a sweep
- **Leader election**: Redis with Lua compare-and-renew / compare-and-delete
- **Dead-letter queues**: permanently failed tasks and unprocessable Kafka records
- **Security**: JWT auth, per-user isolation of workflows and executions, admin-token protected ops API
- **Observability**: custom metrics, provisioned Grafana dashboard, 7 Prometheus alert rules
- **Graceful shutdown**: workers finish in-flight tasks before leaving; scheduler releases leadership

---

## 🛠️ Technology Stack

| Area | Technologies |
|---|---|
| Language & framework | Java 21, Spring Boot 3.2 (Web, Security, Data MongoDB, Data Redis, Kafka, Actuator) |
| Messaging & storage | Apache Kafka, MongoDB 7, Redis 7 |
| Resilience | Resilience4j circuit breakers, Redis locks/leases, transactional outbox |
| Observability | Micrometer, Prometheus, Grafana |
| Testing | JUnit 5, Mockito, Testcontainers, Newman (Postman) |
| Packaging | Docker (multi-stage builds), Docker Compose |

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose) — enough to run everything
- **Java 21+ and Maven 3.9+** — only to build or test outside Docker

### 1. Start the stack

```bash
git clone https://github.com/mainak569/chronos-workflow-engine.git
cd chronos-workflow-engine
cp .env.example .env            # change JWT_SECRET for anything beyond local use
docker compose up -d --build --scale worker-service=3
```

The first build compiles all services inside Docker and takes a few minutes. Wait until every
container reports `healthy`:

```bash
docker compose ps
curl http://localhost:8080/readyz     # {"status":"UP"}
```

### 2. Run a workflow

```bash
# Register (returns a JWT)
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"jane@example.com","username":"jane","password":"SecurePass123!"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')

# (Already registered? Use /api/v1/auth/login with {"email": ..., "password": ...} instead.)

# Create a workflow: resize -> compress -> validate
WORKFLOW=$(curl -s -X POST http://localhost:8080/api/v1/workflows \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"Image pipeline","tasks":[
        {"taskId":"resize","name":"Resize","taskType":"IMAGE_RESIZE"},
        {"taskId":"compress","name":"Compress","taskType":"IMAGE_COMPRESS","dependencies":["resize"]},
        {"taskId":"validate","name":"Validate","taskType":"DATA_VALIDATION","dependencies":["compress"]}]}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["workflowId"])')

# Execute it
EXECUTION=$(curl -s -X POST http://localhost:8080/api/v1/workflows/$WORKFLOW/execute \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["executionId"])')

# A few seconds later: status COMPLETED, with each task's output
curl -s http://localhost:8080/api/v1/workflows/executions/$EXECUTION -H "Authorization: Bearer $TOKEN"
curl -s http://localhost:8080/api/v1/workflows/executions/$EXECUTION/tasks -H "Authorization: Bearer $TOKEN"
```

Or run the whole API tour with the Postman collection (also importable into Postman):

```bash
npx newman run postman/chronos-api.postman_collection.json
```

### 3. Watch it

- **Grafana**: http://localhost:3000/d/chronos-overview (admin / admin)
- **Prometheus**: http://localhost:9090 (alerts at `/alerts`)

### Stop

```bash
docker compose down          # keep data
docker compose down -v       # also delete volumes
```

---

## 🧪 Try the failure handling

Task `configuration` controls the simulated work:

| Key | Effect |
|---|---|
| `simulateDurationMs` | how long an attempt runs (default 1000) |
| `failUntilAttempt` | fail with a retriable error while `attempt <= value` |
| `failPermanently` | fail with a non-retriable error |

```bash
# Fails on attempt 1, succeeds on attempt 2 after a 1s backoff
curl -s -X POST http://localhost:8080/api/v1/workflows -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"name":"flaky","tasks":[{"taskId":"t","name":"T",
  "taskType":"DATA_PROCESSING","configuration":{"failUntilAttempt":1},
  "retryConfig":{"maxAttempts":3,"initialDelayMs":1000}}]}'

# Times out after 1s on every attempt, then fails
... "configuration":{"simulateDurationMs":5000}, "timeoutMs":1000 ...
```

**Kill a worker mid-task**: start a workflow with a long task (`"simulateDurationMs": 30000`), find the
worker running it (`workerId` in `/tasks` is `worker-<container id>`), and `docker kill <container id>`.
Within ~45s the scheduler requeues the task and another worker completes it.

---

## 📂 Project Structure

```
chronos/
├── services/
│   ├── api-gateway/         # JWT validation, routing, rate limiting
│   ├── workflow-service/    # REST API, users, workflows, executions
│   ├── scheduler-service/   # orchestration, dispatch, retries, recovery, cron, leader election
│   └── worker-service/      # task execution, locks/leases, heartbeats
├── infrastructure/
│   ├── mongodb/             # init script
│   ├── prometheus/          # scrape config + alert rules
│   └── grafana/             # provisioned datasource + Chronos Overview dashboard
├── postman/                 # API collection with tests (runs with Newman)
├── docs/                    # architecture, design decisions, API reference, operations
├── scripts/                 # build, start/stop, health check, logs helpers
├── docker-compose.yml
├── Makefile                 # shortcuts: make up, make test, make health, ...
└── pom.xml                  # Maven aggregator for all services
```

---

## ⚙️ Configuration

Set in `.env` (see `.env.example`); the most relevant:

| Variable | Default | Purpose |
|---|---|---|
| `JWT_SECRET` | dev value | HMAC key shared by the gateway and workflow service (≥ 32 chars) |
| `JWT_EXPIRATION_MS` | `3600000` | Token lifetime |
| `CHRONOS_ADMIN_TOKEN` | empty | Enables the scheduler ops API (`/api/locks`, header `X-Admin-Token`) |
| `GATEWAY_RATE_LIMIT_PER_MINUTE` | `120` | Requests per user (or IP) per minute |
| `MONGODB_ROOT_USERNAME` / `_PASSWORD` | `chronos` / `chronos123` | MongoDB credentials |
| `GRAFANA_ADMIN_USER` / `_PASSWORD` | `admin` / `admin` | Grafana login |

Service-level settings (retry defaults, recovery intervals, heartbeat TTLs, ...) are listed in
[docs/README.md](docs/README.md#configuration-defaults).

---

## 💻 Development

Run the infrastructure in Docker and the services from your IDE or Maven:

```bash
docker compose up -d mongodb zookeeper kafka redis prometheus grafana
mvn clean package -DskipTests                 # from the repository root
cd services/workflow-service && mvn spring-boot:run   # likewise scheduler, worker, api-gateway
```

Useful shortcuts: `make help`, `make health`, `make logs`, `make scale-workers N=5`, `make kafka-topics`.

---

## ✅ Testing

```bash
mvn test        # from the repository root; Docker must be running for Testcontainers
```

235 automated tests across the four services, including integration tests that run against real MongoDB,
Kafka and Redis containers — for example pausing and restarting MongoDB/Redis mid-operation, concurrent
leader election, and concurrent scheduling of the same workflow. The Postman collection
(`npx newman run postman/chronos-api.postman_collection.json`) exercises the full API against a running stack.

---

## 📚 Documentation

| Document | Contents |
|---|---|
| [docs/README.md](docs/README.md) | Overview, guarantees, configuration defaults, troubleshooting |
| [API reference](docs/api/workflow-api.md) | Endpoints with real request/response examples, error codes |
| [Architecture](docs/architecture.md) | Services, data model, Kafka topics, Redis keys |
| [Design decisions](docs/design-decisions.md) | Trade-offs behind the main choices |
| [Failure recovery](docs/failure-recovery-guarantees.md) | Failure scenarios and recovery behaviour |
| [Observability](docs/observability-architecture.md) | Metrics catalog, dashboard, alerts |
| [PROJECT_SPEC.md](PROJECT_SPEC.md) | The original specification the project was built from |

---

## 🧭 Limitations

- Task execution is simulated (no real image/data processing)
- No distributed tracing backend (correlation IDs only)
- Rate limiting is per gateway instance (in memory)
- All workers share one consumer group, so every worker must support every task type in use
- MongoDB runs standalone, so multi-document updates are made crash-safe by ordering and recovery sweeps rather than transactions

---

## 📄 License

MIT — see [LICENSE](LICENSE).

## 🙏 Acknowledgments

Inspired by [Temporal](https://temporal.io/), [Apache Airflow](https://airflow.apache.org/),
[AWS Step Functions](https://aws.amazon.com/step-functions/) and [Celery](https://docs.celeryq.dev/).
