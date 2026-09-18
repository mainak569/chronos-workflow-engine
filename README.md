# Chronos — Distributed Workflow Orchestration Engine

> Define workflows as DAGs of tasks; Chronos runs them across a pool of workers with retries,
> timeouts, crash recovery and cron scheduling.

![Java](https://img.shields.io/badge/Java_21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot_3.2-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
![Spring Security](https://img.shields.io/badge/Spring_Security-6DB33F?style=for-the-badge&logo=springsecurity&logoColor=white)
![JWT](https://img.shields.io/badge/JWT-000000?style=for-the-badge&logo=jsonwebtokens&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![ZooKeeper](https://img.shields.io/badge/ZooKeeper-D22128?style=for-the-badge&logo=apache&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Lua](https://img.shields.io/badge/Lua-2C2D72?style=for-the-badge&logo=lua&logoColor=white)
![Resilience4j](https://img.shields.io/badge/Resilience4j-4B6BFB?style=for-the-badge)
![Micrometer](https://img.shields.io/badge/Micrometer-1A73E8?style=for-the-badge)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apachemaven&logoColor=white)
![JUnit 5](https://img.shields.io/badge/JUnit_5-25A162?style=for-the-badge&logo=junit5&logoColor=white)
![Mockito](https://img.shields.io/badge/Mockito-78A641?style=for-the-badge)
![Testcontainers](https://img.shields.io/badge/Testcontainers-291A3F?style=for-the-badge)
![Postman](https://img.shields.io/badge/Postman-FF6C37?style=for-the-badge&logo=postman&logoColor=white)

---

## Overview

Four Spring Boot services communicate through Kafka, with MongoDB for state and Redis for
coordination. The project tackles the core problems of real orchestrators (Temporal, Airflow,
Step Functions):

- **At-least-once execution**: idempotent consumers, transactional outbox, atomic per-attempt dispatch
- **Leader election** (Redis + Lua) so exactly one scheduler runs background work
- **Fault tolerance**: exponential-backoff retries, timeouts, dead-letter queues, crash recovery
- **Horizontal scaling** of workers, with distributed locks against duplicate execution
- **Observability**: Prometheus metrics, a Grafana dashboard and alert rules

> **Task execution is simulated**: workers sleep and return a result. Everything around it
> (dispatch, retries, timeouts, recovery, scheduling) is real.

## 🏗️ Architecture

```
 Client ──▶ API Gateway :8080 ──▶ Workflow Service :8081 ──▶ MongoDB
            (JWT, rate limit)            │
                                         │ chronos.workflow.created
                                         ▼
                               ──────▶ Kafka ──────▶
           chronos.task.ready ▲                     │ task.started / completed / failed
                              │                     ▼
                      Scheduler :8082 ◀──────── Workers :8083+ (× N)
                          (leader-elected)               │
                                 └──────── Redis ────────┘
                            leader lock · task locks · heartbeats

                  Prometheus :9090 ──▶ Grafana :3000
```

| Service | Responsibilities |
|---|---|
| **API Gateway** | JWT validation, routing, per-client rate limiting, correlation IDs |
| **Workflow Service** | Users & JWTs, workflow definitions (DAG + cron validation), start / inspect / cancel executions |
| **Scheduler Service** | Applies task results, dispatches ready tasks via outbox, retries, recovery sweeps, cron |
| **Worker Service** | Claims tasks with Redis locks, runs them with timeouts, heartbeats, reports results |

**Execution flow:** the workflow service stores the execution and publishes an event → the scheduler
claims each ready task attempt and publishes `TaskReady` through the outbox → a worker locks and runs it
→ the scheduler records the result and dispatches the next tasks. Failed attempts are retried with
backoff; if a worker dies, its heartbeat expires and its tasks are requeued to other workers.

## ✨ Features

- DAG workflows with parallel fan-out/fan-in and validation (unique IDs, missing dependencies, cycles)
- Per-task retries (exponential backoff) and timeouts; non-retriable errors fail fast
- Cron scheduling per workflow, idempotent per scheduled slot
- Crash recovery: a dead worker's tasks move to other workers within about a minute; stale dispatches are re-sent automatically
- JWT auth with per-user isolation of workflows and executions
- Graceful shutdown: workers finish in-flight tasks, the scheduler releases leadership
- 235 automated tests, including Testcontainers integration tests against real MongoDB, Kafka and Redis

## 🚀 Quick Start

Requires **Docker**. Java 21 and Maven 3.9 are only needed to build or test outside Docker.

```bash
git clone https://github.com/mainak569/chronos-workflow-engine.git
cd chronos-workflow-engine
cp .env.example .env
docker compose up -d --build --scale worker-service=3   # first build takes a few minutes
curl http://localhost:8080/readyz                         # {"status":"UP"} once healthy
```

Run a workflow:

```bash
# Register (use /api/v1/auth/login with email + password if already registered)
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"jane@example.com","username":"jane","password":"SecurePass123!"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')

# Create a workflow: resize -> compress -> validate
WORKFLOW=$(curl -s -X POST http://localhost:8080/api/v1/workflows \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"Image pipeline","tasks":[
        {"taskId":"resize","name":"Resize","taskType":"IMAGE_RESIZE"},
        {"taskId":"compress","name":"Compress","taskType":"IMAGE_COMPRESS","dependencies":["resize"]},
        {"taskId":"validate","name":"Validate","taskType":"DATA_VALIDATION","dependencies":["compress"]}]}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["workflowId"])')

# Execute it, then check the result a few seconds later (status COMPLETED)
EXECUTION=$(curl -s -X POST http://localhost:8080/api/v1/workflows/$WORKFLOW/execute \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["executionId"])')
sleep 5
curl -s http://localhost:8080/api/v1/workflows/executions/$EXECUTION/tasks -H "Authorization: Bearer $TOKEN"
```

Or run the full API tour: `npx newman run postman/chronos-api.postman_collection.json`

**Dashboards:** Grafana at http://localhost:3000/d/chronos-overview (admin / admin), Prometheus at http://localhost:9090.

**Try failures:** add `"configuration": {"failUntilAttempt": 1}` to a task to see a retry, or
`"simulateDurationMs": 5000` with `"timeoutMs": 1000` to see a timeout. Kill a worker container
mid-task (`docker kill <id>`) and another worker picks the task up.

## 🟢 Testing

```bash
mvn test    # from the repository root; Docker must be running for Testcontainers
```

## 📚 Documentation

| | |
|---|---|
| [API reference](docs/api/workflow-api.md) | Endpoints, real request/response examples, error codes |
| [Architecture](docs/architecture.md) | Services, data model, Kafka topics, Redis keys |
| [Design decisions](docs/design-decisions.md) | Trade-offs behind the main choices |
| [Failure recovery](docs/failure-recovery-guarantees.md) | Failure scenarios and recovery behaviour |
| [Observability](docs/observability-architecture.md) | Metrics, dashboard, alerts |
| [Local development](docs/local-development.md) | Running services outside Docker, configuration, troubleshooting |

## 🧭 Limitations

- Task execution is simulated
- No distributed tracing backend (correlation IDs only)
- Rate limiting is in-memory per gateway instance
- Every worker must support every task type in use (single consumer group)
