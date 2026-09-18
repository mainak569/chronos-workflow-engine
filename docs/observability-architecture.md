# Observability

Chronos exposes Prometheus metrics from every service, ships a provisioned Grafana dashboard and a
set of Prometheus alert rules. Everything here is part of the root `docker-compose.yml`.

| Component | URL | Notes |
|---|---|---|
| Grafana | http://localhost:3000/d/chronos-overview | `admin` / `admin` (from `.env`) |
| Prometheus | http://localhost:9090 | Targets: `/targets`, alerts: `/alerts` |
| Service metrics | `http://<service>:908x/actuator/prometheus` | Management ports, inside the Docker network |
| Readiness | `http://localhost:808x/readyz` | Application ports, reachable from the host |

## How Metrics Are Collected

Each Spring Boot service exposes Micrometer metrics at `/actuator/prometheus`. In the `docker`
profile actuator runs on a separate management port (gateway 9080, workflow 9081, scheduler 9082,
worker 9083), which is not published to the host. Prometheus scrapes these ports every 15s
(`infrastructure/prometheus/prometheus.yml`); worker replicas are discovered through Docker DNS
(`dns_sd_configs` on `worker-service`), so scaling workers needs no configuration change.

To look at raw metrics of one service:

```bash
docker compose exec workflow-service wget -qO- localhost:9081/actuator/prometheus | grep chronos_
```

## Metrics Catalog

All Chronos metrics use low-cardinality labels only (no workflow, execution or user IDs), so the
number of time series stays constant regardless of load.

### Workflows

| Metric | Type | Labels | Emitted by | Meaning |
|---|---|---|---|---|
| `chronos_workflow_executions_total` | counter | `status` | scheduler (COMPLETED, FAILED), workflow (CANCELLED) | Executions that reached a final status |
| `chronos_workflow_duration_seconds` | histogram | | scheduler | Start-to-finish duration of finished executions |
| `chronos_workflow_active` | gauge | | scheduler | Executions currently PENDING or RUNNING |
| `chronos_workflow_executions_started_total` | counter | | workflow | Executions started through the API |
| `chronos_workflows_created_total` / `_deleted_total` | counter | | workflow | Workflow definitions created / deleted |

### Tasks and Workers

| Metric | Type | Labels | Emitted by | Meaning |
|---|---|---|---|---|
| `chronos_tasks_total` | counter | `outcome` = completed, failed, retried, requeued | scheduler | Task attempt outcomes (`retried`: failed with attempts left; `requeued`: worker lost) |
| `chronos_tasks_dispatched_total` | counter | | scheduler | Task attempts dispatched to workers |
| `chronos_task_execution_seconds` | histogram | `task_type`, `outcome` = success, failure | worker | Execution time of each attempt on a worker |
| `chronos_worker_tasks_inflight` | gauge | | worker | Tasks currently executing on the worker |
| `chronos_task_dlq_total` | counter | | worker | Tasks dead-lettered after their final attempt |

### Scheduler

| Metric | Type | Meaning |
|---|---|---|
| `chronos_scheduler_leader` | gauge | 1 on the instance holding leadership, else 0 |
| `chronos_outbox_pending` | gauge | Outbox messages not yet published to Kafka |

Gauges that need a database query are refreshed every 15s in the background
(`chronos.metrics.gauge-refresh-interval`), so a scrape never hits MongoDB.

### Standard Metrics

Spring Boot and Micrometer also export, among others:
- `http_server_requests_seconds` (request rate and latency per endpoint)
- `jvm_memory_used_bytes`, `jvm_gc_pause_seconds`, `process_cpu_usage`
- `kafka_consumer_fetch_manager_records_lag_max` and other Kafka client metrics (scheduler and worker
  consumers register a Micrometer listener)
- `resilience4j_circuitbreaker_state` for the MongoDB circuit breakers
- `mongodb_driver_pool_*` connection pool metrics

## Grafana Dashboard

`infrastructure/grafana/dashboards/chronos-overview.json` is provisioned automatically
(`infrastructure/grafana/provisioning/`), together with the Prometheus datasource (uid `prometheus`).

| Row | Panels |
|---|---|
| Health | Services up, workers up, scheduler leader, active executions, tasks in flight, outbox backlog |
| Workflows | Executions finished/min by status, success rate, duration p50/p95/p99, API activity |
| Tasks | Outcomes/min and dispatch rate, execution time p95 by task type, attempts on workers, dead-lettered tasks |
| Kafka & services | Consumer lag, HTTP requests/s, HTTP p95 latency, JVM heap |

Useful queries:

```promql
# Workflow success rate over 5 minutes
100 * sum(rate(chronos_workflow_executions_total{status="COMPLETED"}[5m]))
    / sum(rate(chronos_workflow_executions_total{status=~"COMPLETED|FAILED"}[5m]))

# p95 workflow duration
histogram_quantile(0.95, sum by (le) (rate(chronos_workflow_duration_seconds_bucket[5m])))

# Retries per minute
sum(rate(chronos_tasks_total{outcome="retried"}[5m])) * 60
```

## Alerts

Rules live in `infrastructure/prometheus/alerts.yml` and are loaded through `rule_files` in
`prometheus.yml`. Firing alerts are listed at http://localhost:9090/alerts (no Alertmanager is
deployed, so nothing is sent anywhere).

| Alert | Condition | Severity |
|---|---|---|
| ServiceDown | a service target is down for 1m | critical |
| NoWorkersAvailable | no worker target up for 1m | critical |
| NoSchedulerLeader | no scheduler holds leadership for 2m | critical |
| HighWorkflowFailureRate | more than 20% of executions failed over 10m | warning |
| TasksDeadLettered | any task dead-lettered in the last 15m | warning |
| OutboxBacklog | more than 100 unpublished outbox messages for 5m | warning |
| KafkaConsumerLag | consumer lag above 1000 records for 5m | warning |

## Logging

- **Workflow service**: JSON logs (logstash encoder) in the `docker` profile, including the
  `correlation_id` MDC field; plain text with the correlation ID in the `dev`/`local` profiles.
- **Other services**: plain-text console logs.
- **Correlation IDs**: the gateway accepts an incoming `X-Correlation-ID` or generates one, returns it
  in the response and forwards it to the workflow service, which logs it with every request.
  Kafka events carry the execution ID as `correlationId`.
- Containers use Docker's `json-file` log driver (10 MB x 3 files). View logs with
  `docker compose logs -f <service>` or `./scripts/logs.sh`.

## Not Implemented

- Distributed tracing (no OpenTelemetry/Jaeger backend)
- Centralised log aggregation (e.g. Loki or ELK)
- Alert delivery (Alertmanager)
