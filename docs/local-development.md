# Local Development Guide

This document describes how to run and develop Chronos locally using Docker Compose.

---

## Table of Contents

- [Infrastructure Overview](#infrastructure-overview)
- [Quick Start](#quick-start)
- [Infrastructure Services](#infrastructure-services)
- [Development Workflows](#development-workflows)
- [Troubleshooting](#troubleshooting)
- [Useful Commands](#useful-commands)

---

## Infrastructure Overview

Chronos uses a production-inspired but development-friendly local infrastructure stack:

### Infrastructure Services (5)

| Service | Purpose | Port | Data Persistence |
|---------|---------|------|------------------|
| **MongoDB** | Document database for workflow state | 27017 | Persistent volume |
| **Kafka** | Event streaming platform | 9092 | Persistent volume |
| **Zookeeper** | Kafka coordination | 2181 | Persistent volume |
| **Redis** | Distributed coordination & caching | 6379 | Persistent volume |
| **Prometheus** | Metrics collection | 9090 | Persistent volume (15 days retention) |
| **Grafana** | Metrics visualization | 3000 | Persistent volume |

### Application Services (4)

| Service | Purpose | Port | Scalable |
|---------|---------|------|----------|
| **API Gateway** | Authentication & routing | 8080 | No |
| **Workflow Service** | Workflow CRUD & execution | 8081 | No |
| **Scheduler Service** | Task dependency resolution | 8082 | Yes (with leader election) |
| **Worker Service** | Task execution | 8083+ | Yes (horizontal scaling) |

---

## Quick Start

### Option 1: Start Everything (Recommended)

```bash
# Start all services
make up

# Or without make:
docker compose up --build -d
```

### Option 2: Infrastructure First, Then Applications

```bash
# Start infrastructure only
make infra-up

# Wait for infrastructure to be healthy (30-60 seconds)
make infra-status

# Start application services
make app-up
```

### Verify Services

```bash
# Check all service health
make health

# View logs
make logs

# Check specific service status
make status
```

---

## Infrastructure Services

### MongoDB (Port 27017)

**Purpose**: Durable storage for workflows, executions, tasks, and workers.

**Configuration**:
- Storage Engine: WiredTiger with Snappy compression
- Cache Size: 500MB (configurable)
- Authentication: Enabled
- Default Credentials: `chronos:chronos123` (change in production)

**Collections**:
- `users` - User accounts
- `workflows` - Workflow definitions
- `executions` - Workflow execution instances
- `tasks` - Task execution state
- `workers` - Worker registration and metadata

**Persistent Volumes**:
- `chronos-mongodb-data` → `/data/db`
- `chronos-mongodb-config` → `/data/configdb`

**Health Check**: Runs `mongosh` ping command every 10 seconds

**Connect**:
```bash
# Open MongoDB shell
make mongodb-shell

# Or directly
docker exec -it chronos-mongodb mongosh -u chronos -p chronos123 --authenticationDatabase admin chronos
```

**Useful Commands**:
```javascript
// Show databases
show dbs

// Switch to chronos database
use chronos

// Show collections
show collections

// Query workflows
db.workflows.find().pretty()

// Count executions
db.executions.count()

// Check indexes
db.tasks.getIndexes()
```

---

### Kafka (Port 9092)

**Purpose**: Event streaming for asynchronous workflow orchestration.

**Configuration**:
- Broker ID: 1
- Replication Factor: 1 (local development)
- Default Partitions: 6
- Log Retention: 24 hours
- Max Message Size: 10MB

**Topics** (auto-created):
- `workflow-events` - Workflow lifecycle events (3 partitions)
- `task-events` - Task lifecycle events (6 partitions)
- `worker-events` - Worker health events (1 partition)
- `dead-letter-queue` - Failed tasks (1 partition)

**Persistent Volume**:
- `chronos-kafka-data` → `/var/lib/kafka/data`

**Health Check**: Runs `kafka-broker-api-versions` every 10 seconds

**Connect**:
```bash
# List topics
make kafka-topics

# Or directly
docker exec chronos-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
docker exec chronos-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic task-events

# Consume from a topic
docker exec chronos-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic task-events --from-beginning

# Produce to a topic (testing)
docker exec -it chronos-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic task-events
```

---

### Zookeeper (Port 2181)

**Purpose**: Coordination service for Kafka.

**Configuration**:
- Client Port: 2181
- Tick Time: 2000ms
- Autopurge: Enabled (snapshots retained: 3, interval: 24h)

**Persistent Volumes**:
- `chronos-zookeeper-data` → `/var/lib/zookeeper/data`
- `chronos-zookeeper-logs` → `/var/lib/zookeeper/log`

**Health Check**: Runs `echo ruok | nc localhost 2181` every 10 seconds (expects "imok")

---

### Redis (Port 6379)

**Purpose**: Distributed coordination, distributed locking, worker heartbeats, caching.

**Configuration**:
- Persistence: AOF (Append-Only File) with fsync every second
- Max Memory: 256MB
- Eviction Policy: allkeys-lru
- Snapshots: Enabled (900s/1 change, 300s/10 changes, 60s/10000 changes)

**Data Structures**:
- `task:{executionId}:{taskId}:lock` - Task locks (TTL: 5 minutes)
- `worker:{workerId}:heartbeat` - Worker heartbeats (TTL: 30 seconds)
- `worker:{workerId}:metadata` - Worker metadata (persistent)
- `scheduler:leader:lock` - Scheduler leader lock (TTL: 10 seconds)

**Persistent Volume**:
- `chronos-redis-data` → `/data`

**Health Check**: Runs `redis-cli ping` every 10 seconds (expects "PONG")

**Connect**:
```bash
# Open Redis CLI
make redis-cli

# Or directly
docker exec -it chronos-redis redis-cli
```

**Useful Commands**:
```bash
# List all keys (careful in production!)
KEYS *

# Get worker heartbeat
GET worker:worker-01:heartbeat

# Check task lock
GET task:execution-123:task-456:lock

# Get scheduler leader
GET scheduler:leader:lock

# Monitor commands in real-time
MONITOR

# Get server info
INFO

# Get memory usage
INFO memory
```

---

### Prometheus (Port 9090)

**Purpose**: Metrics collection from all services.

**Configuration**:
- Scrape Interval: 15 seconds
- Retention Time: 15 days
- Retention Size: 5GB
- Storage Path: `/prometheus`

**Targets** (scraped):
- API Gateway: `api-gateway:8080/actuator/prometheus`
- Workflow Service: `workflow-service:8081/actuator/prometheus`
- Scheduler Service: `scheduler-service:8082/actuator/prometheus`
- Worker Service: `worker-service:8083/actuator/prometheus` (all instances)

**Persistent Volume**:
- `chronos-prometheus-data` → `/prometheus`

**Health Check**: Checks `http://localhost:9090/-/healthy` every 10 seconds

**Access**: http://localhost:9090

**Useful Queries**:
```promql
# Total workflows created
workflow_created_total

# Task execution rate
rate(task_completed_total[5m])

# Active workers
worker_available_count

# Kafka consumer lag
kafka_consumer_lag

# Task retry rate
rate(task_retry_total[5m])

# HTTP request rate
rate(http_server_requests_seconds_count[5m])
```

---

### Grafana (Port 3000)

**Purpose**: Metrics visualization and dashboards.

**Configuration**:
- Admin User: `admin` (configurable via `GRAFANA_ADMIN_USER`)
- Admin Password: `admin` (configurable via `GRAFANA_ADMIN_PASSWORD`)
- Datasource: Prometheus (pre-configured)

**Persistent Volume**:
- `chronos-grafana-data` → `/var/lib/grafana`

**Health Check**: Checks `http://localhost:3000/api/health` every 10 seconds

**Access**: http://localhost:3000 (login: admin/admin)

**Pre-configured**:
- Prometheus datasource at `http://prometheus:9090`
- Dashboard provisioning enabled

---

## Development Workflows

### Workflow 1: Full Stack Development

Start everything and develop with hot reload:

```bash
# Start all services
make up

# View logs
make logs

# Code changes in services will be picked up on container restart
docker compose restart workflow-service
```

### Workflow 2: Local Service Development

Run infrastructure in Docker, services locally:

```bash
# 1. Start infrastructure only
make infra-up

# 2. Run services locally with Maven
# Terminal 1
cd services/workflow-service
mvn spring-boot:run

# Terminal 2
cd services/scheduler-service
mvn spring-boot:run

# Terminal 3
cd services/worker-service
mvn spring-boot:run

# Terminal 4
cd services/api-gateway
mvn spring-boot:run
```

**Benefits**:
- Hot reload with Spring DevTools
- Easier debugging
- Faster iteration

**Configuration**:
Update `application.yml` to use `localhost` instead of Docker service names:
```yaml
spring:
  data:
    mongodb:
      uri: mongodb://chronos:chronos123@localhost:27017/chronos
  kafka:
    bootstrap-servers: localhost:9092
  data:
    redis:
      host: localhost
```

### Workflow 3: Horizontal Worker Scaling

Test worker scaling behavior:

```bash
# Start with 3 workers (default)
make up

# Scale to 5 workers
make scale-workers N=5

# Verify
make app-status

# View worker logs
docker compose logs -f worker-service
```

Each worker instance will have a unique ID and will process tasks in parallel.

---

## Troubleshooting

### Issue: Services Won't Start

**Symptoms**: Containers exit immediately, health checks fail

**Solutions**:

1. **Check logs**:
   ```bash
   make logs
   # Or specific service:
   docker compose logs mongodb
   ```

2. **Check port conflicts**:
   ```bash
   # macOS
   lsof -i :8080
   lsof -i :27017
   lsof -i :9092
   
   # Linux
   netstat -tuln | grep 8080
   ```

3. **Check Docker resources**:
   - Ensure Docker Desktop has at least 4GB RAM allocated
   - Check disk space: `docker system df`

4. **Clean slate**:
   ```bash
   make down
   make clean-volumes
   make up
   ```

### Issue: Kafka Won't Start

**Symptoms**: Kafka container exits, "Connection refused" errors

**Solutions**:

1. **Ensure Zookeeper is healthy**:
   ```bash
   docker compose ps zookeeper
   # Should show "healthy"
   ```

2. **Check Zookeeper logs**:
   ```bash
   docker compose logs zookeeper
   ```

3. **Restart Kafka**:
   ```bash
   docker compose restart kafka
   ```

4. **Clean Kafka data** (WARNING: deletes all topics):
   ```bash
   docker compose down
   docker volume rm chronos-kafka-data
   docker compose up -d kafka
   ```

### Issue: MongoDB Authentication Fails

**Symptoms**: "Authentication failed" errors in service logs

**Solutions**:

1. **Verify credentials** in `.env`:
   ```bash
   cat .env | grep MONGODB
   ```

2. **Check MongoDB logs**:
   ```bash
   docker compose logs mongodb
   ```

3. **Recreate MongoDB** (WARNING: deletes all data):
   ```bash
   docker compose down
   docker volume rm chronos-mongodb-data chronos-mongodb-config
   docker compose up -d mongodb
   ```

### Issue: Redis Connection Timeout

**Symptoms**: Services can't connect to Redis

**Solutions**:

1. **Check Redis is running**:
   ```bash
   docker compose ps redis
   ```

2. **Test connection**:
   ```bash
   docker exec chronos-redis redis-cli ping
   # Should return "PONG"
   ```

3. **Check Redis logs**:
   ```bash
   docker compose logs redis
   ```

### Issue: High Memory Usage

**Symptoms**: Docker uses excessive memory

**Solutions**:

1. **Check memory usage**:
   ```bash
   docker stats
   ```

2. **Limit service memory**:
   Edit `docker-compose.yml` and add memory limits:
   ```yaml
   services:
     mongodb:
       deploy:
         resources:
           limits:
             memory: 512M
   ```

3. **Reduce Kafka memory**:
   Kafka already has `KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"`

4. **Prune unused resources**:
   ```bash
   docker system prune -a
   ```

---

## Useful Commands

### Health Checks

```bash
# Check all service health
make health

# Check infrastructure status
make infra-status

# Check application status
make app-status

# Check specific service
curl http://localhost:8081/actuator/health
```

### Logs

```bash
# All logs
make logs

# Infrastructure logs
make infra-logs

# Application logs
make app-logs

# Specific service logs
docker compose logs -f workflow-service

# Last 100 lines
docker compose logs --tail=100 scheduler-service
```

### Metrics

```bash
# Show metrics URLs
make metrics

# View specific service metrics
curl http://localhost:8081/actuator/prometheus

# View Prometheus UI
open http://localhost:9090

# View Grafana
open http://localhost:3000
```

### Data Access

```bash
# MongoDB
make mongodb-shell

# Redis
make redis-cli

# Kafka topics
make kafka-topics

# Consume Kafka messages
docker exec chronos-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic task-events \
  --from-beginning
```

### Scaling

```bash
# Scale workers
make scale-workers N=5

# Verify scaling
docker compose ps worker-service
```

### Cleanup

```bash
# Stop services
make down

# Stop infrastructure only
make infra-down

# Remove all volumes (WARNING: deletes data)
make clean-volumes

# Clean Docker system
docker system prune -a --volumes
```

---

## Environment Variables

Configure via `.env` file:

```bash
# MongoDB
MONGODB_PORT=27017
MONGODB_ROOT_USERNAME=chronos
MONGODB_ROOT_PASSWORD=chronos123
MONGODB_DATABASE=chronos

# Kafka
KAFKA_PORT=9092

# Redis
REDIS_PORT=6379

# Prometheus
PROMETHEUS_PORT=9090

# Grafana
GRAFANA_PORT=3000
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin

# Application
API_GATEWAY_PORT=8080
WORKFLOW_SERVICE_PORT=8081
SCHEDULER_SERVICE_PORT=8082

# JWT
JWT_SECRET=<your-secure-secret-256-bits>
JWT_EXPIRATION_MS=3600000

# Logging
LOGGING_LEVEL_ROOT=INFO
LOGGING_LEVEL_COM_CHRONOS=DEBUG
```

---

## Network Architecture

All services communicate through the `chronos-network` Docker bridge network:

```
chronos-network (bridge)
├── mongodb (chronos-mongodb)
├── zookeeper (chronos-zookeeper)
├── kafka (chronos-kafka)
├── redis (chronos-redis)
├── prometheus (chronos-prometheus)
├── grafana (chronos-grafana)
├── api-gateway (chronos-api-gateway)
├── workflow-service (chronos-workflow-service)
├── scheduler-service (chronos-scheduler-service)
└── worker-service (multiple instances)
```

**Service Discovery**:
- Services communicate using container names (e.g., `mongodb`, `kafka`, `redis`)
- DNS resolution is handled by Docker
- External access uses mapped ports (e.g., `localhost:27017` → `mongodb:27017`)

---

## Volume Management

### List Volumes

```bash
docker volume ls | grep chronos
```

### Inspect Volume

```bash
docker volume inspect chronos-mongodb-data
```

### Backup Volume

```bash
# Backup MongoDB
docker run --rm -v chronos-mongodb-data:/data -v $(pwd):/backup alpine tar czf /backup/mongodb-backup.tar.gz /data

# Restore MongoDB
docker run --rm -v chronos-mongodb-data:/data -v $(pwd):/backup alpine tar xzf /backup/mongodb-backup.tar.gz -C /
```

### Remove Specific Volume

```bash
# Stop services first
make down

# Remove volume
docker volume rm chronos-mongodb-data

# Recreate
make up
```

---

## Performance Tuning

### MongoDB

Edit `infrastructure/mongodb/mongod.conf`:
```yaml
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 1.0  # Increase cache
```

### Kafka

Edit `docker-compose.yml`:
```yaml
environment:
  KAFKA_HEAP_OPTS: "-Xmx1G -Xms1G"  # Increase heap
```

### Redis

Edit `docker-compose.yml`:
```yaml
command: >
  redis-server
  --maxmemory 512mb  # Increase memory
```

---

## Next Steps

- [Architecture Documentation](architecture.md)
- [Design Decisions](design-decisions.md)
- [Failure Scenarios](failure-scenarios.md)
- [API Documentation](../README.md#api-documentation)

---

**Happy Developing!** 
