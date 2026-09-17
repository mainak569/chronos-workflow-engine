# Chronos — Distributed Workflow Orchestration Engine

> A fault-tolerant, event-driven workflow orchestration platform for distributed task execution.

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.1-green.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Running Services](#running-services)
- [API Documentation](#api-documentation)
- [Monitoring](#monitoring)
- [Development](#development)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)

---

## 🎯 Overview

Chronos is a production-inspired distributed workflow orchestration engine built to demonstrate advanced distributed systems concepts including:

- **Event-Driven Architecture** using Apache Kafka
- **Distributed Worker Coordination** via Redis
- **Fault Tolerance** and failure recovery
- **Horizontal Scaling** of worker services
- **At-Least-Once Delivery** with idempotent handlers
- **Distributed Locking** for safe concurrent task execution
- **Observability** with Prometheus and Grafana

Chronos allows you to define multi-step workflows with task dependencies and execute them reliably across a pool of distributed workers.

---

## 🏗️ Architecture

### High-Level Architecture

```
                         CLIENT
                            |
                            v
                     +-------------+
                     | API Gateway |
                     |  (Port 8080)|
                     +-------------+
                            |
                            v
                  +-------------------+
                  | Workflow Service  |
                  |    (Port 8081)    |
                  +-------------------+
                            |
                            v
                       +---------+
                       | MongoDB |
                       | (27017) |
                       +---------+
                            |
                    Workflow Events
                            |
                            v
                      +-----------+
                      |   Kafka   |
                      |  (9092)   |
                      +-----------+
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
        +-----------+ +-----------+ +-----------+
        | Worker 1  | | Worker 2  | | Worker N  |
        | (8083)    | | (8084)    | | (8083+N)  |
        +-----------+ +-----------+ +-----------+
              |             |             |
              +-------------+-------------+
                            |
                            v
                         +-------+
                         | Redis |
                         | (6379)|
                         +-------+

                  +----------------------+
                  | Prometheus + Grafana |
                  |   (9090)     (3000)  |
                  +----------------------+
```

### Service Responsibilities

| Service | Port | Responsibilities |
|---------|------|------------------|
| **API Gateway** | 8080 | Authentication, authorization, request routing |
| **Workflow Service** | 8081 | Workflow CRUD, execution management, event publishing |
| **Scheduler Service** | 8082 | Task dependency resolution, ready task scheduling |
| **Worker Service** | 8083+ | Task execution, heartbeat, distributed locking |

---

## ✨ Features

- ✅ **Workflow Management**: Create, retrieve, and execute workflows with task dependencies
- ✅ **Distributed Execution**: Multiple workers execute tasks in parallel
- ✅ **Failure Recovery**: Automatic detection and recovery from worker failures
- ✅ **Retry Mechanism**: Exponential backoff retry for failed tasks
- ✅ **Dead Letter Queue**: Permanently failed tasks moved to DLQ for investigation
- ✅ **Distributed Locking**: Redis-based locks prevent duplicate task execution
- ✅ **Worker Heartbeats**: Automatic worker health monitoring
- ✅ **Event-Driven**: Asynchronous communication via Kafka
- ✅ **Observability**: Metrics collection with Prometheus, visualization with Grafana
- ✅ **Horizontal Scaling**: Scale worker service independently
- ✅ **Idempotent Handlers**: Safe handling of duplicate events

---

## 🛠️ Technology Stack

### Core Technologies
- **Java 21** - Programming language
- **Spring Boot 3.2.1** - Application framework
- **Maven** - Build tool

### Infrastructure
- **Apache Kafka** - Event streaming and message broker
- **MongoDB** - Document database for workflow state
- **Redis** - Distributed coordination and caching
- **Docker & Docker Compose** - Containerization

### Observability
- **Prometheus** - Metrics collection
- **Grafana** - Metrics visualization
- **Spring Actuator** - Health checks and metrics endpoints

### Testing
- **JUnit 5** - Unit testing
- **Mockito** - Mocking framework
- **Testcontainers** - Integration testing with real infrastructure

---

## 📦 Prerequisites

Before running Chronos, ensure you have the following installed:

- **Java 21** or higher ([Download](https://adoptium.net/temurin/releases/?version=21))
- **Maven 3.9+** ([Download](https://maven.apache.org/download.cgi))
- **Docker Desktop** ([Download](https://www.docker.com/products/docker-desktop/))
- **Docker Compose** (included with Docker Desktop)

Verify installations:

```bash
java -version    # Should show Java 21
mvn -version     # Should show Maven 3.9+
docker --version
docker compose version
```

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/chronos.git
cd chronos
```

### 2. Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

**Important**: Update `JWT_SECRET` in `.env` with a secure random string (minimum 256 bits).

```bash
# Generate a secure JWT secret
openssl rand -base64 32
```

### 3. Build All Services

```bash
# Build all services using Maven
cd services/api-gateway && mvn clean package -DskipTests && cd ../..
cd services/workflow-service && mvn clean package -DskipTests && cd ../..
cd services/scheduler-service && mvn clean package -DskipTests && cd ../..
cd services/worker-service && mvn clean package -DskipTests && cd ../..
```

Or use the convenience script (if provided):

```bash
./scripts/build-all.sh
```

### 4. Start the System

Start all services with Docker Compose:

```bash
docker compose up --build
```

To run in detached mode:

```bash
docker compose up --build -d
```

### 5. Verify Services are Running

Check service health:

```bash
# API Gateway
curl http://localhost:8080/actuator/health

# Workflow Service
curl http://localhost:8081/actuator/health

# Scheduler Service
curl http://localhost:8082/actuator/health

# Worker Service
curl http://localhost:8083/actuator/health
```

All services should return:
```json
{"status":"UP"}
```

### 6. Access Monitoring Dashboards

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

---

## 📂 Project Structure

```
chronos/
├── services/
│   ├── api-gateway/          # API Gateway service
│   │   ├── src/
│   │   ├── pom.xml
│   │   └── Dockerfile
│   ├── workflow-service/     # Workflow management service
│   │   ├── src/
│   │   ├── pom.xml
│   │   └── Dockerfile
│   ├── scheduler-service/    # Task scheduling service
│   │   ├── src/
│   │   ├── pom.xml
│   │   └── Dockerfile
│   └── worker-service/       # Task execution service
│       ├── src/
│       ├── pom.xml
│       └── Dockerfile
├── infrastructure/
│   ├── mongodb/              # MongoDB initialization scripts
│   ├── kafka/                # Kafka configuration
│   ├── redis/                # Redis configuration
│   ├── prometheus/           # Prometheus configuration
│   └── grafana/              # Grafana dashboards and datasources
├── docs/
│   ├── architecture.md       # System architecture documentation
│   ├── design-decisions.md   # Design rationale and trade-offs
│   └── failure-scenarios.md  # Failure handling documentation
├── postman/                  # Postman collection for API testing
├── docker-compose.yml        # Docker Compose configuration
├── .env.example              # Example environment variables
├── .gitignore
└── README.md
```

---

## ⚙️ Configuration

All services are configured via environment variables. See `.env.example` for available options.

### Key Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://chronos:chronos123@mongodb:27017/chronos` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `kafka:29092` |
| `REDIS_HOST` | Redis hostname | `redis` |
| `REDIS_PORT` | Redis port | `6379` |
| `JWT_SECRET` | JWT signing secret (256+ bits) | *Must be set* |
| `JWT_EXPIRATION_MS` | JWT token expiration time | `3600000` (1 hour) |

---

## 🏃 Running Services

### Start All Services

```bash
docker compose up --build
```

### Scale Worker Service

Run multiple worker instances:

```bash
docker compose up --scale worker-service=5
```

This starts 5 worker instances that will automatically distribute tasks.

### Stop All Services

```bash
docker compose down
```

### Stop and Remove Volumes (Clean Slate)

```bash
docker compose down -v
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f workflow-service
```

---

## 📡 API Documentation

### Base URL

```
http://localhost:8080/api/v1
```

### Authentication

Most endpoints require JWT authentication. Obtain a token via login:

```bash
# Register
curl -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "name": "John Doe",
    "password": "SecurePass123!"
  }'

# Login
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "SecurePass123!"
  }'
```

Response:
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenType": "Bearer",
  "expiresIn": 3600
}
```

### Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/register` | Register new user |
| POST | `/api/v1/auth/login` | Login and get JWT token |
| POST | `/api/v1/workflows` | Create workflow |
| GET | `/api/v1/workflows/{id}` | Get workflow details |
| GET | `/api/v1/workflows` | List workflows |
| POST | `/api/v1/workflows/{id}/execute` | Start workflow execution |
| GET | `/api/v1/executions/{id}` | Get execution status |
| GET | `/api/v1/executions/{id}/tasks` | Get execution tasks |

For complete API documentation with examples, see the [Postman collection](./postman/chronos-api.json).

---

## 📊 Monitoring

### Grafana Dashboards

Access Grafana at http://localhost:3000 (username: `admin`, password: `admin`)

**Key Metrics to Monitor**:
- Workflow executions (total, success, failure rates)
- Task execution duration
- Worker health and availability
- Kafka consumer lag
- Task retry and dead-letter queue metrics

### Prometheus

Access Prometheus at http://localhost:9090

**Sample Queries**:

```promql
# Total workflows created
workflow_created_total

# Task execution rate
rate(task_completed_total[5m])

# Active workers
worker_available_count

# Kafka consumer lag
kafka_consumer_lag
```

### Health Endpoints

Each service exposes health and metrics endpoints:

```bash
# Health check
curl http://localhost:8081/actuator/health

# Prometheus metrics
curl http://localhost:8081/actuator/metrics

# Detailed metrics
curl http://localhost:8081/actuator/prometheus
```

---

## 💻 Development

### Running Services Locally (Outside Docker)

For development, you can run services locally while using Docker for infrastructure:

1. Start infrastructure only:

```bash
docker compose up mongodb kafka redis prometheus grafana
```

2. Run services with Maven:

```bash
# Terminal 1: Workflow Service
cd services/workflow-service
mvn spring-boot:run

# Terminal 2: Scheduler Service
cd services/scheduler-service
mvn spring-boot:run

# Terminal 3: Worker Service
cd services/worker-service
mvn spring-boot:run

# Terminal 4: API Gateway
cd services/api-gateway
mvn spring-boot:run
```

### Hot Reload

Spring Boot DevTools is included for automatic restart on code changes during development.

---

## 🧪 Testing

### Unit Tests

Run unit tests for a specific service:

```bash
cd services/workflow-service
mvn test
```

### Integration Tests

Integration tests use Testcontainers to spin up real MongoDB, Kafka, and Redis instances:

```bash
cd services/workflow-service
mvn verify
```

### Run All Tests

```bash
# From project root
for service in api-gateway workflow-service scheduler-service worker-service; do
  cd services/$service
  mvn clean verify
  cd ../..
done
```

---

## 📚 Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[Architecture](docs/architecture.md)** - System architecture, service boundaries, data models
- **[Design Decisions](docs/design-decisions.md)** - Technology choices, trade-offs, rationale
- **[Failure Scenarios](docs/failure-scenarios.md)** - Failure detection and recovery strategies

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Chronos is inspired by production workflow orchestration systems including:
- [Temporal](https://temporal.io/)
- [Apache Airflow](https://airflow.apache.org/)
- [AWS Step Functions](https://aws.amazon.com/step-functions/)
- [Celery](https://docs.celeryq.dev/)

---

## 📞 Support

For questions, issues, or feature requests, please:
- Open an [issue](https://github.com/yourusername/chronos/issues)
- Check existing [documentation](docs/)
- Review [PROJECT_SPEC.md](PROJECT_SPEC.md) for detailed specifications

---

**Built with ❤️ for learning distributed systems**
