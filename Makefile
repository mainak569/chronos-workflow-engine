.PHONY: help clean build build-gateway build-workflow build-scheduler build-worker test package \
	up down restart logs status \
	infra-up infra-down infra-logs infra-status \
	app-up app-down app-logs app-status \
	scale-workers mongodb-shell kafka-topics redis-cli \
	health metrics clean-volumes check-docker

# Default target
help:
	@echo "Chronos - Distributed Workflow Orchestration Engine"
	@echo ""
	@echo "Prerequisites:"
	@echo "  make check-docker    - Verify Docker is running"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build           - Build all services"
	@echo "  make build-gateway   - Build API Gateway only"
	@echo "  make build-workflow  - Build Workflow Service only"
	@echo "  make build-scheduler - Build Scheduler Service only"
	@echo "  make build-worker    - Build Worker Service only"
	@echo "  make test            - Run tests for all services"
	@echo "  make package         - Package all services (creates JARs)"
	@echo "  make clean           - Clean all build artifacts"
	@echo ""
	@echo "Docker Compose Commands:"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - View logs from all services"
	@echo "  make status          - Show status of all services"
	@echo ""
	@echo "Infrastructure Commands:"
	@echo "  make infra-up        - Start infrastructure only (MongoDB, Kafka, Redis, etc.)"
	@echo "  make infra-down      - Stop infrastructure"
	@echo "  make infra-logs      - View infrastructure logs"
	@echo "  make infra-status    - Show infrastructure status"
	@echo ""
	@echo "Application Commands:"
	@echo "  make app-up          - Start application services only"
	@echo "  make app-down        - Stop application services"
	@echo "  make app-logs        - View application logs"
	@echo "  make app-status      - Show application status"
	@echo ""
	@echo "Scaling Commands:"
	@echo "  make scale-workers N=5  - Scale worker service to N instances"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make mongodb-shell   - Open MongoDB shell"
	@echo "  make kafka-topics    - List Kafka topics"
	@echo "  make redis-cli       - Open Redis CLI"
	@echo "  make health          - Check health of all services"
	@echo "  make metrics         - Show Prometheus metrics URLs"
	@echo ""
	@echo "Cleanup Commands:"
	@echo "  make clean-volumes   - Remove all Docker volumes (WARNING: deletes all data)"
	@echo ""

# ============================================================================
# Prerequisites
# ============================================================================

check-docker:
	@./scripts/check-docker.sh

# ============================================================================
# Build Commands
# ============================================================================

build:
	@echo "Building all services..."
	mvn clean compile -DskipTests

build-gateway:
	@echo "Building API Gateway..."
	cd services/api-gateway && mvn clean compile -DskipTests

build-workflow:
	@echo "Building Workflow Service..."
	cd services/workflow-service && mvn clean compile -DskipTests

build-scheduler:
	@echo "Building Scheduler Service..."
	cd services/scheduler-service && mvn clean compile -DskipTests

build-worker:
	@echo "Building Worker Service..."
	cd services/worker-service && mvn clean compile -DskipTests

test:
	@echo "Running tests..."
	mvn test

package:
	@echo "Packaging all services..."
	mvn clean package -DskipTests

clean:
	@echo "Cleaning build artifacts..."
	mvn clean

# ============================================================================
# Docker Compose Commands
# ============================================================================

up:
	@echo "Starting all Chronos services..."
	docker compose up --build -d
	@echo ""
	@echo "Services started. Use 'make logs' to view logs."
	@echo "Use 'make status' to check service status."

down:
	@echo "Stopping all Chronos services..."
	docker compose down
	@echo "Services stopped."

restart:
	@echo "Restarting all services..."
	docker compose restart
	@echo "Services restarted."

logs:
	@echo "Viewing logs from all services (Ctrl+C to exit)..."
	docker compose logs -f

status:
	@echo "Status of all services:"
	@echo ""
	docker compose ps

# ============================================================================
# Infrastructure Commands
# ============================================================================

infra-up:
	@echo "Starting infrastructure services..."
	docker compose up -d mongodb zookeeper kafka redis prometheus grafana
	@echo ""
	@echo "Infrastructure services started:"
	@echo "  MongoDB:    localhost:27017"
	@echo "  Kafka:      localhost:9092"
	@echo "  Redis:      localhost:6379"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"
	@echo ""
	@echo "Use 'make infra-logs' to view logs."

infra-down:
	@echo "Stopping infrastructure services..."
	docker compose stop mongodb zookeeper kafka redis prometheus grafana
	@echo "Infrastructure stopped."

infra-logs:
	@echo "Viewing infrastructure logs (Ctrl+C to exit)..."
	docker compose logs -f mongodb kafka redis prometheus grafana

infra-status:
	@echo "Infrastructure status:"
	@echo ""
	docker compose ps mongodb zookeeper kafka redis prometheus grafana

# ============================================================================
# Application Commands
# ============================================================================

app-up:
	@echo "Starting application services..."
	@echo "Note: Infrastructure must be running first (make infra-up)"
	docker compose up --build -d api-gateway workflow-service scheduler-service worker-service
	@echo ""
	@echo "Application services started:"
	@echo "  API Gateway:        http://localhost:8080"
	@echo "  Workflow Service:   http://localhost:8081"
	@echo "  Scheduler Service:  http://localhost:8082"
	@echo "  Worker Service:     ports 8083-8093 (one per replica, see docker compose ps)"
	@echo ""
	@echo "Use 'make app-logs' to view logs."

app-down:
	@echo "Stopping application services..."
	docker compose stop api-gateway workflow-service scheduler-service worker-service
	@echo "Application services stopped."

app-logs:
	@echo "Viewing application logs (Ctrl+C to exit)..."
	docker compose logs -f api-gateway workflow-service scheduler-service worker-service

app-status:
	@echo "Application status:"
	@echo ""
	docker compose ps api-gateway workflow-service scheduler-service worker-service

# ============================================================================
# Scaling Commands
# ============================================================================

scale-workers:
	@echo "Scaling worker service to $(N) instances..."
	docker compose up --scale worker-service=$(N) -d worker-service
	@echo "Worker service scaled to $(N) instances."

# ============================================================================
# Utility Commands
# ============================================================================

mongodb-shell:
	@echo "Opening MongoDB shell..."
	@echo "Use: show dbs, use chronos, show collections, db.workflows.find()"
	docker exec -it chronos-mongodb mongosh -u chronos -p chronos123 --authenticationDatabase admin chronos

kafka-topics:
	@echo "Listing Kafka topics..."
	docker exec chronos-kafka kafka-topics --bootstrap-server localhost:9092 --list

redis-cli:
	@echo "Opening Redis CLI..."
	@echo "Use: KEYS *, GET key, SET key value"
	docker exec -it chronos-redis redis-cli

health:
	@echo "Checking health of all services..."
	@echo ""
	@echo "API Gateway:"
	@curl -s http://localhost:8080/readyz | grep -q UP && echo "  ✓ UP" || echo "  ✗ DOWN"
	@echo ""
	@echo "Workflow Service:"
	@curl -s http://localhost:8081/readyz | grep -q UP && echo "  ✓ UP" || echo "  ✗ DOWN"
	@echo ""
	@echo "Scheduler Service:"
	@curl -s http://localhost:8082/readyz | grep -q UP && echo "  ✓ UP" || echo "  ✗ DOWN"
	@echo ""
	@echo "Worker Service (each replica gets its own host port):"
	@for c in $$(docker compose ps -q worker-service); do \
		name=$$(docker inspect -f '{{.Name}}' $$c | tr -d /); \
		port=$$(docker port $$c 8083/tcp | head -n 1 | cut -d: -f2); \
		curl -s http://localhost:$$port/readyz | grep -q UP && echo "  ✓ $$name UP (port $$port)" || echo "  ✗ $$name DOWN (port $$port)"; \
	done
	@echo ""

metrics:
	@echo "Metrics (actuator runs on management ports 9080-9083 inside the Docker network):"
	@echo ""
	@echo "Grafana dashboard:  http://localhost:3000/d/chronos-overview (admin/admin)"
	@echo "Prometheus targets: http://localhost:9090/targets"
	@echo "Prometheus alerts:  http://localhost:9090/alerts"
	@echo ""
	@echo "Raw metrics of one service, e.g.:"
	@echo "  docker compose exec workflow-service wget -qO- localhost:9081/actuator/prometheus | grep chronos_"

# ============================================================================
# Cleanup Commands
# ============================================================================

clean-volumes:
	@echo "WARNING: This will delete all Docker volumes and data!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	docker compose down -v
	@echo "All volumes removed."
