#!/bin/bash
# ============================================================================
# Chronos Health Check Script
# ============================================================================
# Checks health of all Chronos services
# Usage: ./scripts/health-check.sh
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

# Print colored message
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if a service is healthy
check_service_health() {
    local service_name=$1
    local health_url=$2
    local timeout=${3:-10}
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    if curl -sf --max-time "$timeout" "$health_url" > /dev/null 2>&1; then
        print_success "$service_name is healthy"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        print_error "$service_name is unhealthy or not responding"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
}

# Check if container is running
check_container_running() {
    local container_name=$1
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        print_success "Container $container_name is running"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        print_error "Container $container_name is not running"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
}

# Check Docker service health
check_docker_health() {
    local container_name=$1
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "unknown")
    
    if [ "$health_status" = "healthy" ]; then
        print_success "$container_name health check: healthy"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    elif [ "$health_status" = "starting" ]; then
        print_warning "$container_name health check: starting"
        return 0
    else
        print_error "$container_name health check: $health_status"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
}

# Print header
print_header() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "         Chronos Health Check"
    echo "════════════════════════════════════════════════════════════"
    echo ""
}

# Print summary
print_summary() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "         Health Check Summary"
    echo "════════════════════════════════════════════════════════════"
    echo ""
    echo "Total checks:  $TOTAL_CHECKS"
    echo -e "Passed:        ${GREEN}$PASSED_CHECKS${NC}"
    echo -e "Failed:        ${RED}$FAILED_CHECKS${NC}"
    echo ""
    
    if [ "$FAILED_CHECKS" -eq 0 ]; then
        print_success "All services are healthy!"
        return 0
    else
        print_error "Some services are unhealthy. Check logs with: docker compose logs -f"
        return 1
    fi
}

# Main health check
main() {
    print_header
    
    print_info "Checking infrastructure services..."
    check_docker_health "chronos-mongodb" || true
    check_docker_health "chronos-redis" || true
    check_docker_health "chronos-kafka" || true
    check_docker_health "chronos-zookeeper" || true
    
    echo ""
    print_info "Checking observability services..."
    check_docker_health "chronos-prometheus" || true
    check_docker_health "chronos-grafana" || true
    
    echo ""
    print_info "Checking application services..."
    check_service_health "API Gateway" "http://localhost:8080/actuator/health" 5 || true
    check_service_health "Workflow Service" "http://localhost:8081/actuator/health" 5 || true
    check_service_health "Scheduler Service" "http://localhost:8082/actuator/health" 5 || true
    
    # Check at least one worker
    if docker ps --format '{{.Names}}' | grep -q "worker-service"; then
        # Get first worker container
        local worker_container=$(docker ps --format '{{.Names}}' | grep "worker-service" | head -n 1)
        local worker_port=$(docker port "$worker_container" 8083 2>/dev/null | cut -d: -f2)
        if [ -n "$worker_port" ]; then
            check_service_health "Worker Service" "http://localhost:${worker_port}/actuator/health" 5 || true
        else
            print_warning "Worker service port not exposed, checking container status only"
            check_container_running "$worker_container" || true
        fi
    else
        print_error "No worker service containers found"
        TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
    fi
    
    echo ""
    print_info "Checking external endpoints..."
    check_service_health "Prometheus" "http://localhost:9090/-/healthy" 5 || true
    check_service_health "Grafana" "http://localhost:3000/api/health" 5 || true
    
    print_summary
}

# Run main function
main
exit $?
