#!/bin/bash

# Verify infrastructure services are running correctly
# Usage: ./scripts/verify-infrastructure.sh

set -e

echo "========================================="
echo "Verifying Chronos Infrastructure"
echo "========================================="

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

errors=0
warnings=0

# Function to check if a container is running
check_container() {
    local container_name=$1
    local service_name=$2
    
    if docker ps --filter "name=$container_name" --filter "status=running" --format "{{.Names}}" | grep -q "$container_name"; then
        echo -e "${GREEN}✓${NC} $service_name is running"
        return 0
    else
        echo -e "${RED}✗${NC} $service_name is not running"
        ((errors++))
        return 1
    fi
}

# Function to check health status
check_health() {
    local container_name=$1
    local service_name=$2
    
    local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "none")
    
    if [ "$health_status" = "healthy" ]; then
        echo -e "${GREEN}✓${NC} $service_name is healthy"
        return 0
    elif [ "$health_status" = "none" ]; then
        echo -e "${YELLOW}⚠${NC} $service_name has no health check"
        ((warnings++))
        return 0
    elif [ "$health_status" = "starting" ]; then
        echo -e "${YELLOW}⚠${NC} $service_name is still starting..."
        ((warnings++))
        return 0
    else
        echo -e "${RED}✗${NC} $service_name is unhealthy (status: $health_status)"
        ((errors++))
        return 1
    fi
}

# Function to check port connectivity
check_port() {
    local host=$1
    local port=$2
    local service_name=$3
    
    if nc -z "$host" "$port" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $service_name port $port is accessible"
        return 0
    else
        echo -e "${RED}✗${NC} $service_name port $port is not accessible"
        ((errors++))
        return 1
    fi
}

echo ""
echo "${BLUE}Checking Docker Containers...${NC}"
echo ""

# Check MongoDB
check_container "chronos-mongodb" "MongoDB"
check_health "chronos-mongodb" "MongoDB"
check_port "localhost" "27017" "MongoDB"

echo ""

# Check Zookeeper
check_container "chronos-zookeeper" "Zookeeper"
check_health "chronos-zookeeper" "Zookeeper"

echo ""

# Check Kafka
check_container "chronos-kafka" "Kafka"
check_health "chronos-kafka" "Kafka"
check_port "localhost" "9092" "Kafka"

echo ""

# Check Redis
check_container "chronos-redis" "Redis"
check_health "chronos-redis" "Redis"
check_port "localhost" "6379" "Redis"

echo ""

# Check Prometheus
check_container "chronos-prometheus" "Prometheus"
check_health "chronos-prometheus" "Prometheus"
check_port "localhost" "9090" "Prometheus"

echo ""

# Check Grafana
check_container "chronos-grafana" "Grafana"
check_health "chronos-grafana" "Grafana"
check_port "localhost" "3000" "Grafana"

echo ""
echo "${BLUE}Checking Service Connectivity...${NC}"
echo ""

# Test MongoDB connection
if docker exec chronos-mongodb mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} MongoDB responds to ping"
else
    echo -e "${RED}✗${NC} MongoDB does not respond to ping"
    ((errors++))
fi

# Test Redis connection
if docker exec chronos-redis redis-cli ping | grep -q "PONG"; then
    echo -e "${GREEN}✓${NC} Redis responds to PING"
else
    echo -e "${RED}✗${NC} Redis does not respond to PING"
    ((errors++))
fi

# Test Kafka broker
if docker exec chronos-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Kafka broker is reachable"
else
    echo -e "${RED}✗${NC} Kafka broker is not reachable"
    ((errors++))
fi

# Test Prometheus
if curl -s http://localhost:9090/-/healthy | grep -q "Prometheus"; then
    echo -e "${GREEN}✓${NC} Prometheus API is responding"
else
    echo -e "${RED}✗${NC} Prometheus API is not responding"
    ((errors++))
fi

# Test Grafana
if curl -s http://localhost:3000/api/health | grep -q "ok"; then
    echo -e "${GREEN}✓${NC} Grafana API is responding"
else
    echo -e "${RED}✗${NC} Grafana API is not responding"
    ((errors++))
fi

echo ""
echo "${BLUE}Checking Docker Volumes...${NC}"
echo ""

# Check volumes exist
volumes=("chronos-mongodb-data" "chronos-kafka-data" "chronos-redis-data" "chronos-prometheus-data" "chronos-grafana-data")

for volume in "${volumes[@]}"; do
    if docker volume inspect "$volume" > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Volume $volume exists"
    else
        echo -e "${YELLOW}⚠${NC} Volume $volume does not exist"
        ((warnings++))
    fi
done

echo ""
echo "${BLUE}Checking Docker Network...${NC}"
echo ""

if docker network inspect chronos-network > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Network chronos-network exists"
    
    # Count connected containers
    connected=$(docker network inspect chronos-network --format='{{range .Containers}}{{.Name}} {{end}}' | wc -w)
    echo -e "${GREEN}✓${NC} $connected containers connected to chronos-network"
else
    echo -e "${RED}✗${NC} Network chronos-network does not exist"
    ((errors++))
fi

echo ""
echo "========================================="

if [ $errors -eq 0 ] && [ $warnings -eq 0 ]; then
    echo -e "${GREEN}✓ All infrastructure checks passed!${NC}"
    echo "========================================="
    echo ""
    echo "Infrastructure is ready:"
    echo "  MongoDB:    localhost:27017"
    echo "  Kafka:      localhost:9092"
    echo "  Redis:      localhost:6379"
    echo "  Prometheus: http://localhost:9090"
    echo "  Grafana:    http://localhost:3000 (admin/admin)"
    echo ""
    exit 0
elif [ $errors -eq 0 ]; then
    echo -e "${YELLOW}⚠ Infrastructure is running with $warnings warning(s)${NC}"
    echo "========================================="
    echo ""
    echo "Some services may still be starting. Wait a few moments and try again."
    echo ""
    exit 0
else
    echo -e "${RED}✗ Infrastructure check failed with $errors error(s) and $warnings warning(s)${NC}"
    echo "========================================="
    echo ""
    echo "Troubleshooting steps:"
    echo "  1. Check logs: docker compose logs"
    echo "  2. Restart services: docker compose restart"
    echo "  3. Check port conflicts: lsof -i :27017"
    echo "  4. Clean restart: docker compose down && docker compose up -d"
    echo ""
    exit 1
fi
