#!/bin/bash

# Build script for all Chronos services
# Usage: ./scripts/build-all.sh

set -e

echo "========================================="
echo "Building Chronos Services"
echo "========================================="

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to build a service
build_service() {
    local service_name=$1
    local service_path=$2
    
    echo ""
    echo "Building $service_name..."
    echo "-----------------------------------------"
    
    cd "$service_path"
    
    if mvn clean package -DskipTests; then
        echo -e "${GREEN}✓ $service_name built successfully${NC}"
    else
        echo -e "${RED}✗ $service_name build failed${NC}"
        exit 1
    fi
    
    cd - > /dev/null
}

# Build all services
build_service "API Gateway" "services/api-gateway"
build_service "Workflow Service" "services/workflow-service"
build_service "Scheduler Service" "services/scheduler-service"
build_service "Worker Service" "services/worker-service"

echo ""
echo "========================================="
echo -e "${GREEN}All services built successfully!${NC}"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Start the system: docker compose up --build"
echo "  2. Check health: curl http://localhost:8080/readyz"
echo "  3. View logs: docker compose logs -f"
