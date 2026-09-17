#!/bin/bash

# Verify POM files are correctly structured
# Usage: ./scripts/verify-poms.sh

echo "========================================="
echo "Verifying Maven POM Files"
echo "========================================="

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

errors=0

echo ""
echo "Checking parent POM..."
if [ -f "pom.xml" ]; then
    if grep -q "<packaging>pom</packaging>" pom.xml; then
        echo -e "${GREEN}✓${NC} Parent POM exists and is correctly configured"
    else
        echo -e "${RED}✗${NC} Parent POM missing packaging type"
        ((errors++))
    fi
else
    echo -e "${RED}✗${NC} Parent POM not found"
    ((errors++))
fi

echo ""
echo "Checking service POMs..."

services=("api-gateway" "workflow-service" "scheduler-service" "worker-service")

for service in "${services[@]}"; do
    pom_path="services/$service/pom.xml"
    if [ -f "$pom_path" ]; then
        echo -e "${GREEN}✓${NC} $service POM exists"
        
        # Check for required elements
        if ! grep -q "<groupId>com.chronos</groupId>" "$pom_path"; then
            echo -e "${YELLOW}⚠${NC}  Warning: $service groupId may be incorrect"
        fi
        
        if ! grep -q "<java.version>21</java.version>" "$pom_path"; then
            echo -e "${YELLOW}⚠${NC}  Warning: $service Java version may not be 21"
        fi
    else
        echo -e "${RED}✗${NC} $service POM not found"
        ((errors++))
    fi
done

echo ""
echo "Checking application classes..."

# Check each service application class with correct paths
if [ -f "services/api-gateway/src/main/java/com/chronos/gateway/ApiGatewayApplication.java" ]; then
    echo -e "${GREEN}✓${NC} api-gateway application class exists"
else
    echo -e "${RED}✗${NC} api-gateway application class not found"
    ((errors++))
fi

if [ -f "services/workflow-service/src/main/java/com/chronos/workflow/WorkflowServiceApplication.java" ]; then
    echo -e "${GREEN}✓${NC} workflow-service application class exists"
else
    echo -e "${RED}✗${NC} workflow-service application class not found"
    ((errors++))
fi

if [ -f "services/scheduler-service/src/main/java/com/chronos/scheduler/SchedulerServiceApplication.java" ]; then
    echo -e "${GREEN}✓${NC} scheduler-service application class exists"
else
    echo -e "${RED}✗${NC} scheduler-service application class not found"
    ((errors++))
fi

if [ -f "services/worker-service/src/main/java/com/chronos/worker/WorkerServiceApplication.java" ]; then
    echo -e "${GREEN}✓${NC} worker-service application class exists"
else
    echo -e "${RED}✗${NC} worker-service application class not found"
    ((errors++))
fi

echo ""
echo "========================================="
if [ $errors -eq 0 ]; then
    echo -e "${GREEN}✓ All POM checks passed!${NC}"
    echo "========================================="
    echo ""
    echo "You can now build the project with:"
    echo "  mvn clean package -DskipTests"
    echo ""
    echo "Or build individual services:"
    echo "  cd services/api-gateway && mvn clean package -DskipTests"
    echo ""
    echo "Or use Docker Compose (easiest):"
    echo "  docker compose up --build"
    exit 0
else
    echo -e "${RED}✗ $errors check(s) failed${NC}"
    echo "========================================="
    exit 1
fi
