#!/bin/bash

# Verify Chronos project structure
# Usage: ./scripts/verify-structure.sh

set -e

echo "========================================="
echo "Verifying Chronos Project Structure"
echo "========================================="

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if a file exists
check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✓${NC} $1"
        return 0
    else
        echo -e "${RED}✗${NC} $1 (missing)"
        return 1
    fi
}

# Function to check if a directory exists
check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✓${NC} $1/"
        return 0
    else
        echo -e "${RED}✗${NC} $1/ (missing)"
        return 1
    fi
}

errors=0

echo ""
echo "Checking root files..."
check_file "README.md" || ((errors++))
check_file ".gitignore" || ((errors++))
check_file ".env.example" || ((errors++))
check_file "docker-compose.yml" || ((errors++))
check_file "PROJECT_SPEC.md" || ((errors++))

echo ""
echo "Checking documentation..."
check_dir "docs" || ((errors++))
check_file "docs/architecture.md" || ((errors++))
check_file "docs/design-decisions.md" || ((errors++))
check_file "docs/failure-scenarios.md" || ((errors++))

echo ""
echo "Checking API Gateway..."
check_dir "services/api-gateway" || ((errors++))
check_file "services/api-gateway/pom.xml" || ((errors++))
check_file "services/api-gateway/Dockerfile" || ((errors++))
check_file "services/api-gateway/src/main/java/com/chronos/gateway/ApiGatewayApplication.java" || ((errors++))
check_file "services/api-gateway/src/main/resources/application.yml" || ((errors++))
check_file "services/api-gateway/src/test/java/com/chronos/gateway/ApiGatewayApplicationTests.java" || ((errors++))

echo ""
echo "Checking Workflow Service..."
check_dir "services/workflow-service" || ((errors++))
check_file "services/workflow-service/pom.xml" || ((errors++))
check_file "services/workflow-service/Dockerfile" || ((errors++))
check_file "services/workflow-service/src/main/java/com/chronos/workflow/WorkflowServiceApplication.java" || ((errors++))
check_file "services/workflow-service/src/main/resources/application.yml" || ((errors++))
check_file "services/workflow-service/src/test/java/com/chronos/workflow/WorkflowServiceApplicationTests.java" || ((errors++))

echo ""
echo "Checking Scheduler Service..."
check_dir "services/scheduler-service" || ((errors++))
check_file "services/scheduler-service/pom.xml" || ((errors++))
check_file "services/scheduler-service/Dockerfile" || ((errors++))
check_file "services/scheduler-service/src/main/java/com/chronos/scheduler/SchedulerServiceApplication.java" || ((errors++))
check_file "services/scheduler-service/src/main/resources/application.yml" || ((errors++))
check_file "services/scheduler-service/src/test/java/com/chronos/scheduler/SchedulerServiceApplicationTests.java" || ((errors++))

echo ""
echo "Checking Worker Service..."
check_dir "services/worker-service" || ((errors++))
check_file "services/worker-service/pom.xml" || ((errors++))
check_file "services/worker-service/Dockerfile" || ((errors++))
check_file "services/worker-service/src/main/java/com/chronos/worker/WorkerServiceApplication.java" || ((errors++))
check_file "services/worker-service/src/main/resources/application.yml" || ((errors++))
check_file "services/worker-service/src/test/java/com/chronos/worker/WorkerServiceApplicationTests.java" || ((errors++))

echo ""
echo "Checking infrastructure..."
check_dir "infrastructure/mongodb" || ((errors++))
check_file "infrastructure/mongodb/init-mongo.js" || ((errors++))
check_dir "infrastructure/prometheus" || ((errors++))
check_file "infrastructure/prometheus/prometheus.yml" || ((errors++))
check_dir "infrastructure/grafana" || ((errors++))
check_file "infrastructure/grafana/provisioning/datasources/prometheus.yml" || ((errors++))
check_file "infrastructure/grafana/provisioning/dashboards/dashboard.yml" || ((errors++))

echo ""
echo "========================================="
if [ $errors -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    echo "========================================="
    echo ""
    echo "Project structure is complete."
    echo ""
    echo "Next steps:"
    echo "  1. Install Java 21, Maven, and Docker"
    echo "  2. Run: cp .env.example .env"
    echo "  3. Update JWT_SECRET in .env"
    echo "  4. Run: ./scripts/build-all.sh"
    echo "  5. Run: docker compose up --build"
    exit 0
else
    echo -e "${RED}✗ $errors check(s) failed${NC}"
    echo "========================================="
    exit 1
fi
