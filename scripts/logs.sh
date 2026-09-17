#!/bin/bash
# ============================================================================
# Chronos Logs Viewer Script
# ============================================================================
# View logs from Chronos services
# Usage: ./scripts/logs.sh [SERVICE] [OPTIONS]
# ============================================================================

# Colors for output
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default values
FOLLOW=false
TAIL_LINES=100
SERVICE=""

# Print info message
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [SERVICE] [OPTIONS]

View logs from Chronos services

SERVICE (optional):
    all                 All services (default)
    api-gateway         API Gateway logs
    workflow            Workflow Service logs
    scheduler           Scheduler Service logs
    worker              Worker Service logs
    mongodb             MongoDB logs
    redis               Redis logs
    kafka               Kafka logs
    prometheus          Prometheus logs
    grafana             Grafana logs

OPTIONS:
    -f, --follow        Follow log output (live tail)
    -n, --lines NUM     Number of lines to show (default: 100)
    -h, --help          Show this help message

EXAMPLES:
    $0                          # Show last 100 lines from all services
    $0 --follow                 # Follow all service logs
    $0 api-gateway              # Show API Gateway logs
    $0 scheduler --follow       # Follow scheduler logs
    $0 worker -n 500            # Show last 500 lines from workers

EOF
}

# Parse command line arguments
parse_args() {
    # Check if first argument is a service name
    if [ $# -gt 0 ] && [[ ! "$1" =~ ^- ]]; then
        SERVICE=$1
        shift
    fi
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--follow)
                FOLLOW=true
                shift
                ;;
            -n|--lines)
                TAIL_LINES="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Map service names to container names
get_container_names() {
    case $SERVICE in
        api-gateway|api|gateway)
            echo "chronos-api-gateway"
            ;;
        workflow|workflow-service)
            echo "chronos-workflow-service"
            ;;
        scheduler|scheduler-service)
            echo "chronos-scheduler-service"
            ;;
        worker|worker-service|workers)
            # Get all worker containers
            docker ps --format '{{.Names}}' | grep "worker-service" | tr '\n' ' '
            ;;
        mongodb|mongo)
            echo "chronos-mongodb"
            ;;
        redis)
            echo "chronos-redis"
            ;;
        kafka)
            echo "chronos-kafka"
            ;;
        zookeeper)
            echo "chronos-zookeeper"
            ;;
        prometheus)
            echo "chronos-prometheus"
            ;;
        grafana)
            echo "chronos-grafana"
            ;;
        all|"")
            echo ""  # Empty means all services
            ;;
        *)
            echo "ERROR: Unknown service: $SERVICE" >&2
            usage
            exit 1
            ;;
    esac
}

# View logs
view_logs() {
    cd "${PROJECT_ROOT}"
    
    COMPOSE_ARGS="--tail=$TAIL_LINES"
    
    if [ "$FOLLOW" = true ]; then
        COMPOSE_ARGS="$COMPOSE_ARGS --follow"
    fi
    
    local container_names=$(get_container_names)
    
    if [ -z "$container_names" ]; then
        print_info "Showing logs from all services (last $TAIL_LINES lines)..."
        docker compose logs $COMPOSE_ARGS
    else
        print_info "Showing logs from: $container_names (last $TAIL_LINES lines)..."
        docker logs $COMPOSE_ARGS $container_names
    fi
}

# Main execution
main() {
    parse_args "$@"
    view_logs
}

# Run main function
main "$@"
