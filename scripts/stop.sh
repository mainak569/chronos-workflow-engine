#!/bin/bash
# ============================================================================
# Chronos Stop Script
# ============================================================================
# Stops all Chronos services
# Usage: ./scripts/stop.sh [OPTIONS]
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default values
REMOVE_VOLUMES=false
REMOVE_IMAGES=false

# Print colored message
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Stop Chronos services

OPTIONS:
    -v, --volumes       Remove volumes (deletes all data)
    -i, --images        Remove images
    -h, --help          Show this help message

EXAMPLES:
    $0                  # Stop services only
    $0 --volumes        # Stop and remove volumes (DESTRUCTIVE)
    $0 --images         # Stop and remove images

WARNING:
    Using --volumes will delete ALL data including:
    - MongoDB data
    - Redis data
    - Kafka data
    - Prometheus metrics
    - Grafana dashboards

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -v|--volumes)
                REMOVE_VOLUMES=true
                shift
                ;;
            -i|--images)
                REMOVE_IMAGES=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Confirm destructive operations
confirm_action() {
    local message=$1
    print_warning "$message"
    read -p "Are you sure? (yes/no): " -r
    echo
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
}

# Stop services
stop_services() {
    print_info "Stopping Chronos services..."
    cd "${PROJECT_ROOT}"
    
    COMPOSE_ARGS=""
    
    if [ "$REMOVE_VOLUMES" = true ]; then
        confirm_action "This will DELETE ALL DATA (MongoDB, Redis, Kafka, metrics)"
        COMPOSE_ARGS="$COMPOSE_ARGS -v"
    fi
    
    docker compose down $COMPOSE_ARGS
    
    print_success "Services stopped"
}

# Remove images
remove_images() {
    print_info "Removing Chronos images..."
    
    # Remove application images
    docker images | grep chronos | awk '{print $3}' | xargs -r docker rmi -f || true
    
    print_success "Images removed"
}

# Main execution
main() {
    parse_args "$@"
    stop_services
    
    if [ "$REMOVE_IMAGES" = true ]; then
        confirm_action "This will remove all Chronos Docker images"
        remove_images
    fi
    
    print_success "Chronos stopped successfully"
}

# Run main function
main "$@"
