#!/bin/bash
# ============================================================================
# Chronos Startup Script
# ============================================================================
# Starts all Chronos services using Docker Compose
# Usage: ./scripts/start.sh [OPTIONS]
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
BUILD=false
DETACHED=false
SCALE_WORKERS=3
CLEAN=false

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

# Print banner
print_banner() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                                                            ║"
    echo "║   ██████╗██╗  ██╗██████╗  ██████╗ ███╗   ██╗ ██████╗ ███████╗"
    echo "║  ██╔════╝██║  ██║██╔══██╗██╔═══██╗████╗  ██║██╔═══██╗██╔════╝"
    echo "║  ██║     ███████║██████╔╝██║   ██║██╔██╗ ██║██║   ██║███████╗"
    echo "║  ██║     ██╔══██║██╔══██╗██║   ██║██║╚██╗██║██║   ██║╚════██║"
    echo "║  ╚██████╗██║  ██║██║  ██║╚██████╔╝██║ ╚████║╚██████╔╝███████║"
    echo "║   ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝ ╚══════╝"
    echo "║                                                            ║"
    echo "║          Workflow Orchestration Engine                    ║"
    echo "║                  Version 1.0.0                            ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Start Chronos services using Docker Compose

OPTIONS:
    -b, --build         Build images before starting
    -d, --detached      Run in detached mode (background)
    -w, --workers NUM   Number of worker instances (default: 3)
    -c, --clean         Clean start (remove volumes and rebuild)
    -h, --help          Show this help message

EXAMPLES:
    $0                              # Start with existing images
    $0 --build                      # Build and start
    $0 --build --detached           # Build, start in background
    $0 --workers 5                  # Start with 5 worker instances
    $0 --clean --build              # Clean rebuild and start

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -b|--build)
                BUILD=true
                shift
                ;;
            -d|--detached)
                DETACHED=true
                shift
                ;;
            -w|--workers)
                SCALE_WORKERS="$2"
                shift 2
                ;;
            -c|--clean)
                CLEAN=true
                BUILD=true
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

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Check if docker compose is available
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Check if .env file exists
check_env_file() {
    if [ ! -f "${PROJECT_ROOT}/.env" ]; then
        print_warning ".env file not found. Creating from .env.example..."
        if [ -f "${PROJECT_ROOT}/.env.example" ]; then
            cp "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/.env"
            print_info "Please review and update .env file with your configuration"
            print_warning "Using default configuration. Press Ctrl+C to cancel or Enter to continue..."
            read -r
        else
            print_error ".env.example file not found"
            exit 1
        fi
    fi
}

# Clean volumes and containers
clean_environment() {
    print_warning "Cleaning environment (removing volumes and containers)..."
    cd "${PROJECT_ROOT}"
    docker compose down -v
    print_success "Environment cleaned"
}

# Start services
start_services() {
    print_info "Starting Chronos services..."
    cd "${PROJECT_ROOT}"
    
    COMPOSE_ARGS=""
    
    if [ "$BUILD" = true ]; then
        print_info "Building Docker images..."
        COMPOSE_ARGS="$COMPOSE_ARGS --build"
    fi
    
    if [ "$DETACHED" = true ]; then
        COMPOSE_ARGS="$COMPOSE_ARGS -d"
    fi
    
    # Start infrastructure and application services
    print_info "Starting services with $SCALE_WORKERS worker instances..."
    docker compose up $COMPOSE_ARGS --scale worker-service=$SCALE_WORKERS
    
    if [ "$DETACHED" = true ]; then
        print_success "Services started in background mode"
        echo ""
        print_info "Service URLs:"
        echo "  - API Gateway:      http://localhost:8080"
        echo "  - Workflow Service: http://localhost:8081"
        echo "  - Scheduler Service: http://localhost:8082"
        echo "  - Grafana Dashboard: http://localhost:3000 (admin/admin)"
        echo "  - Prometheus:       http://localhost:9090"
        echo ""
        print_info "Useful commands:"
        echo "  - View logs:        docker compose logs -f"
        echo "  - Stop services:    docker compose down"
        echo "  - Service status:   docker compose ps"
        echo "  - Health check:     ./scripts/health-check.sh"
        echo ""
    fi
}

# Wait for services to be healthy
wait_for_health() {
    if [ "$DETACHED" = true ]; then
        print_info "Waiting for services to become healthy..."
        sleep 10
        
        print_info "Checking service health..."
        "${SCRIPT_DIR}/health-check.sh" || print_warning "Some services may not be healthy yet. Check logs with: docker compose logs -f"
    fi
}

# Main execution
main() {
    print_banner
    parse_args "$@"
    check_prerequisites
    check_env_file
    
    if [ "$CLEAN" = true ]; then
        clean_environment
    fi
    
    start_services
    wait_for_health
    
    print_success "Chronos startup complete!"
}

# Run main function
main "$@"
