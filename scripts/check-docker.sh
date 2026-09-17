#!/bin/bash

# Check if Docker is running and provide guidance
# Usage: ./scripts/check-docker.sh

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================="
echo "Checking Docker Environment"
echo "========================================="
echo ""

# Check if Docker command exists
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    echo ""
    echo "Please install Docker Desktop:"
    echo "  https://www.docker.com/products/docker-desktop/"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker command is available"
echo ""

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}✗ Docker daemon is not running${NC}"
    echo ""
    echo "To start Docker:"
    echo ""
    echo "  ${BLUE}macOS:${NC}"
    echo "    1. Open Docker Desktop application"
    echo "    2. Wait for Docker to start (whale icon in menu bar)"
    echo "    3. Run this script again"
    echo ""
    echo "  ${BLUE}Alternative:${NC}"
    echo "    • Open Spotlight (Cmd+Space)"
    echo "    • Type 'Docker' and press Enter"
    echo "    • Wait for Docker Desktop to start"
    echo ""
    echo "  ${BLUE}Check if running:${NC}"
    echo "    docker ps"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker daemon is running"
echo ""

# Check Docker Compose
if ! docker compose version &> /dev/null; then
    echo -e "${RED}✗ Docker Compose is not available${NC}"
    echo ""
    echo "Docker Compose should be included with Docker Desktop."
    echo "Try updating Docker Desktop to the latest version."
    echo ""
    exit 1
fi

COMPOSE_VERSION=$(docker compose version --short 2>/dev/null || echo "unknown")
echo -e "${GREEN}✓${NC} Docker Compose is available (version: $COMPOSE_VERSION)"
echo ""

# Check Docker resources
echo "Docker Resources:"
echo ""

# Get Docker info
if docker info &> /dev/null; then
    CONTAINERS=$(docker ps -q | wc -l | xargs)
    IMAGES=$(docker images -q | wc -l | xargs)
    VOLUMES=$(docker volume ls -q | wc -l | xargs)
    NETWORKS=$(docker network ls -q | wc -l | xargs)
    
    echo "  Running containers: $CONTAINERS"
    echo "  Images: $IMAGES"
    echo "  Volumes: $VOLUMES"
    echo "  Networks: $NETWORKS"
else
    echo -e "${YELLOW}⚠${NC} Could not retrieve Docker info"
fi

echo ""

# Check if Chronos containers exist
if docker ps -a --filter "name=chronos-" --format "{{.Names}}" | grep -q "chronos-"; then
    echo -e "${BLUE}Existing Chronos containers:${NC}"
    docker ps -a --filter "name=chronos-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""
else
    echo "No existing Chronos containers found."
    echo ""
fi

# Check if Chronos volumes exist
if docker volume ls --filter "name=chronos-" --format "{{.Name}}" | grep -q "chronos-"; then
    echo -e "${BLUE}Existing Chronos volumes:${NC}"
    docker volume ls --filter "name=chronos-" --format "table {{.Name}}\t{{.Driver}}"
    echo ""
else
    echo "No existing Chronos volumes found."
    echo ""
fi

# Check if Chronos network exists
if docker network ls --filter "name=chronos-network" --format "{{.Name}}" | grep -q "chronos-network"; then
    echo -e "${GREEN}✓${NC} Chronos network exists"
    echo ""
else
    echo "No Chronos network found (will be created on first run)."
    echo ""
fi

echo "========================================="
echo -e "${GREEN}✓ Docker environment is ready!${NC}"
echo "========================================="
echo ""
echo "Next steps:"
echo "  ${BLUE}Start infrastructure:${NC}"
echo "    make infra-up"
echo ""
echo "  ${BLUE}Verify infrastructure:${NC}"
echo "    ./scripts/verify-infrastructure.sh"
echo ""
echo "  ${BLUE}Start all services:${NC}"
echo "    make up"
echo ""
exit 0
