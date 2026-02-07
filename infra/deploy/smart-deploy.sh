#!/bin/bash
# Smart deployment script using Docker Compose's built-in change detection
# Docker automatically detects build context changes and only rebuilds what's needed

set -e

echo "=========================================="
echo "Smart Deployment Script"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../docker"
cd "$DOCKER_DIR"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${RED}Error: .env file not found!${NC}"
    echo "Please create a .env file with required environment variables."
    exit 1
fi

echo -e "${BLUE}Building services (Docker will skip unchanged layers)...${NC}"
echo ""

# Build images - Docker BuildKit automatically:
# 1. Uses layer cache for unchanged files
# 2. Only rebuilds layers that changed
# 3. Reuses base image and dependency layers
docker compose build

echo ""
echo -e "${BLUE}Updating services...${NC}"
echo ""

# Start/update services - Docker Compose automatically:
# 1. Only recreates containers if their image ID changed
# 2. Leaves unchanged containers running (no restart)
# 3. Starts any stopped services
docker compose up -d

echo ""
echo -e "${BLUE}Waiting for services to be healthy...${NC}"

# Function to check if a service is healthy
check_service_health() {
    local service=$1
    local status=$(docker compose ps --format json "$service" 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4)
    
    # If no health check defined, check if container is running
    if [ -z "$status" ]; then
        status=$(docker compose ps --format json "$service" 2>/dev/null | grep -o '"State":"[^"]*"' | cut -d'"' -f4)
        if [ "$status" = "running" ]; then
            echo "running"
        else
            echo "unhealthy"
        fi
    else
        echo "$status"
    fi
}

# Function to get color for health status
get_status_color() {
    local status=$1
    case "$status" in
        "healthy"|"running")
            echo "$GREEN"
            ;;
        "starting"|"unhealthy")
            echo "$YELLOW"
            ;;
        "exited"|"dead"|"restarting")
            echo "$RED"
            ;;
        *)
            echo "$BLUE"
            ;;
    esac
}

# Function to print service status with color
print_service_status() {
    local service_name=$1
    local status=$2
    local color=$(get_status_color "$status")
    local status_symbol="●"
    
    case "$status" in
        "healthy"|"running")
            status_symbol="✓"
            ;;
        "starting")
            status_symbol="⟳"
            ;;
        "unhealthy"|"exited"|"dead")
            status_symbol="✗"
            ;;
    esac
    
    echo -e "  ${color}${status_symbol} ${service_name}: ${status}${NC}"
}

# Wait for all services to be healthy (max 120 seconds)
MAX_WAIT=120
ELAPSED=0
ALL_HEALTHY=false
LAST_STATUS=""

echo ""

while [ $ELAPSED -lt $MAX_WAIT ]; do
    POSTGRES_HEALTH=$(check_service_health "postgres")
    API_HEALTH=$(check_service_health "stats-api-ingestion")
    BOT_HEALTH=$(check_service_health "discord-stats-bot")
    
    # Create current status string for comparison
    CURRENT_STATUS="${POSTGRES_HEALTH}|${API_HEALTH}|${BOT_HEALTH}"
    
    # Print status when it changes or every 10 seconds
    if [ "$CURRENT_STATUS" != "$LAST_STATUS" ] || [ $((ELAPSED % 10)) -eq 0 ]; then
        echo -ne "\r\033[K"  # Clear current line
        echo -n "[${ELAPSED}s] "
        
        # Print all services on one line with colors
        pg_color=$(get_status_color "$POSTGRES_HEALTH")
        api_color=$(get_status_color "$API_HEALTH")
        bot_color=$(get_status_color "$BOT_HEALTH")
        
        echo -ne "${pg_color}postgres: ${POSTGRES_HEALTH}${NC} | "
        echo -ne "${api_color}api-ingestion: ${API_HEALTH}${NC} | "
        echo -ne "${bot_color}discord-bot: ${BOT_HEALTH}${NC}"
        
        LAST_STATUS="$CURRENT_STATUS"
    fi
    
    if [ "$POSTGRES_HEALTH" = "healthy" ] && \
       [ "$API_HEALTH" = "healthy" ] && \
       [ "$BOT_HEALTH" = "healthy" ]; then
        ALL_HEALTHY=true
        break
    fi
    
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

# Clear the status line and print final status
echo -ne "\r\033[K"

if [ "$ALL_HEALTHY" = true ]; then
    echo -e "${GREEN}✓ All services are healthy!${NC}"
    echo ""
    print_service_status "postgres" "$POSTGRES_HEALTH"
    print_service_status "stats-api-ingestion" "$API_HEALTH"
    print_service_status "discord-stats-bot" "$BOT_HEALTH"
else
    echo -e "${YELLOW}Warning: Some services may not be fully healthy yet${NC}"
    print_service_status "postgres" "$POSTGRES_HEALTH"
    print_service_status "stats-api-ingestion" "$API_HEALTH"
    print_service_status "discord-stats-bot" "$BOT_HEALTH"
    echo ""
    echo "Check logs for more details: docker compose logs -f"
fi

echo ""
echo "Service status:"
docker compose ps

echo ""
echo "Recent logs:"
docker compose logs --tail=20

echo ""
echo "=========================================="
echo -e "${GREEN}Smart deployment completed!${NC}"
echo "=========================================="
echo ""
echo "Docker Compose automatically:"
echo "  ✓ Rebuilt only services with changed code (using BuildKit cache)"
echo "  ✓ Recreated only containers with new images"
echo "  ✓ Left unchanged services running without restart"
echo ""
echo "Useful commands:"
echo "  View logs:        docker compose logs -f [service]"
echo "  Stop services:    docker compose down"
echo "  Service status:   docker compose ps"
echo "  Force rebuild:    docker compose build --no-cache && docker compose up -d"
