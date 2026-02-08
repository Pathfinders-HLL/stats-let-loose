#!/bin/bash
# Smart deployment script that only rebuilds and restarts services when their code changes

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
REPO_ROOT="$SCRIPT_DIR/../.."
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

# File to store deployment state
STATE_FILE="$DOCKER_DIR/.deployment-state"

# Function to compute hash of directory contents
compute_dir_hash() {
    local dir="$1"
    if [ -d "$dir" ]; then
        find "$dir" -type f \( -name "*.py" -o -name "*.txt" -o -name "*.sql" -o -name "*.yml" -o -name "*.yaml" -o -name "*.json" -o -name "*.sh" -o -name "Dockerfile*" \) -print0 2>/dev/null | \
            sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1
    else
        echo "none"
    fi
}

# Function to compute hash of a single file
compute_file_hash() {
    local file="$1"
    if [ -f "$file" ]; then
        sha256sum "$file" 2>/dev/null | cut -d' ' -f1
    else
        echo "none"
    fi
}

# Function to get stored hash for a service
get_stored_hash() {
    local service="$1"
    if [ -f "$STATE_FILE" ]; then
        grep "^${service}=" "$STATE_FILE" 2>/dev/null | cut -d'=' -f2 || echo "none"
    else
        echo "none"
    fi
}

# Function to save hash for a service
save_hash() {
    local service="$1"
    local hash="$2"
    
    touch "$STATE_FILE"
    grep -v "^${service}=" "$STATE_FILE" > "${STATE_FILE}.tmp" 2>/dev/null || true
    echo "${service}=${hash}" >> "${STATE_FILE}.tmp"
    mv "${STATE_FILE}.tmp" "$STATE_FILE"

    # If running under sudo, ensure the original user can write state file
    if [ -n "$SUDO_USER" ]; then
        chown "$SUDO_USER":"$SUDO_USER" "$STATE_FILE" 2>/dev/null || true
    fi
}

# Function to check if image exists
image_exists() {
    local service="$1"
    docker compose images -q "$service" 2>/dev/null | grep -q . && return 0 || return 1
}

echo -e "${BLUE}Analyzing which services need updates...${NC}"
echo ""

# Array to track services that need building/recreating
declare -a SERVICES_TO_BUILD
declare -a SERVICES_TO_RECREATE
RECREATE_ALL=false
RECREATE_POSTGRES=false

# Check API Ingestion Service
API_HASH=$(compute_dir_hash "$REPO_ROOT/apps/api_stats_ingestion")_$(compute_dir_hash "$REPO_ROOT/libs")_$(compute_file_hash "$DOCKER_DIR/Dockerfile.api-ingestion")
API_STORED=$(get_stored_hash "stats-api-ingestion")

if [ "$API_HASH" != "$API_STORED" ] || ! image_exists "stats-api-ingestion"; then
    SERVICES_TO_BUILD+=("stats-api-ingestion")
    echo -e "${YELLOW}✓ stats-api-ingestion needs rebuild${NC}"
else
    echo -e "${GREEN}✓ stats-api-ingestion is up to date${NC}"
fi

# Check Discord Bot Service
DISCORD_HASH=$(compute_dir_hash "$REPO_ROOT/apps/discord_stats_bot")_$(compute_dir_hash "$REPO_ROOT/libs")_$(compute_file_hash "$DOCKER_DIR/Dockerfile.discord-bot")
DISCORD_STORED=$(get_stored_hash "discord-stats-bot")

if [ "$DISCORD_HASH" != "$DISCORD_STORED" ] || ! image_exists "discord-stats-bot"; then
    SERVICES_TO_BUILD+=("discord-stats-bot")
    echo -e "${YELLOW}✓ discord-stats-bot needs rebuild${NC}"
else
    echo -e "${GREEN}✓ discord-stats-bot is up to date${NC}"
fi

# Check deploy config changes (compose only) that require recreation.
DEPLOY_CONFIG_HASH=$(compute_file_hash "$DOCKER_DIR/docker-compose.yml")
DEPLOY_CONFIG_STORED=$(get_stored_hash "deploy-config")

if [ "$DEPLOY_CONFIG_HASH" != "$DEPLOY_CONFIG_STORED" ]; then
    RECREATE_ALL=true
    echo -e "${YELLOW}✓ docker-compose.yml changed - will recreate services${NC}"
else
    echo -e "${GREEN}✓ docker-compose.yml unchanged${NC}"
fi

# Check Postgres config/init changes
POSTGRES_HASH=$(compute_dir_hash "$REPO_ROOT/sql")_$(compute_file_hash "$DOCKER_DIR/check-and-create-tables.sh")_$(compute_file_hash "$DOCKER_DIR/postgres-entrypoint.sh")_$(compute_file_hash "$DOCKER_DIR/ensure-postgres-config.sh")_$(compute_file_hash "$DOCKER_DIR/postgresql.conf")
POSTGRES_STORED=$(get_stored_hash "postgres-config")

if [ "$POSTGRES_HASH" != "$POSTGRES_STORED" ]; then
    RECREATE_POSTGRES=true
    echo -e "${YELLOW}✓ postgres config/init changed - will recreate postgres${NC}"
else
    echo -e "${GREEN}✓ postgres config/init unchanged${NC}"
fi

echo ""
echo "=========================================="

# If no services need building or recreating
if [ ${#SERVICES_TO_BUILD[@]} -eq 0 ] && [ "$RECREATE_ALL" = false ] && [ "$RECREATE_POSTGRES" = false ]; then
    echo -e "${GREEN}All services are up to date!${NC}"
    echo ""
    
    # Just ensure services are running (start if stopped, but never recreate)
    echo "Ensuring all services are running..."
    docker compose start 2>/dev/null || docker compose up -d
    
    echo ""
    echo "Service status:"
    docker compose ps
    
    echo ""
    echo -e "${GREEN}Deployment completed - no changes needed!${NC}"
    exit 0
fi

# Build only the services that need updates
echo -e "${BLUE}Building ${#SERVICES_TO_BUILD[@]} service(s)...${NC}"
echo ""

for service in "${SERVICES_TO_BUILD[@]}"; do
    echo -e "${YELLOW}Building $service...${NC}"
    docker compose build "$service"
done

# Save new hashes
if [[ " ${SERVICES_TO_BUILD[@]} " =~ " stats-api-ingestion " ]]; then
    save_hash "stats-api-ingestion" "$API_HASH"
fi

if [[ " ${SERVICES_TO_BUILD[@]} " =~ " discord-stats-bot " ]]; then
    save_hash "discord-stats-bot" "$DISCORD_HASH"
fi

# Save config hashes
save_hash "deploy-config" "$DEPLOY_CONFIG_HASH"
save_hash "postgres-config" "$POSTGRES_HASH"

echo ""
echo -e "${BLUE}Restarting updated services...${NC}"
echo ""

# Restart only the services that were rebuilt (recreates containers with new images)
for service in "${SERVICES_TO_BUILD[@]}"; do
    echo -e "${YELLOW}Restarting $service...${NC}"
    docker compose up -d "$service"
done

# Recreate services if docker-compose.yml changed
if [ "$RECREATE_ALL" = true ]; then
    SERVICES_TO_RECREATE=("postgres" "stats-api-ingestion" "discord-stats-bot")
fi

# Recreate postgres if config/init changed
if [ "$RECREATE_POSTGRES" = true ]; then
    if ! [[ " ${SERVICES_TO_RECREATE[@]} " =~ " postgres " ]]; then
        SERVICES_TO_RECREATE+=("postgres")
    fi
fi

# Apply recreates for services not already rebuilt
for service in "${SERVICES_TO_RECREATE[@]}"; do
    if ! [[ " ${SERVICES_TO_BUILD[@]} " =~ " $service " ]]; then
        echo -e "${YELLOW}Recreating $service...${NC}"
        docker compose up -d --force-recreate "$service"
    fi
done

# Ensure unchanged services are running (start if stopped, but never recreate)
echo ""
echo -e "${BLUE}Ensuring unchanged services are running...${NC}"

# Function to safely start a service without recreating
safe_start_service() {
    local service=$1
    # Try to start existing container first (won't recreate)
    if docker compose start "$service" 2>/dev/null; then
        return 0
    fi
    # If start failed, container might not exist - create it but don't recreate if exists
    docker compose up -d --no-recreate "$service" 2>/dev/null || true
}

# Start postgres if stopped (never recreate)
if ! [[ " ${SERVICES_TO_RECREATE[@]} " =~ " postgres " ]]; then
    safe_start_service "postgres"
fi

# Start other unchanged services if stopped
for service in "stats-api-ingestion" "discord-stats-bot"; do
    if ! [[ " ${SERVICES_TO_BUILD[@]} " =~ " $service " ]] && ! [[ " ${SERVICES_TO_RECREATE[@]} " =~ " $service " ]]; then
        safe_start_service "$service"
    fi
done

# Restart dependents if postgres was recreated
if [[ " ${SERVICES_TO_RECREATE[@]} " =~ " postgres " ]]; then
    for service in "stats-api-ingestion" "discord-stats-bot"; do
        if ! [[ " ${SERVICES_TO_BUILD[@]} " =~ " $service " ]] && ! [[ " ${SERVICES_TO_RECREATE[@]} " =~ " $service " ]]; then
            echo -e "${YELLOW}Restarting dependent $service after postgres change...${NC}"
            docker compose up -d "$service"
        fi
    done
fi

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
if [ ${#SERVICES_TO_BUILD[@]} -eq 0 ]; then
    echo "Summary:"
    echo "  ✓ No code changes detected"
    echo "  ✓ All services kept running (no restarts)"
else
    echo "Summary:"
    echo "  ✓ Rebuilt and restarted: ${SERVICES_TO_BUILD[@]}"
    echo "  ✓ Unchanged services: kept running (no restart)"
    echo "  ✓ PostgreSQL: never restarted"
fi
echo ""
echo "Useful commands:"
echo "  View logs:        docker compose logs -f [service]"
echo "  Stop services:    docker compose down"
echo "  Service status:   docker compose ps"
echo "  Force rebuild:    rm .deployment-state && bash smart-deploy.sh"
