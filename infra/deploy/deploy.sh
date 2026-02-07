#!/bin/bash
# Manual deployment script for AWS Lightsail
# Leverages Docker's built-in BuildKit caching and change detection

set -e

echo "=========================================="
echo "StatsFinder Manual Deployment Script"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Navigate to deployment directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/../docker/.env" ]; then
    echo "Warning: .env file not found!"
    echo "Please create a .env file with required environment variables."
    echo "See infra/docker/README.md for details."
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "Running smart deployment..."
echo "Docker will automatically:"
echo "  - Use BuildKit to cache unchanged layers"
echo "  - Only recreate containers with new images"
echo "  - Leave unchanged services running"
echo ""

# Run the smart deployment script
bash "$SCRIPT_DIR/smart-deploy.sh"

echo ""
echo "=========================================="
echo "Manual deployment completed!"
echo "=========================================="
echo ""
echo "Useful commands:"
echo "  View logs:        cd infra/docker && docker compose logs -f"
echo "  Stop services:    cd infra/docker && docker compose down"
echo "  Restart:          cd infra/deploy && bash deploy.sh"
echo "  Force rebuild:    cd infra/docker && docker compose build --no-cache && docker compose up -d"
