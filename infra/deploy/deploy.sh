#!/bin/bash
# Deployment script for AWS Lightsail
# This script can be run manually on the Lightsail instance

set -e

echo "=========================================="
echo "StatsFinder Deployment Script"
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

# Navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../docker"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Warning: .env file not found!"
    echo "Please create a .env file with required environment variables."
    echo "See infra/docker/README.md for details."
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "Building Docker images..."
docker compose build

echo "Stopping existing containers..."
docker compose down

echo "Starting services..."
docker compose up -d

echo "Waiting for services to be healthy..."
sleep 10

echo "Service status:"
docker compose ps

echo ""
echo "Recent logs:"
docker compose logs --tail=30

echo ""
echo "=========================================="
echo "Deployment completed!"
echo "=========================================="
echo ""
echo "Useful commands:"
echo "  View logs:        docker compose logs -f"
echo "  Stop services:    docker compose down"
echo "  Restart:          docker compose restart"
echo "  Service status:   docker compose ps"
