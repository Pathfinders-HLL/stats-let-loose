#!/bin/bash
set -e

# Ensure scripts are executable (in case permissions weren't preserved from host)
chmod +x /check-and-create-tables.sh 2>/dev/null || true

# Function to run migrations after postgres is ready
run_migrations() {
    echo "Waiting for PostgreSQL to be ready for migrations..."
    until pg_isready -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-stats_let_loose}" > /dev/null 2>&1; do
        sleep 1
    done
    
    echo "PostgreSQL is ready. Running migrations..."
    /bin/bash /check-and-create-tables.sh || echo "Migration script completed (some tables may already exist)"
}

# Run migrations in the background (non-blocking)
run_migrations &

# The postgres image's entrypoint is at /usr/local/bin/docker-entrypoint.sh
# We need to call it to properly initialize postgres and handle all the setup
# Note: $@ already contains "postgres" and all the command arguments from docker-compose
ORIGINAL_ENTRYPOINT="/usr/local/bin/docker-entrypoint.sh"

if [ -f "$ORIGINAL_ENTRYPOINT" ]; then
    exec "$ORIGINAL_ENTRYPOINT" "$@"
else
    echo "Error: Could not find postgres entrypoint at $ORIGINAL_ENTRYPOINT"
    echo "Falling back to direct postgres execution..."
    exec "$@"
fi

