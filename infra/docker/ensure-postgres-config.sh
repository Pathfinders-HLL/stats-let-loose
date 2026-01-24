#!/bin/bash
set -e

# Script to ensure PostgreSQL configuration persists across restarts
# Uses ALTER SYSTEM to write to postgresql.auto.conf

echo "Ensuring PostgreSQL configuration is persisted..."

# Connection parameters
PGHOST="${POSTGRES_HOST:-localhost}"
PGPORT="${POSTGRES_PORT:-5432}"
PGUSER="${POSTGRES_USER:-postgres}"
PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"
PGDATABASE="${POSTGRES_DB:-stats_let_loose}"

# Export for psql
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready for configuration..."
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

echo "PostgreSQL is ready. Applying configuration..."

# Apply configuration using ALTER SYSTEM
# This writes to postgresql.auto.conf which persists across restarts
psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" <<-EOSQL
    -- Critical: shared_preload_libraries (requires restart to take effect)
    ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
    
    -- pg_stat_statements configuration
    ALTER SYSTEM SET pg_stat_statements.max = 500;
    ALTER SYSTEM SET pg_stat_statements.track = 'top';
    ALTER SYSTEM SET pg_stat_statements.track_utility = 'off';
    
    -- Memory configuration
    ALTER SYSTEM SET shared_buffers = '512MB';
    ALTER SYSTEM SET effective_cache_size = '1408MB';
    ALTER SYSTEM SET maintenance_work_mem = '256MB';
    ALTER SYSTEM SET work_mem = '16MB';
    
    -- WAL configuration
    ALTER SYSTEM SET checkpoint_completion_target = 0.9;
    ALTER SYSTEM SET wal_buffers = '64MB';
    
    -- Query planner configuration
    ALTER SYSTEM SET default_statistics_target = 100;
    ALTER SYSTEM SET random_page_cost = 1.1;
    ALTER SYSTEM SET effective_io_concurrency = 200;
    
    -- Connection configuration
    ALTER SYSTEM SET max_connections = 50;
    
    -- Parallel query configuration
    ALTER SYSTEM SET max_parallel_workers_per_gather = 2;
    ALTER SYSTEM SET max_parallel_workers = 4;
EOSQL

echo "Configuration applied successfully!"
echo ""
echo "NOTE: shared_preload_libraries requires a server restart to take effect."
echo "The configuration has been written to postgresql.auto.conf and will"
echo "persist across container restarts. After the next restart, run:"
echo "  SHOW shared_preload_libraries;"
echo "to verify it's set correctly."
