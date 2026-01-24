#!/bin/bash
set -e

echo "Checking and creating database tables if needed..."

# Connection parameters - Use localhost when running inside postgres container, otherwise use POSTGRES_HOST
PGHOST="${POSTGRES_HOST:-localhost}"
PGPORT="${POSTGRES_PORT:-5432}"
PGUSER="${POSTGRES_USER:-postgres}"
PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"
PGDATABASE="${POSTGRES_DB:-stats_let_loose}"

# Export for psql
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "PostgreSQL is ready. Checking tables..."

# SQL directory path
SQL_DIR="/docker-entrypoint-initdb.d/sql"

# Re--usabble Function to check if a table exists
table_exists() {
    local table_name=$1
    local schema_name=$2
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tAc \
        "SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '$schema_name' 
            AND table_name = '$table_name'
        );" | grep -q t
}

# Create the pathfinder_stats schema if it doesn't exist
echo "Ensuring schema exists..."
psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS pathfinder_stats;
EOSQL

# Enable pg_stat_statements extension for query performance tracking
echo "Enabling pg_stat_statements extension..."
psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EOSQL

# Check and create tables in order
SCHEMA="pathfinder_stats"

# Table: match_history (tracks only the matches)
if ! table_exists "match_history" "$SCHEMA"; then
    echo "Table match_history does not exist. Creating..."
    psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_DIR/1-create_match_history_schema.sql"
else
    echo "Table match_history already exists. Skipping."
fi

# Table: player_match_stats (to track player stats per match)
if ! table_exists "player_match_stats" "$SCHEMA"; then
    echo "Table player_match_stats does not exist. Creating..."
    psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_DIR/2-create_player_match_stats_schema.sql"
else
    echo "Table player_match_stats already exists. Skipping."
fi

# Table: player_kill_stats (to track player kill stats per match)
if ! table_exists "player_kill_stats" "$SCHEMA"; then
    echo "Table player_kill_stats does not exist. Creating..."
    psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_DIR/3-create_player_kill_stats_schema.sql"
else
    echo "Table player_kill_stats already exists. Skipping."
fi

# Table: player_death_stats (to track player death stats per match)
if ! table_exists "player_death_stats" "$SCHEMA"; then
    echo "Table player_death_stats does not exist. Creating..."
    psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_DIR/4-create_player_death_stats_schema.sql"
else
    echo "Table player_death_stats already exists. Skipping."
fi

# Performance indexes: (always run - indexes are idempotent with IF NOT EXISTS)
echo "Creating/updating performance indexes..."
if [ -f "$SQL_DIR/999-create_performance_indexes.sql" ]; then
    psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" -f "$SQL_DIR/999-create_performance_indexes.sql"
    echo "Performance indexes created/updated."
else
    echo "Warning: Performance indexes file not found at $SQL_DIR/999-create_performance_indexes.sql"
fi

# USERS: Setup users idempotent (checks if users exist)
# pathfinder_ingestion user - used for ingesting the API results into the database in the api-ingestion project (write permissions)
# pathfinder_ro user - used for reading the database in the discord_stats_bot project (read-only permissions)
echo "Setting up users..."
INGESTION_PASSWORD="${POSTGRES_INGESTION_PASSWORD}"
RO_PASSWORD="${POSTGRES_RO_PASSWORD}"

# Escape single quotes in passwords for SQL safety
INGESTION_PASSWORD_ESCAPED=$(echo "$INGESTION_PASSWORD" | sed "s/'/''/g")
RO_PASSWORD_ESCAPED=$(echo "$RO_PASSWORD" | sed "s/'/''/g")

psql -h "$PGHOST" -p "$PGPORT" -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" <<-EOSQL
    -- Create or update ingestion user
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'pathfinder_ingestion') THEN
            EXECUTE 'CREATE USER pathfinder_ingestion WITH PASSWORD ' || quote_literal('$INGESTION_PASSWORD_ESCAPED');
        ELSE
            EXECUTE 'ALTER USER pathfinder_ingestion WITH PASSWORD ' || quote_literal('$INGESTION_PASSWORD_ESCAPED');
        END IF;
    END
    \$\$;
    
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pathfinder_stats TO pathfinder_ingestion;
    ALTER DEFAULT PRIVILEGES IN SCHEMA pathfinder_stats GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pathfinder_ingestion;
    GRANT USAGE ON SCHEMA pathfinder_stats TO pathfinder_ingestion;

    -- Create or update read-only user
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'pathfinder_ro') THEN
            EXECUTE 'CREATE USER pathfinder_ro WITH PASSWORD ' || quote_literal('$RO_PASSWORD_ESCAPED');
        ELSE
            EXECUTE 'ALTER USER pathfinder_ro WITH PASSWORD ' || quote_literal('$RO_PASSWORD_ESCAPED');
        END IF;
    END
    \$\$;

    GRANT SELECT ON ALL TABLES IN SCHEMA pathfinder_stats TO pathfinder_ro;
    ALTER DEFAULT PRIVILEGES IN SCHEMA pathfinder_stats GRANT SELECT ON TABLES TO pathfinder_ro;
    GRANT USAGE ON SCHEMA pathfinder_stats TO pathfinder_ro;
EOSQL

echo "Table check and creation completed successfully!"

