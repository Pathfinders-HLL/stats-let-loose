-- Schema for storing match history data
-- Create a database and schema for storing match history data
--CREATE DATABASE stats_let_loose;
CREATE SCHEMA IF NOT EXISTS pathfinder_stats;

-- Creates table for storing match history from get_scoreboard_maps API responses

-- Main table for match history
-- Each row represents a single match from the maps array
CREATE TABLE IF NOT EXISTS pathfinder_stats.match_history (
    -- Primary key: the match ID (from the 'id' field in each array entry)
    match_id INTEGER PRIMARY KEY,

    -- Map identifier from map.id field (e.g., "smolensk_warfare_day")
    map_id VARCHAR(255) NOT NULL,

    -- Map friendly name from map.map.pretty_name (e.g., "Smolensk")
    map_name VARCHAR(255) NOT NULL,

    -- Map short name from map.map.shortname (e.g., "Smolensk")
    map_short_name VARCHAR(100) NOT NULL,

    -- Game mode from map.game_mode (e.g., "warfare")
    game_mode VARCHAR(50) NOT NULL,

    -- Map environment from map.environment (e.g., "day", "dusk", "night")
    environment VARCHAR(50) NOT NULL,

    -- Score for Allies team from result.allied
    allies_score INTEGER NOT NULL,

    -- Score for Axis team from result.axis
    axis_score INTEGER NOT NULL,

    -- Winning team: 'Allies', 'Axis', or 'Tie'
    -- Calculated based on allies_score vs axis_score comparison
    -- Can be populated via Python script during data insertion
    winning_team VARCHAR(10) NOT NULL CHECK (winning_team IN ('Allies', 'Axis', 'Tie')),

    -- Match start time (ISO 8601 format: "2025-12-14T02:40:23")
    start_time TIMESTAMP NOT NULL,

    -- Match end time (ISO 8601 format: "2025-12-14T03:21:06")
    end_time TIMESTAMP NOT NULL,

    -- Match duration in seconds (calculated as end_time - start_time)
    -- Can be calculated using: EXTRACT(EPOCH FROM (end_time - start_time))
    -- Stored as INTEGER for easier querying and indexing
    match_duration INTEGER NOT NULL,

    -- Optional: Add timestamps for when the record was created/updated
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_match_history_map_id ON pathfinder_stats.match_history(map_id);
CREATE INDEX IF NOT EXISTS idx_match_history_map_name ON pathfinder_stats.match_history(map_name);
CREATE INDEX IF NOT EXISTS idx_match_history_game_mode ON pathfinder_stats.match_history(game_mode);
CREATE INDEX IF NOT EXISTS idx_match_history_environment ON pathfinder_stats.match_history(environment);
CREATE INDEX IF NOT EXISTS idx_match_history_start_time ON pathfinder_stats.match_history(start_time);
CREATE INDEX IF NOT EXISTS idx_match_history_end_time ON pathfinder_stats.match_history(end_time);
CREATE INDEX IF NOT EXISTS idx_match_history_winning_team ON pathfinder_stats.match_history(winning_team);
CREATE INDEX IF NOT EXISTS idx_match_history_duration ON pathfinder_stats.match_history(match_duration);

-- Add table and column comments for documentation
COMMENT ON TABLE pathfinder_stats.match_history IS 'Main table storing match history data from get_scoreboard_maps API responses';
COMMENT ON COLUMN pathfinder_stats.match_history.match_id IS 'Primary key, unique identifier for each match (from ''id'' field in JSON)';
COMMENT ON COLUMN pathfinder_stats.match_history.map_id IS 'Map identifier string (from ''map.id'' field)';
COMMENT ON COLUMN pathfinder_stats.match_history.map_name IS 'Friendly display name of the map (from ''map.map.pretty_name'' field)';
COMMENT ON COLUMN pathfinder_stats.match_history.map_short_name IS 'Short name/abbreviation of the map (from ''map.map.shortname'' field)';
COMMENT ON COLUMN pathfinder_stats.match_history.game_mode IS 'Type of game mode (e.g., "warfare", "offensive", etc.)';
COMMENT ON COLUMN pathfinder_stats.match_history.environment IS 'Map environment setting (e.g., "day", "dusk", "night")';
COMMENT ON COLUMN pathfinder_stats.match_history.allies_score IS 'Final score for the Allies team (from ''result.allied'' field)';
COMMENT ON COLUMN pathfinder_stats.match_history.axis_score IS 'Final score for the Axis team (from ''result.axis'' field)';
COMMENT ON COLUMN pathfinder_stats.match_history.winning_team IS 'Determined winner - ''Allies'' if allies_score > axis_score, ''Axis'' if axis_score > allies_score, ''Tie'' if equal';
COMMENT ON COLUMN pathfinder_stats.match_history.start_time IS 'Match start timestamp in ISO 8601 format';
COMMENT ON COLUMN pathfinder_stats.match_history.end_time IS 'Match end timestamp in ISO 8601 format';
COMMENT ON COLUMN pathfinder_stats.match_history.match_duration IS 'Duration of match in seconds (calculated from start_time and end_time)';

