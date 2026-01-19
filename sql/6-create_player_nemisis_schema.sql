-- SQL Script for PostgreSQL
-- Creates table for storing player nemesis statistics (who killed a player most)
-- Table name: player_nemesis

-- Main table for player nemesis statistics
-- Composite primary key: (player_id, match_id, nemesis_name)
CREATE TABLE IF NOT EXISTS pathfinder_stats.player_nemesis (
    -- Composite primary key components
    player_id TEXT NOT NULL,
    match_id INTEGER NOT NULL,
    
    -- Player information
    player_name TEXT NOT NULL,
    team TEXT,  -- Team side: either 'allies' or 'axis'
    
    -- Nemesis information
    nemesis_name TEXT NOT NULL,
    death_count INTEGER NOT NULL DEFAULT 0,
    
    -- Composite primary key
    PRIMARY KEY (player_id, match_id, nemesis_name)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_player_nemesis_match_id ON pathfinder_stats.player_nemesis(match_id);
CREATE INDEX IF NOT EXISTS idx_player_nemesis_player_id ON pathfinder_stats.player_nemesis(player_id);
CREATE INDEX IF NOT EXISTS idx_player_nemesis_player_name ON pathfinder_stats.player_nemesis(player_name);
CREATE INDEX IF NOT EXISTS idx_player_nemesis_nemesis_name ON pathfinder_stats.player_nemesis(nemesis_name);

-- Add comments for documentation
COMMENT ON TABLE pathfinder_stats.player_nemesis IS 'Stores which opponents killed a player the most in each match.';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.player_id IS 'Player Steam ID or unique identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.match_id IS 'Match/map identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.player_name IS 'Player display name';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.team IS 'Team side: either "axis" or "allies"';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.nemesis_name IS 'Opponent name that killed this player (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_nemesis.death_count IS 'Number of deaths by player to nemesis_name in the match';
