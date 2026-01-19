-- SQL Script for PostgreSQL
-- Creates table for storing player victim statistics (who a player killed most)
-- Table name: player_victim

-- Main table for player victim statistics
-- Composite primary key: (player_id, match_id, victim_name)
CREATE TABLE IF NOT EXISTS pathfinder_stats.player_victim (
    -- Composite primary key components
    player_id TEXT NOT NULL,
    match_id INTEGER NOT NULL,
    
    -- Player information
    player_name TEXT NOT NULL,
    team TEXT,  -- Team side: either 'allies' or 'axis'
    
    -- Victim information
    victim_name TEXT NOT NULL,
    kill_count INTEGER NOT NULL DEFAULT 0,
    
    -- Composite primary key
    PRIMARY KEY (player_id, match_id, victim_name)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_player_victim_match_id ON pathfinder_stats.player_victim(match_id);
CREATE INDEX IF NOT EXISTS idx_player_victim_player_id ON pathfinder_stats.player_victim(player_id);
CREATE INDEX IF NOT EXISTS idx_player_victim_player_name ON pathfinder_stats.player_victim(player_name);
CREATE INDEX IF NOT EXISTS idx_player_victim_victim_name ON pathfinder_stats.player_victim(victim_name);

-- Add comments for documentation
COMMENT ON TABLE pathfinder_stats.player_victim IS 'Stores which opponents a player killed the most in each match.';
COMMENT ON COLUMN pathfinder_stats.player_victim.player_id IS 'Player Steam ID or unique identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_victim.match_id IS 'Match/map identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_victim.player_name IS 'Player display name';
COMMENT ON COLUMN pathfinder_stats.player_victim.team IS 'Team side: either "axis" or "allies"';
COMMENT ON COLUMN pathfinder_stats.player_victim.victim_name IS 'Opponent name that this player killed (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_victim.kill_count IS 'Number of kills by player against victim_name in the match';
