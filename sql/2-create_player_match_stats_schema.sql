-- SQL Script for PostgreSQL
-- Creates table for storing player match statistics from result.player_stats array
-- Based on the schema from match_updater/example_data JSON files
-- Table name: player_match_stats

-- Main table for player match statistics
-- Composite primary key: (player_id, match_id)
CREATE TABLE IF NOT EXISTS pathfinder_stats.player_match_stats (
    -- Composite primary key components
    player_id TEXT NOT NULL,
    match_id INTEGER NOT NULL,
    
    -- Player information
    player_name TEXT NOT NULL,
    team TEXT,  -- from team.side field (values: 'allies', 'axis')
    
    -- Basic statistics
    total_kills INTEGER NOT NULL DEFAULT 0,  -- from kills field
    total_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths field
    kill_streak INTEGER NOT NULL DEFAULT 0,  -- from kills_streak field
    death_streak INTEGER DEFAULT 0,  -- from deaths_streak field (may not exist in all records)
    kills_per_minute NUMERIC(10, 2) NOT NULL DEFAULT 0.0,  -- from kills_per_minute field
    deaths_per_minute NUMERIC(10, 2) NOT NULL DEFAULT 0.0,  -- from deaths_per_minute field
    kill_death_ratio NUMERIC(10, 2) NOT NULL DEFAULT 0.0,  -- from kill_death_ratio field
    time_played INTEGER NOT NULL DEFAULT 0,  -- from time_played field
    
    -- Score statistics
    combat_score INTEGER NOT NULL DEFAULT 0,  -- from combat field
    offense_score INTEGER NOT NULL DEFAULT 0,  -- from offense field
    defense_score INTEGER NOT NULL DEFAULT 0,  -- from defense field
    support_score INTEGER NOT NULL DEFAULT 0,  -- from support field
    
    -- Life statistics (in seconds)
    shortest_life INTEGER NOT NULL DEFAULT 0,  -- from shortest_life_secs field
    longest_life INTEGER NOT NULL DEFAULT 0,  -- from longest_life_secs field
    
    -- Teamkill statistics
    teamkills INTEGER NOT NULL DEFAULT 0,  -- from teamkills field
    
    -- Kills by type
    infantry_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.infantry
    grenade_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.grenade
    machine_gun_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.machine_gun
    sniper_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.sniper
    artillery_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.artillery
    bazooka_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.bazooka
    mine_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.mine
    satchel_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.satchel
    commander_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.commander
    armor_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.armor
    pak_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.pak
    spa_kills INTEGER NOT NULL DEFAULT 0,  -- from kills_by_type.self_propelled_artillery
    
    -- Deaths by type
    infantry_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.infantry
    grenade_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.grenade
    machine_gun_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.machine_gun
    sniper_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.sniper
    artillery_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.artillery
    bazooka_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.bazooka
    mine_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.mine
    satchel_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.satchel
    commander_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.commander
    armor_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.armor
    pak_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.pak
    spa_deaths INTEGER NOT NULL DEFAULT 0,  -- from deaths_by_type.self_propelled_artillery
    
    -- Raw JSON data
    raw_info JSONB,  -- stores the entire JSON entry being scanned
    
    -- Composite primary key
    PRIMARY KEY (player_id, match_id)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_player_match_stats_match_id ON pathfinder_stats.player_match_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id ON pathfinder_stats.player_match_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_name ON pathfinder_stats.player_match_stats(player_name);

-- Add comments for documentation
COMMENT ON TABLE pathfinder_stats.player_match_stats IS 'Stores player statistics for each match. Composite primary key: (player_id, match_id)';
COMMENT ON COLUMN pathfinder_stats.player_match_stats.player_id IS 'Player Steam ID or unique identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_match_stats.match_id IS 'Match/map identifier from map_id field (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_match_stats.team IS 'Team side: either "axis" or "allies" from team.side field';
COMMENT ON COLUMN pathfinder_stats.player_match_stats.raw_info IS 'Stores the entire JSON entry from result.player_stats array as JSONB for flexible querying';

