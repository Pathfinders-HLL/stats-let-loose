-- SQL Script for PostgreSQL
-- Creates table for storing player death statistics by weapon type
-- Based on weapon_schemas.csv weapon types
-- Table name: player_death_stats

-- Main table for player death statistics by weapon type
-- Composite primary key: (player_id, match_id)
CREATE TABLE IF NOT EXISTS pathfinder_stats.player_death_stats (
    -- Composite primary key components
    player_id TEXT NOT NULL,
    match_id INTEGER NOT NULL,
    
    -- Player information
    player_name TEXT NOT NULL,
    team TEXT,  -- Team side: either 'allies' or 'axis'
    
    -- Weapon type death counts
    m1_garand INTEGER NOT NULL DEFAULT 0,
    m1_carbine INTEGER NOT NULL DEFAULT 0,
    m1918a2_bar INTEGER NOT NULL DEFAULT 0,
    thompson INTEGER NOT NULL DEFAULT 0,
    gewehr_43 INTEGER NOT NULL DEFAULT 0,
    grease_gun INTEGER NOT NULL DEFAULT 0,
    trench_gun INTEGER NOT NULL DEFAULT 0,
    mosin_nagant INTEGER NOT NULL DEFAULT 0,
    mp40 INTEGER NOT NULL DEFAULT 0,
    ppsh_41 INTEGER NOT NULL DEFAULT 0,
    lee_enfield INTEGER NOT NULL DEFAULT 0,
    stg44 INTEGER NOT NULL DEFAULT 0,
    svt40 INTEGER NOT NULL DEFAULT 0,
    sten_gun INTEGER NOT NULL DEFAULT 0,
    fg42 INTEGER NOT NULL DEFAULT 0,
    karabiner_98k INTEGER NOT NULL DEFAULT 0,
    pistol INTEGER NOT NULL DEFAULT 0,
    machine_gun INTEGER NOT NULL DEFAULT 0,
    sniper_rifle INTEGER NOT NULL DEFAULT 0,
    mine INTEGER NOT NULL DEFAULT 0,
    satchel INTEGER NOT NULL DEFAULT 0,
    at_rifle INTEGER NOT NULL DEFAULT 0,
    bazooka INTEGER NOT NULL DEFAULT 0,
    grenade INTEGER NOT NULL DEFAULT 0,
    flamethrower INTEGER NOT NULL DEFAULT 0,
    flare_gun INTEGER NOT NULL DEFAULT 0,
    melee INTEGER NOT NULL DEFAULT 0,
    at_gun INTEGER NOT NULL DEFAULT 0,
    artillery INTEGER NOT NULL DEFAULT 0,
    commander_abilities INTEGER NOT NULL DEFAULT 0,
    self_propelled_artillery INTEGER NOT NULL DEFAULT 0,
    armor_driver INTEGER NOT NULL DEFAULT 0,
    armor_gunner INTEGER NOT NULL DEFAULT 0,
    roadkill INTEGER NOT NULL DEFAULT 0,
    firespot INTEGER NOT NULL DEFAULT 0,
    unknown INTEGER NOT NULL DEFAULT 0,
    
    -- Composite primary key
    PRIMARY KEY (player_id, match_id)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_player_death_stats_match_id ON pathfinder_stats.player_death_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_player_death_stats_player_id ON pathfinder_stats.player_death_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_death_stats_player_name ON pathfinder_stats.player_death_stats(player_name);

-- Add comments for documentation
COMMENT ON TABLE pathfinder_stats.player_death_stats IS 'Stores player death statistics by weapon type for each match. Composite primary key: (player_id, match_id)';
COMMENT ON COLUMN pathfinder_stats.player_death_stats.player_id IS 'Player Steam ID or unique identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_death_stats.match_id IS 'Match/map identifier (part of composite primary key)';
COMMENT ON COLUMN pathfinder_stats.player_death_stats.player_name IS 'Player display name';
COMMENT ON COLUMN pathfinder_stats.player_death_stats.team IS 'Team side: either "axis" or "allies"';
COMMENT ON COLUMN pathfinder_stats.player_death_stats.sniper_rifle IS 'Sniper rifle deaths (note: column name reflects typo in source CSV)';

