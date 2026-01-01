-- Performance optimization indexes for StatsFinder database
-- These indexes are designed to speed up common query patterns used by the Discord bot
-- and API ingestion processes

-- ============================================================================
-- Composite indexes for JOIN + ORDER BY patterns
-- ============================================================================

-- Optimize: match_history queries with ORDER BY start_time DESC
-- Used in: Getting most recent player names (JOIN match_history, ORDER BY start_time DESC)
-- Note: match_id is already indexed as PRIMARY KEY, but this helps with ORDER BY
CREATE INDEX IF NOT EXISTS idx_match_history_start_time_desc 
ON pathfinder_stats.match_history(start_time DESC);

-- Optimize: player_match_stats JOIN match_history on match_id
-- Used in: Getting most recent player names - helps with JOIN performance
-- Pattern: JOIN match_history ON match_id, then ORDER BY start_time
CREATE INDEX IF NOT EXISTS idx_player_match_stats_match_id_player_id 
ON pathfinder_stats.player_match_stats(match_id, player_id);

-- Optimize: player_match_stats queries for DISTINCT ON (player_id) with match_history join
-- Used in: DISTINCT ON (player_id) ORDER BY player_id, mh.start_time DESC
-- This composite index helps when joining and ordering
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_match_id 
ON pathfinder_stats.player_match_stats(player_id, match_id);

-- ============================================================================
-- Indexes for player_kill_stats JOIN patterns
-- ============================================================================

-- Optimize: player_kill_stats JOIN match_history on match_id with date filtering
-- Used in: /topkills command filtering by start_time
CREATE INDEX IF NOT EXISTS idx_player_kill_stats_match_id_player_id 
ON pathfinder_stats.player_kill_stats(match_id, player_id);

-- Optimize: player_kill_stats queries filtering by player_id first, then joining
-- Used in: /player weapon command (single weapon and "All Weapons" queries)
-- This index is optimal for WHERE player_id = X queries before JOIN
CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_id_match_id 
ON pathfinder_stats.player_kill_stats(player_id, match_id);

-- ============================================================================
-- Indexes for WHERE clause filtering
-- ============================================================================

-- Optimize: Filtering by total_kills >= 100
-- Used in: /top100killgames command
CREATE INDEX IF NOT EXISTS idx_player_match_stats_total_kills 
ON pathfinder_stats.player_match_stats(total_kills DESC) 
WHERE total_kills >= 100;

-- Optimize: Window function queries for death columns (PARTITION BY player_id ORDER BY column DESC)
-- Used in: /leaderboard deaths command - speeds up ROW_NUMBER() window function
-- These indexes help with sorting within each player partition
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_deaths 
ON pathfinder_stats.player_match_stats(player_id, total_deaths DESC) 
WHERE total_deaths > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_infantry_deaths 
ON pathfinder_stats.player_match_stats(player_id, infantry_deaths DESC) 
WHERE infantry_deaths > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_armor_deaths 
ON pathfinder_stats.player_match_stats(player_id, armor_deaths DESC) 
WHERE armor_deaths > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_artillery_deaths 
ON pathfinder_stats.player_match_stats(player_id, artillery_deaths DESC) 
WHERE artillery_deaths > 0;

-- Optimize: Window function queries for kill columns (PARTITION BY player_id ORDER BY column DESC)
-- Used in: /leaderboard kills command - speeds up ROW_NUMBER() window function
-- These indexes help with sorting within each player partition
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_kills_window 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE total_kills > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_infantry_kills 
ON pathfinder_stats.player_match_stats(player_id, infantry_kills DESC) 
WHERE infantry_kills > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_armor_kills 
ON pathfinder_stats.player_match_stats(player_id, armor_kills DESC) 
WHERE armor_kills > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_artillery_kills 
ON pathfinder_stats.player_match_stats(player_id, artillery_kills DESC) 
WHERE artillery_kills > 0;

-- Optimize: Case-insensitive player name searches
-- Used in: Finding players by name (LOWER(player_name) = LOWER(%s))
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_name_lower 
ON pathfinder_stats.player_match_stats(LOWER(player_name));

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_name_lower 
ON pathfinder_stats.player_kill_stats(LOWER(player_name));

-- Optimize: Pathfinder name filtering (LIKE 'PF%' or 'PFr%')
-- Used in: only_pathfinders parameter filtering
-- Note: 'PF%' pattern will match both 'PF |' and 'PFr |' since both start with 'PF'
-- Using text_pattern_ops operator class for optimal LIKE prefix matching performance
-- Partial index only indexes rows matching the pattern, reducing index size
CREATE INDEX IF NOT EXISTS idx_player_match_stats_pathfinder_names 
ON pathfinder_stats.player_match_stats(player_name text_pattern_ops) 
WHERE player_name LIKE 'PF%';

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_pathfinder_names 
ON pathfinder_stats.player_kill_stats(player_name text_pattern_ops) 
WHERE player_name LIKE 'PF%';

-- ============================================================================
-- Covering indexes for common SELECT patterns
-- ============================================================================

-- Optimize: Getting player_id and player_name together (covering index)
-- Used in: Various queries that select both player_id and player_name
-- Note: PostgreSQL doesn't support true covering indexes, but composite indexes help

-- ============================================================================
-- Statistics and maintenance
-- ============================================================================

-- Update table statistics to help query planner
ANALYZE pathfinder_stats.match_history;
ANALYZE pathfinder_stats.player_match_stats;
ANALYZE pathfinder_stats.player_kill_stats;
ANALYZE pathfinder_stats.player_death_stats;

-- Add comments for documentation
COMMENT ON INDEX pathfinder_stats.idx_match_history_start_time_desc IS 
'Optimizes ORDER BY start_time DESC queries when joining match_history';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_match_id_player_id IS 
'Composite index for JOINs between player_match_stats and match_history on match_id. Also used by /topaverages command';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_match_id IS 
'Composite index for DISTINCT ON (player_id) queries with match joins';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_match_id_player_id IS 
'Composite index for JOINs between player_kill_stats and match_history on match_id';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id_match_id IS 
'Composite index for player_kill_stats queries filtering by player_id first, then joining on match_id. Used by /player weapon command';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_total_kills IS 
'Partial index for filtering 100+ kill games (covers WHERE total_kills >= 100)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_deaths IS 
'Composite index for window function queries in /leaderboard deaths (PARTITION BY player_id ORDER BY total_deaths DESC)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_infantry_deaths IS 
'Composite index for window function queries in /leaderboard deaths with infantry filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_armor_deaths IS 
'Composite index for window function queries in /leaderboard deaths with armor filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_artillery_deaths IS 
'Composite index for window function queries in /leaderboard deaths with artillery filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_window IS 
'Composite index for window function queries in /leaderboard kills (PARTITION BY player_id ORDER BY total_kills DESC)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_infantry_kills IS 
'Composite index for window function queries in /leaderboard kills with infantry filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_armor_kills IS 
'Composite index for window function queries in /leaderboard kills with armor filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_artillery_kills IS 
'Composite index for window function queries in /leaderboard kills with artillery filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_name_lower IS 
'Expression index for case-insensitive player name searches (LOWER function)';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_name_lower IS 
'Expression index for case-insensitive player name searches in kill stats';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_pathfinder_names IS 
'Partial index for Pathfinder name filtering (LIKE PF% or PFr%)';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_pathfinder_names IS 
'Partial index for Pathfinder name filtering in kill stats (LIKE PF% or PFr%)';

