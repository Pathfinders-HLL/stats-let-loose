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

-- Optimize: Window function queries for score columns (PARTITION BY player_id ORDER BY column DESC)
-- Used in: /leaderboard contributions command - speeds up ROW_NUMBER() window function
-- These indexes help with sorting within each player partition
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_combat_score 
ON pathfinder_stats.player_match_stats(player_id, combat_score DESC) 
WHERE combat_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_offense_score 
ON pathfinder_stats.player_match_stats(player_id, offense_score DESC) 
WHERE offense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_defense_score 
ON pathfinder_stats.player_match_stats(player_id, defense_score DESC) 
WHERE defense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_support_score 
ON pathfinder_stats.player_match_stats(player_id, support_score DESC) 
WHERE support_score > 0;

-- Optimize: Filtering by time_played >= 2700 (45+ minutes)
-- Used in: /leaderboard performance and /player performance commands
CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played 
ON pathfinder_stats.player_match_stats(time_played) 
WHERE time_played >= 2700;

-- Optimize: Composite index for match_history with start_time for time filtering
-- Used in: JOIN match_history ON match_id WHERE start_time >= $X
-- Helps with time-filtered queries across all tables
CREATE INDEX IF NOT EXISTS idx_match_history_match_id_start_time 
ON pathfinder_stats.match_history(match_id, start_time DESC);

-- Optimize: Case-insensitive map name searches
-- Used in: /player maps command (WHERE LOWER(mh.map_name) = LOWER($X))
CREATE INDEX IF NOT EXISTS idx_match_history_map_name_lower 
ON pathfinder_stats.match_history(LOWER(map_name));

-- Optimize: Case-insensitive player name searches
-- Used in: Finding players by name (LOWER(player_name) = LOWER(%s))
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_name_lower 
ON pathfinder_stats.player_match_stats(LOWER(player_name));

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_name_lower 
ON pathfinder_stats.player_kill_stats(LOWER(player_name));

-- Optimize: Pathfinder name filtering (ILIKE 'PFr |%' or 'PF |%')
-- Used in: only_pathfinders parameter filtering
-- Note: Queries use ILIKE which is case-insensitive, so we need LOWER() index
-- Partial index only indexes rows matching the pattern, reducing index size
CREATE INDEX IF NOT EXISTS idx_player_match_stats_pathfinder_names 
ON pathfinder_stats.player_match_stats(LOWER(player_name) text_pattern_ops) 
WHERE LOWER(player_name) LIKE 'pf%';

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_pathfinder_names 
ON pathfinder_stats.player_kill_stats(LOWER(player_name) text_pattern_ops) 
WHERE LOWER(player_name) LIKE 'pf%';

-- Optimize: Pathfinder player_id lookups for ANY() array matching and GROUP BY aggregations
-- Used in: only_pathfinders parameter filtering with player_id = ANY($array)
-- Also used in: /leaderboard weapon and /leaderboard alltime commands for GROUP BY player_id
-- Helps with array membership checks and aggregations
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id 
ON pathfinder_stats.player_match_stats(player_id);

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_id 
ON pathfinder_stats.player_kill_stats(player_id);

CREATE INDEX IF NOT EXISTS idx_player_death_stats_player_id 
ON pathfinder_stats.player_death_stats(player_id);

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
'Partial index for Pathfinder name filtering (ILIKE with LOWER for case-insensitive matching)';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_pathfinder_names IS 
'Partial index for Pathfinder name filtering in kill stats (ILIKE with LOWER for case-insensitive matching)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id IS 
'Index for player_id lookups, used in pathfinder filtering with ANY() array matching';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id IS 
'Index for player_id lookups in kill stats, used in pathfinder filtering and GROUP BY aggregations';

COMMENT ON INDEX pathfinder_stats.idx_player_death_stats_player_id IS 
'Index for player_id lookups in death stats, used in pathfinder filtering';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id IS 
'Index for player_id lookups in match stats, used in pathfinder filtering';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score IS 
'Composite index for window function queries in /leaderboard contributions (PARTITION BY player_id ORDER BY combat_score DESC)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_offense_score IS 
'Composite index for window function queries in /leaderboard contributions with offense score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_defense_score IS 
'Composite index for window function queries in /leaderboard contributions with defense score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_support_score IS 
'Composite index for window function queries in /leaderboard contributions with support score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played IS 
'Partial index for filtering matches where player played 45+ minutes (time_played >= 2700)';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_start_time IS 
'Composite index for JOIN queries with time filtering (JOIN match_history ON match_id WHERE start_time >= X)';

COMMENT ON INDEX pathfinder_stats.idx_match_history_map_name_lower IS 
'Expression index for case-insensitive map name searches (LOWER function)';

