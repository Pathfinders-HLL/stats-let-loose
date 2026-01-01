-- Additional performance optimization indexes for StatsFinder database
-- These indexes optimize queries in the Discord bot subcommands that were missing
-- Run this after 999-create_performance_indexes.sql

-- ============================================================================
-- Indexes for /player performance command
-- Pattern: WHERE player_id = X AND time_played >= 3600 ORDER BY stat_column DESC
-- ============================================================================

-- Optimize: Player performance queries filtering by player_id and time_played
-- Used in: /player performance command for KPM, KDR, DPM, streaks, and kills
-- These indexes help with WHERE player_id = X AND time_played >= 3600 ORDER BY column DESC
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kpm_time 
ON pathfinder_stats.player_match_stats(player_id, kills_per_minute DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kdr_time 
ON pathfinder_stats.player_match_stats(player_id, kill_death_ratio DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_dpm_time 
ON pathfinder_stats.player_match_stats(player_id, deaths_per_minute DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kill_streak_time 
ON pathfinder_stats.player_match_stats(player_id, kill_streak DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_death_streak_time 
ON pathfinder_stats.player_match_stats(player_id, death_streak DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_kills_time 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE time_played >= 3600;

-- ============================================================================
-- Indexes for /player contributions command
-- Pattern: WHERE player_id = X AND score_column > 0 ORDER BY score_column DESC
-- ============================================================================

-- Optimize: Player contributions queries filtering by player_id
-- Used in: /player contributions command for support, attack, defense, combat scores
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_support_score 
ON pathfinder_stats.player_match_stats(player_id, support_score DESC) 
WHERE support_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_offense_score 
ON pathfinder_stats.player_match_stats(player_id, offense_score DESC) 
WHERE offense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_defense_score 
ON pathfinder_stats.player_match_stats(player_id, defense_score DESC) 
WHERE defense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_combat_score 
ON pathfinder_stats.player_match_stats(player_id, combat_score DESC) 
WHERE combat_score > 0;

-- ============================================================================
-- Indexes for /player maps command
-- Pattern: WHERE player_id = X AND LOWER(mh.map_name) = LOWER($2) AND order_column > 0 ORDER BY order_column DESC
-- JOIN match_history ON match_id WHERE map_name filter
-- ============================================================================

-- Optimize: Expression index for case-insensitive map_name filtering
-- Used in: /player maps command filtering by map_name (LOWER(mh.map_name) = LOWER($2))
-- This allows efficient filtering on match_history by map_name regardless of case
CREATE INDEX IF NOT EXISTS idx_match_history_map_name_lower 
ON pathfinder_stats.match_history(LOWER(map_name));

-- Optimize: Composite index for JOIN + map_name filter
-- Used in: /player maps command joining match_history and filtering by map_name
-- This optimizes the JOIN ON match_id WHERE LOWER(map_name) = LOWER($2) pattern
CREATE INDEX IF NOT EXISTS idx_match_history_match_id_map_name_lower 
ON pathfinder_stats.match_history(match_id, LOWER(map_name));

-- Optimize: Player maps queries filtering by player_id
-- Used in: /player maps command ordered by kills, KDR, KPM, or combat score
-- Note: These help with filtering and ordering player_match_stats by player_id
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_kills_maps 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE total_kills > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kdr_maps 
ON pathfinder_stats.player_match_stats(player_id, kill_death_ratio DESC) 
WHERE kill_death_ratio > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kpm_maps 
ON pathfinder_stats.player_match_stats(player_id, kills_per_minute DESC) 
WHERE kills_per_minute > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_combat_score_maps 
ON pathfinder_stats.player_match_stats(player_id, combat_score DESC) 
WHERE combat_score > 0;

-- ============================================================================
-- Indexes for /player kills command (with time filtering)
-- Pattern: WHERE player_id = X AND mh.start_time >= Y AND kill_column > 0 ORDER BY kill_column DESC
-- ============================================================================

-- Note: These are already covered by existing indexes in 999-create_performance_indexes.sql
-- idx_player_match_stats_player_id_total_kills_window (already exists)
-- idx_player_match_stats_player_id_infantry_kills (already exists)
-- idx_player_match_stats_player_id_armor_kills (already exists)
-- idx_player_match_stats_player_id_artillery_kills (already exists)

-- ============================================================================
-- Indexes for /player deaths command (with time filtering)
-- Pattern: WHERE player_id = X AND mh.start_time >= Y AND death_column > 0 ORDER BY death_column DESC
-- ============================================================================

-- Note: These are already covered by existing indexes in 999-create_performance_indexes.sql
-- idx_player_match_stats_player_id_total_deaths (already exists)
-- idx_player_match_stats_player_id_infantry_deaths (already exists)
-- idx_player_match_stats_player_id_armor_deaths (already exists)
-- idx_player_match_stats_player_id_artillery_deaths (already exists)

-- ============================================================================
-- Indexes for /leaderboard performance command
-- Pattern: WHERE time_played >= 3600 [AND mh.start_time >= Y] GROUP BY player_id ORDER BY AVG/MAX(stat_column) DESC
-- ============================================================================

-- Optimize: Leaderboard performance queries filtering by time_played
-- Used in: /leaderboard performance command for KPM, KDR, DPM, streaks
-- These help with GROUP BY player_id queries that filter by time_played >= 3600
CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kpm 
ON pathfinder_stats.player_match_stats(time_played, kills_per_minute DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kdr 
ON pathfinder_stats.player_match_stats(time_played, kill_death_ratio DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_dpm 
ON pathfinder_stats.player_match_stats(time_played, deaths_per_minute DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kill_streak 
ON pathfinder_stats.player_match_stats(time_played, kill_streak DESC) 
WHERE time_played >= 3600;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_death_streak 
ON pathfinder_stats.player_match_stats(time_played, death_streak DESC) 
WHERE time_played >= 3600;

-- ============================================================================
-- Indexes for /leaderboard contributions command
-- Pattern: WHERE score_column > 0 [AND mh.start_time >= Y] PARTITION BY player_id ORDER BY score_column DESC
-- ============================================================================

-- Optimize: Leaderboard contributions queries using window functions
-- Used in: /leaderboard contributions command for support, attack, defense, combat scores
-- These help with ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY score_column DESC)
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_support_score_window 
ON pathfinder_stats.player_match_stats(player_id, support_score DESC) 
WHERE support_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_offense_score_window 
ON pathfinder_stats.player_match_stats(player_id, offense_score DESC) 
WHERE offense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_defense_score_window 
ON pathfinder_stats.player_match_stats(player_id, defense_score DESC) 
WHERE defense_score > 0;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_combat_score_window 
ON pathfinder_stats.player_match_stats(player_id, combat_score DESC) 
WHERE combat_score > 0;

-- ============================================================================
-- Indexes for match_history JOIN optimization
-- Pattern: JOIN match_history ON match_id WHERE start_time >= Y ORDER BY start_time DESC
-- ============================================================================

-- Optimize: match_history queries with time filtering
-- Used in: Many queries that JOIN match_history and filter by start_time
-- This composite index helps with JOIN + WHERE + ORDER BY patterns
CREATE INDEX IF NOT EXISTS idx_match_history_start_time_match_id 
ON pathfinder_stats.match_history(start_time DESC, match_id);

-- ============================================================================
-- Statistics and maintenance
-- ============================================================================

-- Update table statistics to help query planner
ANALYZE pathfinder_stats.match_history;
ANALYZE pathfinder_stats.player_match_stats;
ANALYZE pathfinder_stats.player_kill_stats;
ANALYZE pathfinder_stats.player_death_stats;

-- ============================================================================
-- Comments for documentation
-- ============================================================================

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kpm_time IS 
'Optimizes /player performance queries for KPM with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kdr_time IS 
'Optimizes /player performance queries for KDR with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_dpm_time IS 
'Optimizes /player performance queries for DPM with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kill_streak_time IS 
'Optimizes /player performance queries for kill streak with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_death_streak_time IS 
'Optimizes /player performance queries for death streak with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_time IS 
'Optimizes /player performance queries for most kills with time_played >= 3600 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_support_score IS 
'Optimizes /player contributions queries for support score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_offense_score IS 
'Optimizes /player contributions queries for attack/offense score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_defense_score IS 
'Optimizes /player contributions queries for defense score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score IS 
'Optimizes /player contributions queries for combat score';

COMMENT ON INDEX pathfinder_stats.idx_match_history_map_name_lower IS 
'Expression index for case-insensitive map_name filtering in /player maps command (LOWER(map_name) = LOWER($2))';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_map_name_lower IS 
'Composite index optimizing JOIN match_history ON match_id WHERE LOWER(map_name) = LOWER($2) in /player maps command';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_maps IS 
'Optimizes /player maps queries ordered by total kills';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kdr_maps IS 
'Optimizes /player maps queries ordered by KDR';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kpm_maps IS 
'Optimizes /player maps queries ordered by KPM';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score_maps IS 
'Optimizes /player maps queries ordered by combat score';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kpm IS 
'Optimizes /leaderboard performance queries for KPM with time_played filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kdr IS 
'Optimizes /leaderboard performance queries for KDR with time_played filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_dpm IS 
'Optimizes /leaderboard performance queries for DPM with time_played filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kill_streak IS 
'Optimizes /leaderboard performance queries for kill streak with time_played filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_death_streak IS 
'Optimizes /leaderboard performance queries for death streak with time_played filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_support_score_window IS 
'Optimizes /leaderboard contributions queries for support score using window functions';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_offense_score_window IS 
'Optimizes /leaderboard contributions queries for offense score using window functions';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_defense_score_window IS 
'Optimizes /leaderboard contributions queries for defense score using window functions';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score_window IS 
'Optimizes /leaderboard contributions queries for combat score using window functions';

COMMENT ON INDEX pathfinder_stats.idx_match_history_start_time_match_id IS 
'Composite index for match_history JOIN queries with time filtering and ordering';
