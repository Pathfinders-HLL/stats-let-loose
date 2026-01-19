-- Performance optimization indexes for StatsFinder database
-- These indexes are designed to speed up common query patterns used by the Discord bot
-- and API ingestion processes

-- ============================================================================
-- SECTION 1: Base indexes for JOIN and ORDER BY patterns
-- ============================================================================

-- Optimize: match_history queries with ORDER BY start_time DESC
-- Used in: Getting most recent player names (JOIN match_history, ORDER BY start_time DESC)
CREATE INDEX IF NOT EXISTS idx_match_history_start_time_desc 
ON pathfinder_stats.match_history(start_time DESC);

-- Optimize: match_history queries with time filtering and JOIN
-- Used in: Many queries that JOIN match_history and filter by start_time
CREATE INDEX IF NOT EXISTS idx_match_history_start_time_match_id 
ON pathfinder_stats.match_history(start_time DESC, match_id);

-- Optimize: Composite index for match_history with start_time for time filtering
-- Used in: JOIN match_history ON match_id WHERE start_time >= $X
CREATE INDEX IF NOT EXISTS idx_match_history_match_id_start_time 
ON pathfinder_stats.match_history(match_id, start_time DESC);

-- Optimize: player_match_stats JOIN match_history on match_id
-- Used in: Getting most recent player names - helps with JOIN performance
CREATE INDEX IF NOT EXISTS idx_player_match_stats_match_id_player_id 
ON pathfinder_stats.player_match_stats(match_id, player_id);

-- Optimize: player_match_stats queries for DISTINCT ON (player_id) with match_history join
-- Used in: DISTINCT ON (player_id) ORDER BY player_id, mh.start_time DESC
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_match_id 
ON pathfinder_stats.player_match_stats(player_id, match_id);

-- ============================================================================
-- SECTION 2: Indexes for player_kill_stats JOIN patterns
-- ============================================================================

-- Optimize: player_kill_stats JOIN match_history on match_id with date filtering
-- Used in: /topkills command filtering by start_time
CREATE INDEX IF NOT EXISTS idx_player_kill_stats_match_id_player_id 
ON pathfinder_stats.player_kill_stats(match_id, player_id);

-- Optimize: player_kill_stats queries filtering by player_id first, then joining
-- Used in: /player weapon command (single weapon and "All Weapons" queries)
CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_id_match_id 
ON pathfinder_stats.player_kill_stats(player_id, match_id);

-- ============================================================================
-- SECTION 3: Indexes for /player kills and /leaderboard kills commands
-- Pattern: WHERE player_id = X [AND mh.start_time >= Y] AND kill_column > 0 ORDER BY kill_column DESC
-- Also used for: ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY kill_column DESC)
-- ============================================================================

-- Optimize: Filtering by total_kills >= 100
-- Used in: /leaderboard 100killgames command
CREATE INDEX IF NOT EXISTS idx_player_match_stats_total_kills 
ON pathfinder_stats.player_match_stats(total_kills DESC) 
WHERE total_kills >= 100;

-- Optimize: Kill column queries with player_id filtering and window functions
-- Used in: /player kills, /leaderboard kills commands
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_kills 
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

-- ============================================================================
-- SECTION 4: Indexes for /player deaths and /leaderboard deaths commands
-- Pattern: WHERE player_id = X [AND mh.start_time >= Y] AND death_column > 0 ORDER BY death_column DESC
-- Also used for: ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY death_column DESC)
-- ============================================================================

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

-- ============================================================================
-- SECTION 5: Indexes for /player performance command
-- Pattern: WHERE player_id = X AND time_played >= 2700 ORDER BY stat_column DESC
-- ============================================================================

-- Base time_played filter index
CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played 
ON pathfinder_stats.player_match_stats(time_played) 
WHERE time_played >= 2700;

-- Composite indexes for player_id + stat with time_played filter
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kpm_time 
ON pathfinder_stats.player_match_stats(player_id, kills_per_minute DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kdr_time 
ON pathfinder_stats.player_match_stats(player_id, kill_death_ratio DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_dpm_time 
ON pathfinder_stats.player_match_stats(player_id, deaths_per_minute DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_kill_streak_time 
ON pathfinder_stats.player_match_stats(player_id, kill_streak DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_death_streak_time 
ON pathfinder_stats.player_match_stats(player_id, death_streak DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id_total_kills_time 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE time_played >= 2700;

-- ============================================================================
-- SECTION 6: Indexes for /player contributions and /leaderboard contributions commands
-- Pattern: WHERE player_id = X AND score_column > 0 ORDER BY score_column DESC
-- Also used for: ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY score_column DESC)
-- ============================================================================

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

-- ============================================================================
-- SECTION 7: Indexes for /leaderboard performance command
-- Pattern: WHERE time_played >= 2700 [AND mh.start_time >= Y] GROUP BY player_id ORDER BY AVG/MAX(stat_column) DESC
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kpm 
ON pathfinder_stats.player_match_stats(time_played, kills_per_minute DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kdr 
ON pathfinder_stats.player_match_stats(time_played, kill_death_ratio DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_dpm 
ON pathfinder_stats.player_match_stats(time_played, deaths_per_minute DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_kill_streak 
ON pathfinder_stats.player_match_stats(time_played, kill_streak DESC) 
WHERE time_played >= 2700;

CREATE INDEX IF NOT EXISTS idx_player_match_stats_time_played_death_streak 
ON pathfinder_stats.player_match_stats(time_played, death_streak DESC) 
WHERE time_played >= 2700;

-- ============================================================================
-- SECTION 8: Indexes for /player maps command
-- Pattern: WHERE player_id = X AND LOWER(mh.map_name) = LOWER($2) ORDER BY order_column DESC
-- ============================================================================

-- Expression index for case-insensitive map_name filtering
CREATE INDEX IF NOT EXISTS idx_match_history_map_name_lower 
ON pathfinder_stats.match_history(LOWER(map_name));

-- Composite index for JOIN + map_name filter
CREATE INDEX IF NOT EXISTS idx_match_history_match_id_map_name_lower 
ON pathfinder_stats.match_history(match_id, LOWER(map_name));

-- Player maps queries ordered by various stats
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
-- SECTION 9: Indexes for /player nemesis command
-- Pattern: WHERE player_id = X JOIN match_history [AND start_time >= Y] GROUP BY nemesis_name ORDER BY SUM(death_count) DESC
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_player_nemesis_player_id_nemesis_name 
ON pathfinder_stats.player_nemesis(player_id, nemesis_name);

CREATE INDEX IF NOT EXISTS idx_player_nemesis_player_id_death_count 
ON pathfinder_stats.player_nemesis(player_id, death_count DESC);

-- ============================================================================
-- SECTION 10: Indexes for /player victim command
-- Pattern: WHERE player_id = X JOIN match_history [AND start_time >= Y] GROUP BY victim_name ORDER BY SUM(kill_count) DESC
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_player_victim_player_id_victim_name 
ON pathfinder_stats.player_victim(player_id, victim_name);

CREATE INDEX IF NOT EXISTS idx_player_victim_player_id_kill_count 
ON pathfinder_stats.player_victim(player_id, kill_count DESC);

-- ============================================================================
-- SECTION 11: Indexes for name searches and Pathfinder filtering
-- ============================================================================

-- Case-insensitive player name searches
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_name_lower 
ON pathfinder_stats.player_match_stats(LOWER(player_name));

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_name_lower 
ON pathfinder_stats.player_kill_stats(LOWER(player_name));

-- Pathfinder name filtering (ILIKE 'PFr |%' or 'PF |%')
CREATE INDEX IF NOT EXISTS idx_player_match_stats_pathfinder_names 
ON pathfinder_stats.player_match_stats(LOWER(player_name) text_pattern_ops) 
WHERE LOWER(player_name) LIKE 'pf%';

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_pathfinder_names 
ON pathfinder_stats.player_kill_stats(LOWER(player_name) text_pattern_ops) 
WHERE LOWER(player_name) LIKE 'pf%';

-- Player ID lookups for ANY() array matching and GROUP BY aggregations
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player_id 
ON pathfinder_stats.player_match_stats(player_id);

CREATE INDEX IF NOT EXISTS idx_player_kill_stats_player_id 
ON pathfinder_stats.player_kill_stats(player_id);

CREATE INDEX IF NOT EXISTS idx_player_death_stats_player_id 
ON pathfinder_stats.player_death_stats(player_id);

-- ============================================================================
-- SECTION 12: Statistics and maintenance
-- ============================================================================

-- Update table statistics to help query planner
ANALYZE pathfinder_stats.match_history;
ANALYZE pathfinder_stats.player_match_stats;
ANALYZE pathfinder_stats.player_kill_stats;
ANALYZE pathfinder_stats.player_death_stats;
ANALYZE pathfinder_stats.player_nemesis;
ANALYZE pathfinder_stats.player_victim;

-- ============================================================================
-- SECTION 13: Index documentation comments
-- ============================================================================

-- Base JOIN indexes
COMMENT ON INDEX pathfinder_stats.idx_match_history_start_time_desc IS 
'Optimizes ORDER BY start_time DESC queries when joining match_history';

COMMENT ON INDEX pathfinder_stats.idx_match_history_start_time_match_id IS 
'Composite index for match_history JOIN queries with time filtering and ordering';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_start_time IS 
'Composite index for JOIN queries with time filtering (JOIN match_history ON match_id WHERE start_time >= X)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_match_id_player_id IS 
'Composite index for JOINs between player_match_stats and match_history on match_id';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_match_id IS 
'Composite index for DISTINCT ON (player_id) queries with match joins';

-- player_kill_stats indexes
COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_match_id_player_id IS 
'Composite index for JOINs between player_kill_stats and match_history on match_id';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id_match_id IS 
'Composite index for player_kill_stats queries filtering by player_id first. Used by /player weapon command';

-- Kill indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_total_kills IS 
'Partial index for filtering 100+ kill games (covers WHERE total_kills >= 100)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills IS 
'Composite index for /player kills and /leaderboard kills (PARTITION BY player_id ORDER BY total_kills DESC)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_infantry_kills IS 
'Composite index for kill queries with infantry filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_armor_kills IS 
'Composite index for kill queries with armor filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_artillery_kills IS 
'Composite index for kill queries with artillery filter';

-- Death indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_deaths IS 
'Composite index for /player deaths and /leaderboard deaths (PARTITION BY player_id ORDER BY total_deaths DESC)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_infantry_deaths IS 
'Composite index for death queries with infantry filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_armor_deaths IS 
'Composite index for death queries with armor filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_artillery_deaths IS 
'Composite index for death queries with artillery filter';

-- Performance command indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played IS 
'Partial index for filtering matches where player played 45+ minutes (time_played >= 2700)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kpm_time IS 
'Optimizes /player performance queries for KPM with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kdr_time IS 
'Optimizes /player performance queries for KDR with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_dpm_time IS 
'Optimizes /player performance queries for DPM with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kill_streak_time IS 
'Optimizes /player performance queries for kill streak with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_death_streak_time IS 
'Optimizes /player performance queries for death streak with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_time IS 
'Optimizes /player performance queries for most kills with time_played >= 2700 filter';

-- Contributions indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score IS 
'Composite index for /player contributions and /leaderboard contributions (combat score)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_offense_score IS 
'Composite index for /player contributions and /leaderboard contributions (offense score)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_defense_score IS 
'Composite index for /player contributions and /leaderboard contributions (defense score)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_support_score IS 
'Composite index for /player contributions and /leaderboard contributions (support score)';

-- Leaderboard performance indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kpm IS 
'Optimizes /leaderboard performance queries for KPM with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kdr IS 
'Optimizes /leaderboard performance queries for KDR with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_dpm IS 
'Optimizes /leaderboard performance queries for DPM with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_kill_streak IS 
'Optimizes /leaderboard performance queries for kill streak with time_played >= 2700 filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played_death_streak IS 
'Optimizes /leaderboard performance queries for death streak with time_played >= 2700 filter';

-- Maps command indexes
COMMENT ON INDEX pathfinder_stats.idx_match_history_map_name_lower IS 
'Expression index for case-insensitive map_name filtering (LOWER function)';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_map_name_lower IS 
'Composite index optimizing JOIN match_history ON match_id WHERE LOWER(map_name) = LOWER($2)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_maps IS 
'Optimizes /player maps queries ordered by total kills';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kdr_maps IS 
'Optimizes /player maps queries ordered by KDR';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kpm_maps IS 
'Optimizes /player maps queries ordered by KPM';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score_maps IS 
'Optimizes /player maps queries ordered by combat score';

-- Nemesis command indexes
COMMENT ON INDEX pathfinder_stats.idx_player_nemesis_player_id_nemesis_name IS 
'Optimizes /player nemesis queries with GROUP BY nemesis_name after filtering by player_id';

COMMENT ON INDEX pathfinder_stats.idx_player_nemesis_player_id_death_count IS 
'Optimizes /player nemesis queries for SUM(death_count) aggregation';

-- Victim command indexes
COMMENT ON INDEX pathfinder_stats.idx_player_victim_player_id_victim_name IS 
'Optimizes /player victim queries with GROUP BY victim_name after filtering by player_id';

COMMENT ON INDEX pathfinder_stats.idx_player_victim_player_id_kill_count IS 
'Optimizes /player victim queries for SUM(kill_count) aggregation';

-- Name search indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_name_lower IS 
'Expression index for case-insensitive player name searches (LOWER function)';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_name_lower IS 
'Expression index for case-insensitive player name searches in kill stats';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_pathfinder_names IS 
'Partial index for Pathfinder name filtering (ILIKE with LOWER for case-insensitive matching)';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_pathfinder_names IS 
'Partial index for Pathfinder name filtering in kill stats';

-- Player ID lookup indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id IS 
'Index for player_id lookups, used in pathfinder filtering with ANY() array matching';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id IS 
'Index for player_id lookups in kill stats, used in pathfinder filtering and GROUP BY aggregations';

COMMENT ON INDEX pathfinder_stats.idx_player_death_stats_player_id IS 
'Index for player_id lookups in death stats, used in pathfinder filtering';
