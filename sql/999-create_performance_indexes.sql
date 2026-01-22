-- Performance optimization indexes for StatsFinder database
-- These indexes are designed to speed up common query patterns used by the Discord bot
-- and API ingestion processes
--
-- NOTE: Indexes have storage and write-performance costs. This file focuses on
-- high-impact indexes for the most common query patterns rather than covering
-- every possible column combination.

-- ============================================================================
-- SECTION 1: Core indexes for match_history JOINs and filtering
-- ============================================================================

-- Optimize: match_history queries with time filtering and JOIN
-- Covers: ORDER BY start_time DESC, JOIN on match_id with time filters
DROP INDEX IF EXISTS pathfinder_stats.idx_match_history_start_time_match_id;
CREATE INDEX idx_match_history_start_time_match_id 
ON pathfinder_stats.match_history(start_time DESC, match_id);

-- Optimize: JOIN match_history ON match_id WHERE start_time >= $X
DROP INDEX IF EXISTS pathfinder_stats.idx_match_history_match_id_start_time;
CREATE INDEX idx_match_history_match_id_start_time 
ON pathfinder_stats.match_history(match_id, start_time DESC);

-- Optimize: match_history queries filtering by match_duration (45+ minute matches)
-- Used in: Pathfinder leaderboards WHERE mh.match_duration >= 2700
DROP INDEX IF EXISTS pathfinder_stats.idx_match_history_match_duration;
CREATE INDEX idx_match_history_match_duration
ON pathfinder_stats.match_history(match_duration)
WHERE match_duration >= 2700;

-- ============================================================================
-- SECTION 2: Core indexes for player_match_stats
-- ============================================================================

-- Optimize: JOINs between player_match_stats and match_history on match_id
-- Also supports GROUP BY match_id for quality match subqueries
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_match_id_player_id;
CREATE INDEX idx_player_match_stats_match_id_player_id 
ON pathfinder_stats.player_match_stats(match_id, player_id);

-- Optimize: DISTINCT ON (player_id) queries, player-specific lookups
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_match_id;
CREATE INDEX idx_player_match_stats_player_id_match_id 
ON pathfinder_stats.player_match_stats(player_id, match_id);

-- Optimize: Filtering by total_kills >= 100 (100 kill games leaderboard)
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_total_kills;
CREATE INDEX idx_player_match_stats_total_kills 
ON pathfinder_stats.player_match_stats(total_kills DESC) 
WHERE total_kills >= 100;

-- Optimize: /player kills, /leaderboard kills with total_kills ordering
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_total_kills;
CREATE INDEX idx_player_match_stats_player_id_total_kills 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE total_kills > 0;

-- Optimize: /player deaths, /leaderboard deaths with total_deaths ordering
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_total_deaths;
CREATE INDEX idx_player_match_stats_player_id_total_deaths 
ON pathfinder_stats.player_match_stats(player_id, total_deaths DESC) 
WHERE total_deaths > 0;

-- Optimize: /player contributions, /leaderboard contributions with combat_score ordering
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_combat_score;
CREATE INDEX idx_player_match_stats_player_id_combat_score 
ON pathfinder_stats.player_match_stats(player_id, combat_score DESC) 
WHERE combat_score > 0;

-- Optimize: Infantry kills aggregation for Pathfinder leaderboards (SUM GROUP BY player_id)
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_infantry_kills;
CREATE INDEX idx_player_match_stats_player_id_infantry_kills 
ON pathfinder_stats.player_match_stats(player_id, infantry_kills DESC) 
WHERE infantry_kills > 0;

-- Optimize: Offense/defense score for objective efficiency calculations
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_offense_score;
CREATE INDEX idx_player_match_stats_player_id_offense_score 
ON pathfinder_stats.player_match_stats(player_id, offense_score DESC) 
WHERE offense_score > 0;

-- ============================================================================
-- SECTION 3: Indexes for performance queries (time_played >= 2700 filter)
-- ============================================================================

-- Base time_played filter index for 45+ minute matches
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_time_played;
CREATE INDEX idx_player_match_stats_time_played 
ON pathfinder_stats.player_match_stats(time_played) 
WHERE time_played >= 2700;

-- Optimize: /player performance and /leaderboard performance for KPM
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_kpm_time;
CREATE INDEX idx_player_match_stats_player_id_kpm_time 
ON pathfinder_stats.player_match_stats(player_id, kills_per_minute DESC) 
WHERE time_played >= 2700;

-- Optimize: /player performance and /leaderboard performance for KDR
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_kdr_time;
CREATE INDEX idx_player_match_stats_player_id_kdr_time 
ON pathfinder_stats.player_match_stats(player_id, kill_death_ratio DESC) 
WHERE time_played >= 2700;

-- Optimize: /player performance for kill_streak
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_kill_streak_time;
CREATE INDEX idx_player_match_stats_player_id_kill_streak_time 
ON pathfinder_stats.player_match_stats(player_id, kill_streak DESC) 
WHERE time_played >= 2700;

-- Optimize: /player performance for total_kills with time filter
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_id_total_kills_time;
CREATE INDEX idx_player_match_stats_player_id_total_kills_time 
ON pathfinder_stats.player_match_stats(player_id, total_kills DESC) 
WHERE time_played >= 2700;

-- ============================================================================
-- SECTION 4: Indexes for player_kill_stats
-- ============================================================================

-- Optimize: JOINs between player_kill_stats and match_history on match_id
DROP INDEX IF EXISTS pathfinder_stats.idx_player_kill_stats_match_id_player_id;
CREATE INDEX idx_player_kill_stats_match_id_player_id 
ON pathfinder_stats.player_kill_stats(match_id, player_id);

-- Optimize: /player weapon command - queries filter by player_id first
DROP INDEX IF EXISTS pathfinder_stats.idx_player_kill_stats_player_id_match_id;
CREATE INDEX idx_player_kill_stats_player_id_match_id 
ON pathfinder_stats.player_kill_stats(player_id, match_id);

-- Optimize: /leaderboard weapon - GROUP BY player_id aggregation
DROP INDEX IF EXISTS pathfinder_stats.idx_player_kill_stats_player_id;
CREATE INDEX idx_player_kill_stats_player_id 
ON pathfinder_stats.player_kill_stats(player_id);

-- ============================================================================
-- SECTION 5: Indexes for /player nemesis and /player victim commands
-- ============================================================================

-- Nemesis: Composite for filtering and JOINs with time filter
DROP INDEX IF EXISTS pathfinder_stats.idx_player_nemesis_player_id_match_id;
CREATE INDEX idx_player_nemesis_player_id_match_id
ON pathfinder_stats.player_nemesis(player_id, match_id);

-- Nemesis: Supports GROUP BY nemesis_name and SUM(death_count) aggregation
DROP INDEX IF EXISTS pathfinder_stats.idx_player_nemesis_player_id_nemesis_name;
CREATE INDEX idx_player_nemesis_player_id_nemesis_name 
ON pathfinder_stats.player_nemesis(player_id, nemesis_name);

-- Victim: Composite for filtering and JOINs with time filter
DROP INDEX IF EXISTS pathfinder_stats.idx_player_victim_player_id_match_id;
CREATE INDEX idx_player_victim_player_id_match_id
ON pathfinder_stats.player_victim(player_id, match_id);

-- Victim: Supports GROUP BY victim_name and SUM(kill_count) aggregation
DROP INDEX IF EXISTS pathfinder_stats.idx_player_victim_player_id_victim_name;
CREATE INDEX idx_player_victim_player_id_victim_name 
ON pathfinder_stats.player_victim(player_id, victim_name);

-- ============================================================================
-- SECTION 6: Expression indexes for case-insensitive searches
-- ============================================================================

-- Case-insensitive player name searches
DROP INDEX IF EXISTS pathfinder_stats.idx_player_match_stats_player_name_lower;
CREATE INDEX idx_player_match_stats_player_name_lower 
ON pathfinder_stats.player_match_stats(LOWER(player_name));

-- Case-insensitive map_name filtering for /player maps command
DROP INDEX IF EXISTS pathfinder_stats.idx_match_history_map_name_lower;
CREATE INDEX idx_match_history_map_name_lower 
ON pathfinder_stats.match_history(LOWER(map_name));

-- Composite for JOIN + map filter
DROP INDEX IF EXISTS pathfinder_stats.idx_match_history_match_id_map_name_lower;
CREATE INDEX idx_match_history_match_id_map_name_lower 
ON pathfinder_stats.match_history(match_id, LOWER(map_name));

-- ============================================================================
-- SECTION 7: Indexes for player_death_stats
-- ============================================================================

-- Basic player_id lookup for death stats
DROP INDEX IF EXISTS pathfinder_stats.idx_player_death_stats_player_id;
CREATE INDEX idx_player_death_stats_player_id 
ON pathfinder_stats.player_death_stats(player_id);

-- ============================================================================
-- SECTION 8: Statistics and maintenance
-- ============================================================================

-- Update table statistics to help query planner
ANALYZE pathfinder_stats.match_history;
ANALYZE pathfinder_stats.player_match_stats;
ANALYZE pathfinder_stats.player_kill_stats;
ANALYZE pathfinder_stats.player_death_stats;
ANALYZE pathfinder_stats.player_nemesis;
ANALYZE pathfinder_stats.player_victim;

-- ============================================================================
-- SECTION 9: Index documentation
-- ============================================================================

-- match_history indexes
COMMENT ON INDEX pathfinder_stats.idx_match_history_start_time_match_id IS 
'Composite index for match_history JOIN queries with time filtering and ordering';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_start_time IS 
'Composite index for JOIN queries with time filtering (JOIN match_history ON match_id WHERE start_time >= X)';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_duration IS
'Partial index for match duration filtering (45+ minute matches)';

COMMENT ON INDEX pathfinder_stats.idx_match_history_map_name_lower IS 
'Expression index for case-insensitive map_name filtering';

COMMENT ON INDEX pathfinder_stats.idx_match_history_match_id_map_name_lower IS 
'Composite index for JOIN match_history ON match_id WHERE LOWER(map_name) = X';

-- player_match_stats indexes
COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_match_id_player_id IS 
'Composite index for JOINs on match_id and GROUP BY match_id aggregations';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_match_id IS 
'Composite index for DISTINCT ON (player_id) queries and player lookups';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_total_kills IS 
'Partial index for 100+ kill games leaderboard';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills IS 
'Composite index for /player kills and /leaderboard kills commands';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_deaths IS 
'Composite index for /player deaths and /leaderboard deaths commands';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_combat_score IS 
'Composite index for /player contributions and /leaderboard contributions commands';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_infantry_kills IS 
'Composite index for infantry kills aggregation in Pathfinder leaderboards';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_offense_score IS 
'Composite index for offense score aggregation and objective efficiency calculations';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_time_played IS 
'Partial index for 45+ minute match filtering (time_played >= 2700)';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kpm_time IS 
'Optimizes performance queries for kills per minute with time filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kdr_time IS 
'Optimizes performance queries for kill/death ratio with time filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_kill_streak_time IS 
'Optimizes performance queries for kill streak with time filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_id_total_kills_time IS 
'Optimizes performance queries for total kills with time filter';

COMMENT ON INDEX pathfinder_stats.idx_player_match_stats_player_name_lower IS 
'Expression index for case-insensitive player name searches';

-- player_kill_stats indexes
COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_match_id_player_id IS 
'Composite index for JOINs between player_kill_stats and match_history';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id_match_id IS 
'Composite index for /player weapon queries filtering by player_id first';

COMMENT ON INDEX pathfinder_stats.idx_player_kill_stats_player_id IS 
'Index for GROUP BY player_id aggregations in weapon leaderboards';

-- player_nemesis indexes
COMMENT ON INDEX pathfinder_stats.idx_player_nemesis_player_id_match_id IS
'Optimizes /player nemesis queries with JOIN match_history for time filtering';

COMMENT ON INDEX pathfinder_stats.idx_player_nemesis_player_id_nemesis_name IS 
'Optimizes /player nemesis GROUP BY nemesis_name aggregation';

-- player_victim indexes
COMMENT ON INDEX pathfinder_stats.idx_player_victim_player_id_match_id IS
'Optimizes /player victim queries with JOIN match_history for time filtering';

COMMENT ON INDEX pathfinder_stats.idx_player_victim_player_id_victim_name IS 
'Optimizes /player victim GROUP BY victim_name aggregation';

-- player_death_stats indexes
COMMENT ON INDEX pathfinder_stats.idx_player_death_stats_player_id IS 
'Index for player_id lookups in death stats';
