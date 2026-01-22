"""
Scheduled task to post comprehensive Pathfinder leaderboard statistics.

Posts top 10 players for multiple stat categories every 30 minutes,
with an interactive dropdown to view different timeframes.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple

import discord
from discord.ext import tasks

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    escape_sql_identifier,
    get_pathfinder_player_ids,
    build_pathfinder_filter,
)
from apps.discord_stats_bot.common.weapon_autocomplete import get_weapon_mapping
from apps.discord_stats_bot.config import get_bot_config

logger = logging.getLogger(__name__)

_bot_instance: Optional[discord.Client] = None

# Match quality thresholds
MIN_MATCH_DURATION_SECONDS = 2700  # 45 minutes
MIN_PLAYERS_PER_MATCH = 60
MIN_MATCHES_FOR_AGGREGATE = 5  # Minimum matches for stats #1, #2, #5

# Timeframe options
TIMEFRAME_OPTIONS = {
    "1d": {"days": 1, "label": "Last 24 Hours"},
    "7d": {"days": 7, "label": "Last 7 Days"},
    "30d": {"days": 30, "label": "Last 30 Days"},
    "all": {"days": 0, "label": "All Time"},
}


def _get_time_threshold(days: int) -> Optional[datetime]:
    """Get time threshold for filtering, or None for all-time."""
    if days <= 0:
        return None
    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    return threshold.replace(tzinfo=None)  # Convert to naive for DB


def _build_quality_match_subquery() -> str:
    """Build subquery to filter matches with 60+ players."""
    return """
        SELECT match_id 
        FROM pathfinder_stats.player_match_stats 
        GROUP BY match_id 
        HAVING COUNT(*) >= 60
    """


async def _get_most_infantry_kills(
    pool, 
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #1: Most infantry kills over the time period.
    Requires minimum 5 matches for qualification.
    """
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        # Build lateral pathfinder filter
        lateral_pf_filter, lateral_params, _ = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(lateral_params)
        
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    SUM(pms.infantry_kills) as total_infantry_kills,
                    COUNT(*) as match_count
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                    AND pms.match_id IN (SELECT match_id FROM qualified_matches)
                    {time_filter}
                    {pf_filter}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= {MIN_MATCHES_FOR_AGGREGATE}
            ),
            top_players AS (
                SELECT player_id, total_infantry_kills, match_count
                FROM player_stats
                ORDER BY total_infantry_kills DESC
                LIMIT 10
            )
            SELECT 
                tp.player_id,
                tp.total_infantry_kills as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            LEFT JOIN LATERAL (
                SELECT pms.player_name
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE pms.player_id = tp.player_id
                    {lateral_pf_filter}
                ORDER BY mh.start_time DESC
                LIMIT 1
            ) rn ON TRUE
            ORDER BY tp.total_infantry_kills DESC
        """
        
        results = await conn.fetch(query, *params)
        return [dict(row) for row in results]


async def _get_average_kd(
    pool,
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #2: Average K/D ratio over the time period.
    Requires minimum 5 matches for qualification.
    """
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        lateral_pf_filter, lateral_params, _ = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(lateral_params)
        
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    AVG(pms.kill_death_ratio) as avg_kd,
                    COUNT(*) as match_count
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                    AND pms.match_id IN (SELECT match_id FROM qualified_matches)
                    AND pms.time_played >= {MIN_MATCH_DURATION_SECONDS}
                    {time_filter}
                    {pf_filter}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= {MIN_MATCHES_FOR_AGGREGATE}
            ),
            top_players AS (
                SELECT player_id, avg_kd, match_count
                FROM player_stats
                ORDER BY avg_kd DESC
                LIMIT 10
            )
            SELECT 
                tp.player_id,
                ROUND(tp.avg_kd::numeric, 2) as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            LEFT JOIN LATERAL (
                SELECT pms.player_name
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE pms.player_id = tp.player_id
                    {lateral_pf_filter}
                ORDER BY mh.start_time DESC
                LIMIT 1
            ) rn ON TRUE
            ORDER BY tp.avg_kd DESC
        """
        
        results = await conn.fetch(query, *params)
        return [dict(row) for row in results]


async def _get_most_kills_single_match(
    pool,
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #3: Most infantry kills in a single match.
    No minimum matches required.
    """
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            )
            SELECT DISTINCT ON (pms.player_id)
                pms.player_id,
                pms.player_name,
                pms.infantry_kills as value,
                mh.map_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                AND pms.match_id IN (SELECT match_id FROM qualified_matches)
                {time_filter}
                {pf_filter}
            ORDER BY pms.player_id, pms.infantry_kills DESC
        """
        
        # First get best match per player, then sort by kills
        wrapper_query = f"""
            WITH best_matches AS ({query})
            SELECT player_id, player_name, value, map_name
            FROM best_matches
            ORDER BY value DESC
            LIMIT 10
        """
        
        results = await conn.fetch(wrapper_query, *params)
        return [dict(row) for row in results]


async def _get_best_kd_single_match(
    pool,
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #4: Best K/D ratio in a single match.
    No minimum matches required.
    """
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            )
            SELECT DISTINCT ON (pms.player_id)
                pms.player_id,
                pms.player_name,
                pms.kill_death_ratio as value,
                mh.map_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                AND pms.match_id IN (SELECT match_id FROM qualified_matches)
                AND pms.time_played >= {MIN_MATCH_DURATION_SECONDS}
                {time_filter}
                {pf_filter}
            ORDER BY pms.player_id, pms.kill_death_ratio DESC
        """
        
        wrapper_query = f"""
            WITH best_matches AS ({query})
            SELECT player_id, player_name, ROUND(value::numeric, 2) as value, map_name
            FROM best_matches
            ORDER BY value DESC
            LIMIT 10
        """
        
        results = await conn.fetch(wrapper_query, *params)
        return [dict(row) for row in results]


async def _get_most_k98_kills(
    pool,
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #5: Most Karabiner 98k kills over the time period.
    Requires minimum 5 matches for qualification.
    """
    weapon_mapping = get_weapon_mapping()
    column_name = weapon_mapping.get("karabiner 98k", "karabiner_98k")
    escaped_column = escape_sql_identifier(column_name)
    
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pks", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        lateral_pf_filter, lateral_params, _ = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(lateral_params)
        
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            ),
            player_stats AS (
                SELECT 
                    pks.player_id,
                    SUM(pks.{escaped_column}) as total_k98_kills,
                    COUNT(*) as match_count
                FROM pathfinder_stats.player_kill_stats pks
                INNER JOIN pathfinder_stats.match_history mh ON pks.match_id = mh.match_id
                WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                    AND pks.match_id IN (SELECT match_id FROM qualified_matches)
                    {time_filter}
                    {pf_filter}
                GROUP BY pks.player_id
                HAVING COUNT(*) >= {MIN_MATCHES_FOR_AGGREGATE}
                    AND SUM(pks.{escaped_column}) > 0
            ),
            top_players AS (
                SELECT player_id, total_k98_kills, match_count
                FROM player_stats
                ORDER BY total_k98_kills DESC
                LIMIT 10
            )
            SELECT 
                tp.player_id,
                tp.total_k98_kills as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            LEFT JOIN LATERAL (
                SELECT pms.player_name
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE pms.player_id = tp.player_id
                    {lateral_pf_filter}
                ORDER BY mh.start_time DESC
                LIMIT 1
            ) rn ON TRUE
            ORDER BY tp.total_k98_kills DESC
        """
        
        results = await conn.fetch(query, *params)
        return [dict(row) for row in results]


async def _get_avg_objective_efficiency(
    pool,
    time_threshold: Optional[datetime],
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #6: Average objective efficiency ((offense_score + defense_score) / time_played).
    Calculated per minute for readability.
    """
    async with pool.acquire() as conn:
        params = []
        param_num = 1
        
        time_filter = ""
        if time_threshold:
            time_filter = f"AND mh.start_time >= ${param_num}"
            params.append(time_threshold)
            param_num += 1
        
        pf_filter, pf_params, param_num = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(pf_params)
        
        lateral_pf_filter, lateral_params, _ = build_pathfinder_filter(
            "pms", param_num, pathfinder_ids, use_and=True
        )
        params.extend(lateral_params)
        
        # Calculate efficiency per minute: (offense + defense) / (time_played / 60)
        query = f"""
            WITH qualified_matches AS (
                {_build_quality_match_subquery()}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    AVG(
                        CASE WHEN pms.time_played > 0 
                        THEN (pms.offense_score + pms.defense_score)::float / (pms.time_played / 60.0)
                        ELSE 0 
                        END
                    ) as avg_obj_efficiency,
                    COUNT(*) as match_count
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}
                    AND pms.match_id IN (SELECT match_id FROM qualified_matches)
                    AND pms.time_played >= {MIN_MATCH_DURATION_SECONDS}
                    {time_filter}
                    {pf_filter}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= 3
            ),
            top_players AS (
                SELECT player_id, avg_obj_efficiency, match_count
                FROM player_stats
                ORDER BY avg_obj_efficiency DESC
                LIMIT 10
            )
            SELECT 
                tp.player_id,
                ROUND(tp.avg_obj_efficiency::numeric, 2) as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            LEFT JOIN LATERAL (
                SELECT pms.player_name
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                WHERE pms.player_id = tp.player_id
                    {lateral_pf_filter}
                ORDER BY mh.start_time DESC
                LIMIT 1
            ) rn ON TRUE
            ORDER BY tp.avg_obj_efficiency DESC
        """
        
        results = await conn.fetch(query, *params)
        return [dict(row) for row in results]


async def fetch_all_leaderboard_stats(
    days: int = 7
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch all leaderboard statistics for the given time period.
    
    Args:
        days: Number of days to look back (0 for all-time)
    
    Returns:
        Dictionary with stat category keys and result lists
    """
    pool = await get_readonly_db_pool()
    time_threshold = _get_time_threshold(days)
    pathfinder_ids = list(get_pathfinder_player_ids())
    
    stats = {
        "infantry_kills": await _get_most_infantry_kills(pool, time_threshold, pathfinder_ids),
        "avg_kd": await _get_average_kd(pool, time_threshold, pathfinder_ids),
        "single_match_kills": await _get_most_kills_single_match(pool, time_threshold, pathfinder_ids),
        "single_match_kd": await _get_best_kd_single_match(pool, time_threshold, pathfinder_ids),
        "k98_kills": await _get_most_k98_kills(pool, time_threshold, pathfinder_ids),
        "obj_efficiency": await _get_avg_objective_efficiency(pool, time_threshold, pathfinder_ids),
    }
    
    return stats


def _build_stat_embed(
    title: str,
    results: List[Dict[str, Any]],
    value_label: str,
    color: discord.Color,
    value_format: str = "int",
    footer_note: str = ""
) -> discord.Embed:
    """Build a single stat category embed with 3 columns."""
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        return embed
    
    ranks = []
    players = []
    values = []
    
    for rank, row in enumerate(results, 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        
        if value_format == "int":
            values.append(f"{int(value):,}")
        elif value_format == "float":
            values.append(f"{float(value):.2f}")
        else:
            values.append(str(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    if footer_note:
        embed.set_footer(text=footer_note)
    
    return embed


def build_leaderboard_embeds(
    stats: Dict[str, List[Dict[str, Any]]],
    timeframe_label: str
) -> List[discord.Embed]:
    """Build all leaderboard embeds from stats data."""
    # Color palette - using Pathfinder green variants
    green_dark = discord.Color.from_rgb(16, 74, 0)
    green_mid = discord.Color.from_rgb(34, 111, 14)
    green_light = discord.Color.from_rgb(52, 148, 28)
    olive = discord.Color.from_rgb(85, 107, 47)
    forest = discord.Color.from_rgb(34, 139, 34)
    lime = discord.Color.from_rgb(50, 205, 50)
    
    embeds = [
        _build_stat_embed(
            f"🎯 Most Infantry Kills ({timeframe_label})",
            stats.get("infantry_kills", []),
            "Kills",
            green_dark,
            value_format="int",
            footer_note=f"Min {MIN_MATCHES_FOR_AGGREGATE} matches required"
        ),
        _build_stat_embed(
            f"📊 Highest Average K/D ({timeframe_label})",
            stats.get("avg_kd", []),
            "Avg K/D",
            green_mid,
            value_format="float",
            footer_note=f"Min {MIN_MATCHES_FOR_AGGREGATE} matches, 45+ min each"
        ),
        _build_stat_embed(
            f"💥 Most Kills in Single Match ({timeframe_label})",
            stats.get("single_match_kills", []),
            "Kills",
            green_light,
            value_format="int",
            footer_note="Best single match performance"
        ),
        _build_stat_embed(
            f"⚔️ Best K/D in Single Match ({timeframe_label})",
            stats.get("single_match_kd", []),
            "K/D",
            olive,
            value_format="float",
            footer_note="Best single match K/D ratio"
        ),
        _build_stat_embed(
            f"🔫 Most Karabiner 98k Kills ({timeframe_label})",
            stats.get("k98_kills", []),
            "K98 Kills",
            forest,
            value_format="int",
            footer_note=f"Min {MIN_MATCHES_FOR_AGGREGATE} matches required"
        ),
        _build_stat_embed(
            f"🏆 Highest Objective Efficiency ({timeframe_label})",
            stats.get("obj_efficiency", []),
            "Pts/Min",
            lime,
            value_format="float",
            footer_note="(Offense + Defense) / Time Played per minute"
        ),
    ]
    
    return embeds


class TimeframeSelect(discord.ui.Select):
    """Dropdown select for choosing leaderboard timeframe."""
    
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Last 24 Hours",
                value="1d",
                description="View stats from the past day",
                emoji="📅"
            ),
            discord.SelectOption(
                label="Last 7 Days",
                value="7d",
                description="View stats from the past week",
                emoji="📆",
                default=True
            ),
            discord.SelectOption(
                label="Last 30 Days",
                value="30d",
                description="View stats from the past month",
                emoji="🗓️"
            ),
            discord.SelectOption(
                label="All Time",
                value="all",
                description="View all-time stats",
                emoji="♾️"
            ),
        ]
        super().__init__(
            custom_id="pathfinder_leaderboard_timeframe",
            placeholder="Select a timeframe...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle timeframe selection."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            selected = self.values[0]
            timeframe_config = TIMEFRAME_OPTIONS.get(selected, TIMEFRAME_OPTIONS["7d"])
            days = timeframe_config["days"]
            label = timeframe_config["label"]
            
            # Fetch stats for selected timeframe
            stats = await fetch_all_leaderboard_stats(days)
            embeds = build_leaderboard_embeds(stats, label)
            
            # Send ephemeral response with all embeds
            # Discord allows up to 10 embeds per message
            await interaction.followup.send(
                content=f"**Pathfinder Leaderboards - {label}**",
                embeds=embeds,
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Error in timeframe selection: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while fetching the leaderboards.",
                ephemeral=True
            )


class LeaderboardView(discord.ui.View):
    """Persistent view with timeframe selector for leaderboards."""
    
    def __init__(self):
        # Set timeout to None for persistent view
        super().__init__(timeout=None)
        self.add_item(TimeframeSelect())


@tasks.loop(minutes=30)
async def post_pathfinder_leaderboards():
    """Post comprehensive Pathfinder leaderboards, editing the previous message if possible."""
    global _bot_instance
    
    if not _bot_instance:
        logger.error("Bot instance not set for pathfinder leaderboards")
        return
    
    try:
        bot_config = get_bot_config()
        stats_channel_id = bot_config.stats_channel_id
        
        if not stats_channel_id:
            logger.warning("DISCORD_STATS_CHANNEL_ID not configured, skipping leaderboard posting")
            return
        
        channel = _bot_instance.get_channel(stats_channel_id)
        if not channel:
            logger.error(f"Channel {stats_channel_id} not found")
            return
        
        # Fetch stats for default 7-day period
        stats = await fetch_all_leaderboard_stats(days=7)
        embeds = build_leaderboard_embeds(stats, "Last 7 Days")
        
        # Create the view with timeframe selector
        view = LeaderboardView()
        
        # Get current timestamp for the update
        now_utc = datetime.now(timezone.utc)
        unix_timestamp = int(now_utc.timestamp())
        discord_time = f"<t:{unix_timestamp}:F>"
        
        header_content = (
            "# 🏅 Pathfinder Leaderboards\n"
            f"*Last updated: {discord_time}*\n"
            "*Use the dropdown below to view different timeframes*"
        )
        
        # Try to find and edit the last bot message
        try:
            logger.info("Looking for existing leaderboard message to edit...")
            last_message = None
            
            # Look through recent messages to find our leaderboard post
            async for message in channel.history(limit=10):
                if (message.author == _bot_instance.user and 
                    message.content.startswith("# 🏅 Pathfinder Leaderboards")):
                    last_message = message
                    break
            
            if last_message:
                logger.info(f"Found existing leaderboard message: {last_message.id}, editing...")
                await last_message.edit(
                    content=header_content,
                    embeds=embeds,
                    view=view
                )
                logger.info(f"Successfully edited leaderboard message {last_message.id}")
                return
            else:
                logger.info("No existing leaderboard message found")
                
        except discord.Forbidden as e:
            logger.error(f"Permission error accessing channel history: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error finding/editing leaderboard message: {e}", exc_info=True)
        
        # Send new message if editing failed
        logger.info("Sending new leaderboard message...")
        new_message = await channel.send(
            content=header_content,
            embeds=embeds,
            view=view
        )
        logger.info(f"Posted new leaderboard message {new_message.id}")
        
    except Exception as e:
        logger.error(f"Error in post_pathfinder_leaderboards task: {e}", exc_info=True)


def setup_pathfinder_leaderboards_task(bot: discord.Client) -> None:
    """Start the scheduled leaderboards posting task."""
    global _bot_instance
    _bot_instance = bot
    
    @post_pathfinder_leaderboards.before_loop
    async def before_leaderboards():
        await bot.wait_until_ready()
    
    if not post_pathfinder_leaderboards.is_running():
        post_pathfinder_leaderboards.start()
        logger.info("Started Pathfinder leaderboards task (every 30 min)")
    else:
        logger.warning("Pathfinder leaderboards task already running")
