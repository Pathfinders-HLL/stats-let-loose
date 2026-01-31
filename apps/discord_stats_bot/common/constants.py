"""
Constants and configuration mappings used across Discord bot commands.
"""

import discord

from discord import app_commands

# =============================================================================
# Visual Branding
# =============================================================================

PATHFINDER_COLOR = discord.Color.from_rgb(16, 74, 0)

# =============================================================================
# Discord Limits
# =============================================================================

DISCORD_MESSAGE_MAX_LENGTH = 2000

# =============================================================================
# Query Result Limits
# =============================================================================

LEADERBOARD_RESULT_LIMIT = 25
PLAYER_TOP_MATCHES_LIMIT = 25
DEFAULT_COMPACT_VIEW_PLAYERS = 10

# =============================================================================
# Time/Quality Thresholds
# =============================================================================

MIN_PLAY_TIME_SECONDS = 2700  # 45 minutes
MIN_PLAY_TIME_MINUTES = 45
MIN_PLAYERS_PER_MATCH = 60

# =============================================================================
# Kill Type Configuration
# =============================================================================

KILL_TYPE_CONFIG = {
    "all": {
        "column": "total_kills",
        "display_name": "Kills"
    },
    "infantry": {
        "column": "infantry_kills",
        "display_name": "Infantry Kills"
    },
    "armor": {
        "column": "armor_kills",
        "display_name": "Armor Kills"
    },
    "artillery": {
        "column": "artillery_kills",
        "display_name": "Artillery Kills"
    }
}

KILL_TYPE_CHOICES = [
    app_commands.Choice(name="All Kills", value="all"),
    app_commands.Choice(name="Infantry Kills", value="infantry"),
    app_commands.Choice(name="Armor Kills", value="armor"),
    app_commands.Choice(name="Artillery Kills", value="artillery"),
]

KILL_TYPE_VALID_VALUES = {"all", "infantry", "armor", "artillery"}
KILL_TYPE_DISPLAY_LIST = ["All Kills", "Infantry Kills", "Armor Kills", "Artillery Kills"]

# =============================================================================
# Death Type Configuration
# =============================================================================

DEATH_TYPE_CONFIG = {
    "all": {
        "column": "total_deaths",
        "display_name": "Deaths"
    },
    "infantry": {
        "column": "infantry_deaths",
        "display_name": "Infantry Deaths"
    },
    "armor": {
        "column": "armor_deaths",
        "display_name": "Armor Deaths"
    },
    "artillery": {
        "column": "artillery_deaths",
        "display_name": "Artillery Deaths"
    }
}

DEATH_TYPE_CHOICES = [
    app_commands.Choice(name="All Deaths", value="all"),
    app_commands.Choice(name="Infantry Deaths", value="infantry"),
    app_commands.Choice(name="Armor Deaths", value="armor"),
    app_commands.Choice(name="Artillery Deaths", value="artillery"),
]

DEATH_TYPE_VALID_VALUES = {"all", "infantry", "armor", "artillery"}
DEATH_TYPE_DISPLAY_LIST = ["All Deaths", "Infantry Deaths", "Armor Deaths", "Artillery Deaths"]

# =============================================================================
# Score Type Configuration
# =============================================================================

SCORE_TYPE_CONFIG = {
    "support": {
        "column": "support_score",
        "display_name": "Support Score"
    },
    "attack": {
        "column": "offense_score",
        "display_name": "Attack Score"
    },
    "defense": {
        "column": "defense_score",
        "display_name": "Defense Score"
    },
    "combat": {
        "column": "combat_score",
        "display_name": "Combat Score"
    },
    "seeding": {
        "column": "time_played",
        "display_name": "Seeding Time"
    }
}

SCORE_TYPE_CHOICES = [
    app_commands.Choice(name="Support Score", value="support"),
    app_commands.Choice(name="Attack Score", value="attack"),
    app_commands.Choice(name="Defense Score", value="defense"),
    app_commands.Choice(name="Combat Score", value="combat"),
    app_commands.Choice(name="Seeding Time", value="seeding"),
]

SCORE_TYPE_VALID_VALUES = {"support", "attack", "defense", "combat", "seeding"}
SCORE_TYPE_DISPLAY_LIST = ["Support Score", "Attack Score", "Defense Score", "Combat Score", "Seeding Time"]

# =============================================================================
# Performance Stat Type Configuration
# =============================================================================

STAT_TYPE_CONFIG = {
    "kdr": {
        "column": "kill_death_ratio",
        "display_name": "KDR",
        "format": "{:.2f}",
        "is_streak": False
    },
    "kpm": {
        "column": "kills_per_minute",
        "display_name": "KPM",
        "format": "{:.2f}",
        "is_streak": False
    },
    "dpm": {
        "column": "deaths_per_minute",
        "display_name": "DPM (Deaths per Minute)",
        "format": "{:.2f}",
        "is_streak": False
    },
    "kill_streak": {
        "column": "kill_streak",
        "display_name": "Kill Streak",
        "format": "{:.0f}",
        "is_streak": True
    },
    "death_streak": {
        "column": "death_streak",
        "display_name": "Death Streak",
        "format": "{:.0f}",
        "is_streak": True
    }
}

STAT_TYPE_CHOICES = [
    app_commands.Choice(name="KDR (Kill/Death Ratio)", value="kdr"),
    app_commands.Choice(name="KPM (Kills per Minute)", value="kpm"),
    app_commands.Choice(name="DPM (Deaths per Minute)", value="dpm"),
    app_commands.Choice(name="Kill Streak", value="kill_streak"),
    app_commands.Choice(name="Death Streak", value="death_streak"),
]

STAT_TYPE_VALID_VALUES = {"kdr", "kpm", "dpm", "kill_streak", "death_streak"}
STAT_TYPE_DISPLAY_LIST = ["KDR", "KPM", "DPM", "Kill Streak", "Death Streak"]

# =============================================================================
# Aggregate By Configuration
# =============================================================================

AGGREGATE_BY_CHOICES = [
    app_commands.Choice(name="Average", value="average"),
    app_commands.Choice(name="Sum", value="sum"),
]

AGGREGATE_BY_VALID_VALUES = {"average", "sum"}
AGGREGATE_BY_DISPLAY_LIST = ["Average", "Sum"]

# =============================================================================
# Order By Configuration (for player maps)
# =============================================================================

ORDER_BY_CONFIG = {
    "kills": {
        "column": "total_kills",
        "display_name": "Kills",
        "format": "{:.0f}"
    },
    "kdr": {
        "column": "kill_death_ratio",
        "display_name": "KDR",
        "format": "{:.2f}"
    },
    "kpm": {
        "column": "kills_per_minute",
        "display_name": "KPM",
        "format": "{:.2f}"
    }
}

ORDER_BY_CHOICES = [
    app_commands.Choice(name="Kills", value="kills"),
    app_commands.Choice(name="KDR (Kill-Death Ratio)", value="kdr"),
    app_commands.Choice(name="KPM (Kills Per Minute)", value="kpm"),
]

ORDER_BY_VALID_VALUES = {"kills", "kdr", "kpm"}
ORDER_BY_DISPLAY_LIST = ["Kills", "KDR", "KPM"]

# =============================================================================
# Pathfinder Leaderboard Configuration
# =============================================================================

# Match quality thresholds for leaderboards
MIN_MATCH_DURATION_SECONDS = 2700  # 45 minutes (same as MIN_PLAY_TIME_SECONDS)
MIN_MATCHES_FOR_AGGREGATE = 5  # Minimum matches for aggregate stats

# Pagination settings
TOP_PLAYERS_LIMIT = 150  # Top 150 players per stat
PLAYERS_PER_PAGE = 25    # 25 players per page = 6 pages

# Timeframe options for leaderboard filtering
TIMEFRAME_OPTIONS = {
    "1d": {"days": 1, "label": "Last 24 Hours"},
    "7d": {"days": 7, "label": "Last 7 Days"},
    "30d": {"days": 30, "label": "Last 30 Days"},
    "all": {"days": 0, "label": "All Time"},
}

# Stat configuration for building embeds
LEADERBOARD_STAT_CONFIGS = [
    {
        "key": "infantry_kills",
        "title": "🎯 Most Infantry Kills",
        "compact_title": "🎯 Highest Kills",
        "value_label": "Kills",
        "value_abbrev": "Tot",  # 3-char abbreviation for compact view
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": f"Min {MIN_MATCHES_FOR_AGGREGATE} matches required"
    },
    {
        "key": "avg_kd",
        "title": "📊 Highest Average K/D",
        "compact_title": "📊 Top Average K/D",
        "value_label": "Avg K/D",
        "value_abbrev": "K/D",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": f"Min {MIN_MATCHES_FOR_AGGREGATE} matches, 45+ min each"
    },
    {
        "key": "single_match_kills",
        "title": "💥 Most Kills in Single Match",
        "compact_title": "💥 Most Single Match Kills",
        "value_label": "Kills",
        "value_abbrev": "Kil",
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": "Best single match performance"
    },
    {
        "key": "single_match_kd",
        "title": "⚔️ Best K/D in Single Match",
        "compact_title": "⚔️ Highest Single Match KDR",
        "value_label": "K/D",
        "value_abbrev": "K/D",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": "Best single match K/D ratio"
    },
    {
        "key": "k98_kills",
        "title": "🔫 Most Karabiner 98k Kills",
        "compact_title": "🔫 Most K98 Kills",
        "value_label": "K98 Kills",
        "value_abbrev": "K98",
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": "Total kills with Karabiner 98k"
    },
    {
        "key": "obj_efficiency",
        "title": "🏆 Highest Objective Efficiency",
        "compact_title": "🏆 Objective Efficiency",
        "value_label": "Pts/Min",
        "value_abbrev": "Pts",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": "(Offense + Defense) / Time Played per minute"
    },
]
