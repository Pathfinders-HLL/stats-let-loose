"""
Constants and configuration mappings used across multiple subcommands.
"""

# Kill type configuration mapping
KILL_TYPE_CONFIG = {
    "all": {
        "column": "total_kills",
        "display_name": "All Kills"
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

# Death type configuration mapping
DEATH_TYPE_CONFIG = {
    "all": {
        "column": "total_deaths",
        "display_name": "All Deaths"
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

# Score type configuration mapping
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
    }
}

# Stat type configuration mapping
STAT_TYPE_CONFIG = {
    "kdr": {
        "column": "kill_death_ratio",
        "display_name": "KDR (Kill/Death Ratio)",
        "format": "{:.2f}"
    },
    "kpm": {
        "column": "kills_per_minute",
        "display_name": "KPM (Kills per Minute)",
        "format": "{:.2f}"
    },
    "dpm": {
        "column": "deaths_per_minute",
        "display_name": "DPM (Deaths per Minute)",
        "format": "{:.2f}"
    },
    "kill_streak": {
        "column": "kill_streak",
        "display_name": "Kill Streak",
        "format": "{:.0f}"
    },
    "death_streak": {
        "column": "death_streak",
        "display_name": "Death Streak",
        "format": "{:.0f}"
    }
}

# Discord message limits
DISCORD_MESSAGE_MAX_LENGTH = 2000

# Query result limits
LEADERBOARD_RESULT_LIMIT = 25
PLAYER_TOP_MATCHES_LIMIT = 25

# Time thresholds
MIN_PLAY_TIME_SECONDS = 2700  # 45 minutes
MIN_PLAY_TIME_MINUTES = 45
