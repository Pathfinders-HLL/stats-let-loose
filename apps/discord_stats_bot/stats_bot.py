"""
Discord bot entry point with slash commands for player stats and leaderboards.
"""

import logging
import os
import signal
import time

import discord

from discord import app_commands
from discord.ext import commands

from apps.discord_stats_bot.commands import (
    setup_leaderboard_command,
    setup_profile_command,
    setup_player_command,
)
from apps.discord_stats_bot.common import (
    initialize_cache,
    initialize_format_cache,
    log_command_data,
    log_command_completion,
    close_db_pool,
    get_weapon_names,
)
from apps.discord_stats_bot.common.player_lookup import (
    load_pathfinder_player_ids_from_s3,
)
from apps.discord_stats_bot.bot_config import get_bot_config
from apps.discord_stats_bot.jobs.pathfinder import setup_pathfinder_leaderboards_task
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_ui import LeaderboardView
from apps.discord_stats_bot.jobs.channel_cleanup import setup_channel_cleanup_task
from apps.discord_stats_bot.health_check import READINESS_FILE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot_config = get_bot_config()
DISCORD_TOKEN = bot_config.token
ALLOWED_CHANNEL_IDS = bot_config.allowed_channel_ids

intents = discord.Intents.default()
intents.members = True  # Required for channel cleanup to check member roles


async def get_prefix(bot, message):
    """Return empty prefix list (only slash commands)."""
    return []


bot = commands.Bot(
    command_prefix=get_prefix,
    intents=intents,
    description="StatsLetLoose Discord Bot"
)

tree = bot.tree


def check_channel_permission(interaction: discord.Interaction) -> bool:
    """Returns True if the command is allowed in this channel."""
    if not ALLOWED_CHANNEL_IDS:
        return True
    return interaction.channel_id in ALLOWED_CHANNEL_IDS


# Register command groups
setup_player_command(tree, check_channel_permission)
setup_leaderboard_command(tree, check_channel_permission)
setup_profile_command(tree, check_channel_permission)


@bot.event
async def setup_hook():
    """
    Called before the bot connects to Discord.
    This is where we initialize required resources like loading player IDs from S3.
    """
    logger.info("Running setup hook: Loading pathfinder player IDs from S3...")
    try:
        await load_pathfinder_player_ids_from_s3()
        logger.info("Setup hook completed successfully")
    except Exception as e:
        logger.error(f"Failed to load pathfinder player IDs from S3 during setup: {e}", exc_info=True)
        raise


@bot.event
async def on_ready():
    """Called when the bot is ready and connected to Discord."""
    logger.info(f"{bot.user} has connected to Discord!")
    logger.info(f"Bot is in {len(bot.guilds)} guild(s)")
    
    await bot.wait_until_ready()
    
    try:
        # Initialize caches
        await initialize_cache()
        await initialize_format_cache()

        # Sync commands
        dev_guild_id = bot_config.dev_guild_id
        
        if dev_guild_id:
            logger.info(f"Force-syncing commands to development guild {dev_guild_id}...")

            try:
                dev_guild = discord.Object(id=dev_guild_id)

                # 1. Clear guild commands (prevents stale registrations)
                bot.tree.clear_commands(guild=dev_guild)

                # 2. Copy global commands into the guild
                bot.tree.copy_global_to(guild=dev_guild)

                # 3. Sync the guild explicitly
                synced_guild = await bot.tree.sync(guild=dev_guild)
                
                if synced_guild:
                    command_names = [cmd.name for cmd in synced_guild]
                    logger.info(f"Synced {len(synced_guild)} commands to guild: {', '.join(command_names)}")
                else:
                    logger.warning("No commands were synced to development guild")
            except Exception as e:
                logger.error(f"Failed to sync to development guild: {e}", exc_info=True)
        else:
            logger.info("Syncing commands globally...")
            try:
                synced_global = await tree.sync()
                
                if synced_global:
                    command_names = [cmd.name for cmd in synced_global]
                    logger.info(f"Synced {len(synced_global)} commands globally: {', '.join(command_names)}")
            except discord.HTTPException as e:
                logger.warning(f"HTTP error while syncing globally: {e}", exc_info=True)
            except Exception as e:
                logger.warning(f"Failed to sync globally: {e}", exc_info=True)
        
        await bot.change_presence(activity=discord.Game(name="Use /help for commands"))
        
        # Register persistent views
        bot.add_view(LeaderboardView())
        
        # Start scheduled tasks
        setup_pathfinder_leaderboards_task(bot)
        setup_channel_cleanup_task(bot)

        # Signal ready for healthcheck - only after everything succeeds
        try:
            open(READINESS_FILE, "a").close()
            logger.info("Bot is fully ready - healthcheck file created")
        except OSError as e:
            logger.warning(f"Could not create readiness file: {e}")
    
    except Exception as e:
        logger.error(f"Error during bot initialization: {e}", exc_info=True)
        # Remove readiness file if initialization fails
        try:
            os.remove(READINESS_FILE)
        except OSError:
            pass
        raise


@bot.event
async def on_disconnect():
    """Clean up database connections on disconnect."""
    logger.info("Bot disconnected, closing database pool...")
    _remove_readiness_file()
    await close_db_pool()


#@tree.command(name="ping", description="Check if the bot is responding")
#async def ping(interaction: discord.Interaction):
#    """Check if the bot is responding."""
#    start_time = time.time()
#    log_command_data(interaction, "ping")
#    
#    try:
#        if not check_channel_permission(interaction):
#            await interaction.response.send_message(
#                "❌ This bot can only be used in the designated channel.",
#                ephemeral=True
#            )
#            log_command_completion("ping", start_time, success=False, interaction=interaction, kwargs={})
#            return
#        
#        latency = round(bot.latency * 1000)
#        await interaction.response.send_message(f"Pong! Latency: {latency}ms", ephemeral=True)
#        log_command_completion("ping", start_time, success=True, interaction=interaction, kwargs={})
#    except Exception as e:
#        logger.error(f"Error in ping command: {e}", exc_info=True)
#        log_command_completion("ping", start_time, success=False, interaction=interaction, kwargs={})
#        raise


@tree.command(name="help", description="Show available commands and weapon categories.")
async def help_command(interaction: discord.Interaction):
    """Show available commands."""
    start_time = time.time()
    log_command_data(interaction, "help")
    
    try:
        if not check_channel_permission(interaction):
            await interaction.response.send_message(
                "❌ This bot can only be used in the designated channel.",
                ephemeral=True
            )
            log_command_completion("help", start_time, success=False, interaction=interaction, kwargs={})
            return
        
        await interaction.response.defer(ephemeral=True)
        
        weapon_names = get_weapon_names()
        
        if not weapon_names:
            await interaction.followup.send(
                "❌ Unable to load weapon categories. Please contact the bot administrator.",
                ephemeral=True
            )
            log_command_completion("help", start_time, success=False, interaction=interaction, kwargs={})
            return
        
        commands_text = """
# StatsFinder Bot Commands

## General
**`/help`** - Show commands

## Profile
**`/profile setid`** - Set default player ID
- `player` (required)
**`/profile clearid`** - Clear stored player ID
**`/profile format`** - Set leaderboard format
- `format_type` (required): Cards/Table/List

## Player Commands
**`/player kills`** - Top 25 matches by kills  
**`/player deaths`** - Top 25 matches by deaths  
**`/player weapon`** - Total kills by weapon  
**`/player performance`** - Top matches by stat
- `stat_type` (required): KPM/KDR/DPM/Streaks/Most Kills
**`/player contributions`** - Top 25 matches by score
- `score_type` (required): Support/Attack/Defense/Combat
**`/player maps`** - Best match stats for map
- `map_name` (required)
**`/player nemesis`** - Top 25 players who killed you  
**`/player victim`** - Top 25 players you killed

## Leaderboard Commands
**`/leaderboard kills`** - Top by kills (avg/sum)  
**`/leaderboard deaths`** - Top by deaths (avg/sum)  
**`/leaderboard 100killgames`** - Most 100+ kill games  
**`/leaderboard weapon`** - Top by weapon kills
- `weapon_category` (required)
**`/leaderboard alltime`** - All-time weapon kills
- `weapon_category` (required)
**`/leaderboard performance`** - Top by stat
- `stat_type` (required): KDR/KPM/DPM/Streaks
**`/leaderboard contributions`** - Top by scores
- `score_type` (required): Support/Attack/Defense/Combat/Seeding

## Optional Parameters
- `player`: ID/name (skip if set via `/profile setid`)
- `over_last_days`: Days back (default: 30, 0=all-time)
- `only_pathfinders`: Filter to Pathfinders (default: false)
- `kill_type`/`death_type`: All/Infantry/Armor/Artillery
- `aggregate_by`: Average/Sum (default: Sum)
- `order_by`: Kills/KDR/KPM (default: Kills)
        """
        
        logger.info(f"Help command content length: {len(commands_text)} chars")
        
        await interaction.followup.send(commands_text, ephemeral=True)
        log_command_completion("help", start_time, success=True, interaction=interaction, kwargs={})
        
    except Exception as e:
        logger.error(f"Error in help command: {e}", exc_info=True)
        log_command_completion("help", start_time, success=False, interaction=interaction, kwargs={})
        if not interaction.response.is_done():
            await interaction.response.send_message(
                f"❌ An error occurred while loading help information: {str(e)}",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"❌ An error occurred while loading help information: {str(e)}",
                ephemeral=True
            )


def _remove_readiness_file() -> None:
    """Remove readiness file so healthcheck fails after shutdown."""
    try:
        os.remove(READINESS_FILE)
    except OSError:
        pass


def main():
    """Main entry point for the Discord bot."""
    def handle_shutdown_signal(signum: int, frame) -> None:
        logger.info("Received signal %s, removing readiness file and exiting.", signum)
        _remove_readiness_file()
        raise SystemExit(0)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, handle_shutdown_signal)
        except (ValueError, OSError):
            # SIGINT not available in all contexts (e.g. threads), skip
            pass

    try:
        logger.info("Starting Discord bot...")
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        _remove_readiness_file()
        raise


if __name__ == "__main__":
    main()
