"""
Discord bot entry point with slash commands for player stats and leaderboards.
"""

import logging
import time

import discord
from discord import app_commands
from discord.ext import commands

from apps.discord_stats_bot.commands.leaderboard import setup_leaderboard_command
from apps.discord_stats_bot.commands.management import setup_profile_command
from apps.discord_stats_bot.commands.player import setup_player_command
from apps.discord_stats_bot.common.shared import (
    log_command_data,
    log_command_completion,
    close_db_pool
)
from apps.discord_stats_bot.config import get_bot_config
from apps.discord_stats_bot.jobs.karabiner_stats import setup_karabiner_stats_task

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot_config = get_bot_config()
DISCORD_TOKEN = bot_config.token
ALLOWED_CHANNEL_IDS = bot_config.allowed_channel_ids

intents = discord.Intents.default()

async def get_prefix(bot, message):
    return []

bot = commands.Bot(
    command_prefix=get_prefix,
    intents=intents,
    description="StatsLetLoose Discord Bot"
)

# Use the bot's built-in command tree (automatically created)
tree = bot.tree


def check_channel_permission(interaction: discord.Interaction) -> bool:
    """Returns True if the command is allowed in this channel."""
    if not ALLOWED_CHANNEL_IDS:
        return True
    return interaction.channel_id in ALLOWED_CHANNEL_IDS


setup_player_command(tree, check_channel_permission)
setup_leaderboard_command(tree, check_channel_permission)
setup_profile_command(tree, check_channel_permission)


@bot.event
async def on_ready():
    """Called when the bot is ready and connected to Discord."""
    logger.info(f"{bot.user} has connected to Discord!")
    logger.info(f"Bot is in {len(bot.guilds)} guild(s)")
    
    # Wait a moment to ensure all guilds are loaded
    await bot.wait_until_ready()

    # Sync commands
    dev_guild_id = bot_config.dev_guild_id
    
    if dev_guild_id:
        # Sync to development guild (guild commands only, faster than global sync)
        logger.info(f"Syncing commands to development guild {dev_guild_id}...")
        try:
            dev_guild = discord.Object(id=dev_guild_id)
            
            # Copy global commands to the guild first, then sync
            tree.copy_global_to(guild=dev_guild)
            synced_guild = await tree.sync(guild=dev_guild)
            
            # Log command names for verification
            if synced_guild:
                command_names = [cmd.name for cmd in synced_guild]
                logger.info(f"Synced {len(synced_guild)} commands to guild: {', '.join(command_names)}")
            else:
                logger.warning("No commands were synced to development guild - they may already be synced")
        except Exception as e:
            logger.error(f"Failed to sync to development guild: {e}", exc_info=True)
    else:
        # Sync globally (slower but works for all guilds)
        logger.info("Syncing commands globally...")
        try:
            synced_global = await tree.sync()
            
            # Log command names for verification
            if synced_global:
                command_names = [cmd.name for cmd in synced_global]
                logger.info(f"Synced {len(synced_global)} commands globally: {', '.join(command_names)}")
        except discord.HTTPException as e:
            logger.warning(f"HTTP error while syncing globally: {e}", exc_info=True)
            logger.warning("Commands may still work if they were synced previously")
        except Exception as e:
            logger.warning(f"Failed to sync globally: {e}", exc_info=True)
    
    # Set bot activity
    await bot.change_presence(
        activity=discord.Game(name="Use /help for commands")
    )
    
    # Start scheduled tasks
    setup_karabiner_stats_task(bot)


@bot.event
async def on_disconnect():
    """Clean up database connections on disconnect."""
    logger.info("Bot disconnected, closing database pool...")
    await close_db_pool()


@tree.command(name="ping", description="Check if the bot is responding")
async def ping(interaction: discord.Interaction):
    start_time = time.time()
    log_command_data(interaction, "ping")
    
    try:
        if not check_channel_permission(interaction):
            await interaction.response.send_message(
                f"❌ This bot can only be used in the designated channel.",
                ephemeral=True
            )
            log_command_completion("ping", start_time, success=False, interaction=interaction, kwargs={})
            return
        
        latency = round(bot.latency * 1000)
        await interaction.response.send_message(f"Pong! Latency: {latency}ms")
        log_command_completion("ping", start_time, success=True, interaction=interaction, kwargs={})
    except Exception as e:
        logger.error(f"Error in ping command: {e}", exc_info=True)
        log_command_completion("ping", start_time, success=False, interaction=interaction, kwargs={})
        raise


@tree.command(name="help", description="Show available commands and weapon categories.")
async def help_command(interaction: discord.Interaction):
    start_time = time.time()
    log_command_data(interaction, "help")
    
    try:
        if not check_channel_permission(interaction):
            await interaction.response.send_message(
                f"❌ This bot can only be used in the designated channel.",
                ephemeral=True
            )
            log_command_completion("help", start_time, success=False, interaction=interaction, kwargs={})
            return
        
        await interaction.response.defer(ephemeral=True)
        
        # Import the cached weapon names function
        from apps.discord_stats_bot.common.weapon_autocomplete import get_weapon_names
        
        # Get cached weapon names
        weapon_names = get_weapon_names()
        
        if not weapon_names:
            await interaction.followup.send(
                "❌ Unable to load weapon categories. Please contact the bot administrator.",
                ephemeral=True
            )
            log_command_completion("help", start_time, success=False, interaction=interaction, kwargs={})
            return
        
        # Create help message
        commands_text = "## StatsFinder Bot Commands\n\n"
        
        commands_text += "### Profile Commands:\n"
        commands_text += "**`/profile setid`** - Set default player ID\n"
        commands_text += "  • `player` (required): Your player ID or name\n\n"
        
        commands_text += "**`/profile clearid`** - Clear stored player ID\n\n"
        
        commands_text += "### Player Commands:\n"

        commands_text += "**`/player kills`** - Top matches by total kills\n"
        
        commands_text += "**`/player deaths`** - Top matches by total deaths\n"
        
        commands_text += "**`/player weapon`** - Get total kills for a player by weapon category\n"
        commands_text += "  • `weapon_category` (required))\n\n"
        
        commands_text += "**`/player performance`** - Get top matches for a player by stat (KPM, KDR, Kill Streak, etc)\n"
        commands_text += "  • `stat_type` (required): KPM, KDR, DPM, Kill Streak, Death Streak, Most Kills)\n\n"
        
        commands_text += "**`/player contributions`** - Top matches by score type\n"
        commands_text += "  • `score_type` (required): Support Score, Attack Score, Defense Score, Combat Score)\n\n"
        
        commands_text += "**`/player maps`** - Best stats per map ordered by best to worst\n"
        
        commands_text += "### Leaderboard Commands:\n"
        
        commands_text += "**`/leaderboard kills`** - Top by sum of kills from top matches\n"
        
        commands_text += "**`/leaderboard deaths`** - Top by sum of deaths from top matches\n\n"
        
        commands_text += "**`/leaderboard 100killgames`** - Top by most 100+ kill games\n"
        
        commands_text += "**`/leaderboard weapon`** - Top by weapon kills over time period\n"
        commands_text += "  • `weapon_category` (required)\n\n"
        
        commands_text += "**`/leaderboard alltime`** - Top all-time weapon kills\n"
        commands_text += "  • `weapon_category` (required)\n\n"
        
        commands_text += "**`/leaderboard performance`** - Top by average stat (KPM< KDR, etc)\n"
        commands_text += "  • `stat_type` (required): KDR, KPM, DPM, Kill/Death Streak\n\n"
        
        commands_text += "**`/leaderboard contributions`** - Top by sum of scores (Attack, Support, etc)\n"
        commands_text += "  • `score_type` (required): Support Score, Attack Score, Defense Score, Combat Score\n\n"
        
        # Log the content length for debugging
        logger.info(f"Help command content length: {len(commands_text)} chars (help text)")
        
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


def main():
    try:
        logger.info("Starting Discord bot...")
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

