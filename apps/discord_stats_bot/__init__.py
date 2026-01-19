"""
HLL Discord Stats Bot package for the StatsFinder project.

This package provides a Discord bot with slash commands for viewing
player and leaderboard statistics.
"""

def main():
    """Main entry point for the Discord bot."""
    from apps.discord_stats_bot.stats_bot import main as _main
    _main()

__all__ = ['main']

