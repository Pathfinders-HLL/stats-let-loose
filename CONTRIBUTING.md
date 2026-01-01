# Contributing to StatsLetLoose

Thanks for your interest in contributing! This document outlines the process for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment (see README.md)
4. Create a branch for your changes

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/StatsLetLoose.git
cd StatsLetLoose

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install all dependencies
pip install -r apps/discord_stats_bot/requirements.txt
pip install -r apps/api_stats_ingestion/requirements.txt
```

## Code Style

- Use type hints where practical
- Keep functions focused and reasonably sized
- Write docstrings for public functions and classes

## Making Changes

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes with clear, atomic commits

3. Test your changes locally

4. Push to your fork and open a Pull Request

## Pull Request Guidelines

- Provide a clear description of what your PR does
- Reference any related issues
- Keep PRs focused - one feature or fix per PR
- Ensure the code runs without errors

## Adding New Commands

When adding new Discord bot commands:

1. Create the subcommand in the appropriate directory:
   - Player commands: `apps/discord_stats_bot/subcommands/player/`
   - Leaderboard commands: `apps/discord_stats_bot/subcommands/leaderboard/`

2. Register the command in the parent group file:
   - `apps/discord_stats_bot/commands/player.py`
   - `apps/discord_stats_bot/commands/leaderboard.py`

3. Use the shared utilities from `apps/discord_stats_bot/common/shared.py`

4. Update the `/help` command in `apps/discord_stats_bot/stats_boy.py` _(the characer limit must be <= 2000 characters)_

## Database Changes

If your changes require database schema modifications:

1. Create a new numbered SQL file in `sql/`
2. Document the changes in your PR
3. Consider backwards compatibility

## Questions?

Open an issue for any questions about contributing.

