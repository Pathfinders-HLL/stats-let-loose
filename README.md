# StatsLetLoose

A statistics tracking and analysis platform for Hell Let Loose servers. Ingests match data from CRCON (Community RCON) and exposes it via a Discord bot with comprehensive player and leaderboard commands.

## Features

### Discord Bot Commands

**Player Statistics** (`/player`)
- `/player weapon` - Total kills by weapon category  
- `/player performance` - Top matches by KPM, KDR, kill streaks, etc.
- `/player kills` - Top matches by kill count (infantry, armor, artillery)
- `/player deaths` - Top matches by death count
- `/player contributions` - Top matches by score type (support, attack, defense, combat)
- `/player maps` - Best performance per map

**Leaderboards** (`/leaderboard`)
- `/leaderboard weapon` - Top players by weapon kills
- `/leaderboard alltime` - All-time weapon kill leaders
- `/leaderboard performance` - Top average KDR, KPM, DPM
- `/leaderboard kills` - Top players by cumulative kills
- `/leaderboard deaths` - Players with most deaths
- `/leaderboard 100killgames` - Most 100+ kill games
- `/leaderboard contributions` - Top scorers by category

**Profile** (`/profile`)
- `/profile setid` - Save your player ID for easier command usage
- `/profile clearid` - Clear saved player ID

### Data Pipeline

Automated ingestion from CRCON API with configurable polling intervals.

## Architecture

```
StatsLetLoose/
├── apps/
│   ├── discord_stats_bot/    # Discord bot with slash commands
│   └── api_stats_ingestion/  # ETL pipeline for match data
├── libs/
│   ├── db/                   # Shared database utilities
│   └── hll_data/             # Game data mappings (weapons, maps)
├── infra/
│   └── docker/               # Docker Compose deployment
└── sql/                      # Database schema definitions
```

## Requirements

- Python 3.14
- PostgreSQL 17
- Docker Compose (for deployment)
- RCON instance with API access

## Quick Start

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/yourusername/StatsLetLoose.git
cd StatsLetLoose
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r apps/discord_stats_bot/requirements.txt
pip install -r apps/api_stats_ingestion/requirements.txt
```

4. Copy the example environment file and configure:
```bash
cp .env.example .env
# Edit .env with your settings
```

5. Set up the database:
```bash
# Run the SQL files in order
psql -U postgres -d stats_let_loose -f sql/1-create_match_history_schema.sql
psql -U postgres -d stats_let_loose -f sql/2-create_player_match_stats_schema.sql
psql -U postgres -d stats_let_loose -f sql/3-create_player_kill_stats_schema.sql
psql -U postgres -d stats_let_loose -f sql/4-create_player_death_stats_schema.sql
psql -U postgres -d stats_let_loose -f sql/999-create_performance_indexes.sql
```

6. Run the Discord bot:
```bash
python -m apps.discord_stats_bot.stats_bot
```

### Docker Deployment

1. Copy and configure environment:
```bash
cp .env.example infra/docker/.env
# Edit infra/docker/.env with your settings
```

2. Start all services:
```bash
cd infra/docker
docker-compose up -d
```

This starts:
- PostgreSQL database with auto-initialization
- API ingestion service (runs every 30 minutes)
- Discord bot

## Configuration

See `.env.example` for all available configuration options.

### Required Variables

| Variable | Description |
|----------|-------------|
| `DISCORD_BOT_TOKEN` | Discord bot token from developer portal |
| `INGESTION_BASE_URL` | Base URL of your CRCON instance |
| `POSTGRES_PASSWORD` | PostgreSQL admin password |
| `POSTGRES_INGESTION_PASSWORD` | Password for ingestion service user |
| `POSTGRES_RO_PASSWORD` | Password for read-only user (bot) |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_ALLOWED_CHANNEL_IDS` | (none) | Comma-separated channel IDs to restrict bot |
| `DISCORD_DEV_GUILD_ID` | (none) | Guild ID for faster command sync during dev |
| `DISCORD_STATS_CHANNEL_ID` | (none) | Channel for scheduled stats posts |

## Data Ingestion

The ingestion pipeline fetches match data from CRCON's API:

```bash
# Run full pipeline
python -m apps.api_stats_ingestion.ingestion_cli

# Skip fetching (use existing data)
python -m apps.api_stats_ingestion.ingestion_cli --skip-fetch

# Skip already-fetched matches
python -m apps.api_stats_ingestion.ingestion_cli --skip-existing-fetch
```

## Database Schema

The system uses PostgreSQL with the `pathfinder_stats` schema:

- `match_history` - Match metadata (map, duration, scores)
- `player_match_stats` - Per-player match statistics
- `player_kill_stats` - Detailed kill breakdowns by weapon
- `player_death_stats` - Detailed death breakdowns by weapon

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

