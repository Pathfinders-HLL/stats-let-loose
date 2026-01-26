# Data Integrity Validation

This module provides comprehensive validation for the stats ingestion pipeline to detect and repair partial match insertions that can occur when the pipeline is interrupted (e.g., during a redeploy).

## Overview

A complete match insertion includes data in 6 related tables:
1. `match_history` - Match metadata
2. `player_match_stats` - Player performance per match
3. `player_kill_stats` - Weapon-specific kill counts
4. `player_death_stats` - Weapon-specific death counts
5. `player_victim` - Most-killed opponents per match
6. `player_nemesis` - Players who killed you most per match

## Validation Modes

### Standard Mode (Default)
Fast pattern-based detection suitable for regular checks:
- Detects matches with NO player stats
- Detects matches with zero/null player_count
- Detects ANY players missing kill stats
- Detects ANY players missing death stats
- Detects matches where ALL players are missing victim stats
- Detects matches where ALL players are missing nemesis stats

**Performance:** Fast - Uses aggregated queries
**Use case:** Regular validation runs, pre/post-ingestion checks

### Thorough Mode (`--thorough`)
Comprehensive record-by-record validation:
- Checks EVERY individual player for missing victim stats
- Checks EVERY individual player for missing nemesis stats
- All checks from standard mode

**Performance:** Slower - Uses batched queries (1000 records per batch)
**Use case:** Verification after repairs, comprehensive audits

## Usage

### Standalone CLI

```bash
# Quick check (standard mode)
python -m apps.api_stats_ingestion.validate_cli

# Comprehensive check (thorough mode)
python -m apps.api_stats_ingestion.validate_cli --thorough

# Detailed output
python -m apps.api_stats_ingestion.validate_cli --thorough --verbose

# Preview repairs
python -m apps.api_stats_ingestion.validate_cli --repair --dry-run

# Execute repairs (requires confirmation)
python -m apps.api_stats_ingestion.validate_cli --repair

# Quick fix for player_count only
python -m apps.api_stats_ingestion.validate_cli --fix-player-counts
```

### Integrated with Ingestion Pipeline

```bash
# Run with validation before and after
python -m apps.api_stats_ingestion.ingestion_cli --validate

# Auto-repair detected issues before ingesting
python -m apps.api_stats_ingestion.ingestion_cli --validate-before --auto-repair

# Thorough validation after ingestion
python -m apps.api_stats_ingestion.ingestion_cli --validate-after --thorough-validation
```

## Performance Considerations

### Memory Usage
- **Standard mode:** Minimal - Uses aggregated queries that return only match IDs
- **Thorough mode:** Controlled - Uses batched queries (1000 records per batch) with LIMIT/OFFSET
- The batch size (1000) was chosen to balance memory usage with query performance
- Memory usage scales with the number of affected records found, not total database size

### Query Performance
All queries use indexed columns for optimal performance:

**Standard Mode Queries:**
- Simple NOT EXISTS subqueries with indexed lookups
- Aggregations at the match level
- Typical execution time: <1 second for databases with millions of records

**Thorough Mode Queries:**
- Batched queries with LIMIT/OFFSET
- Execution time scales linearly with the number of missing records
- For databases with good data integrity (few missing records), thorough mode adds minimal overhead

### When to Use Each Mode

**Standard Mode:**
- Regular pre/post-ingestion validation
- Automated checks in CI/CD pipelines
- Quick sanity checks
- When you expect data to be mostly complete

**Thorough Mode:**
- After fixing issues to verify completeness
- Monthly/quarterly comprehensive audits
- When you suspect individual missing records (not just match-level patterns)
- After database migrations or major changes

## Repair Mechanism

The repair process:
1. Identifies affected matches
2. Deletes ALL data for those matches from ALL tables (in reverse dependency order)
3. Allows the ingestion pipeline to cleanly re-ingest those matches

**Tables cleaned (in order):**
1. `player_nemesis`
2. `player_victim`
3. `player_death_stats`
4. `player_kill_stats`
5. `player_match_stats`
6. `match_history`

**Safety features:**
- Dry-run mode by default (`--dry-run`)
- Requires explicit confirmation before deletion
- Reports exactly what will be deleted before proceeding

## Integration Example

```python
from apps.api_stats_ingestion.validate import check_all_integrity, delete_incomplete_match_data
from libs.db.database import get_db_connection

# Standard validation
conn = await get_db_connection()
report = await check_all_integrity(conn, verbose=True, thorough=False)

if report.has_issues:
    print(f"Found {len(report.affected_match_ids)} matches with issues")
    
    # Delete incomplete data
    await delete_incomplete_match_data(
        conn,
        list(report.affected_match_ids),
        dry_run=False
    )
```

## API Reference

### `check_all_integrity(conn, verbose=True, thorough=False, batch_size=1000)`
Run all data integrity checks.

**Args:**
- `conn`: Database connection
- `verbose`: Print progress information
- `thorough`: Check every individual player record
- `batch_size`: Records per batch for thorough checks (default: 1000)

**Returns:** `IntegrityReport` with all issues found

### `delete_incomplete_match_data(conn, match_ids, dry_run=True)`
Delete all data for specified matches.

**Args:**
- `conn`: Database connection
- `match_ids`: List of match IDs to delete
- `dry_run`: If True, only report what would be deleted

**Returns:** Dictionary with deletion counts per table

## Limitations

### Standard Mode
- Victim/nemesis checks only detect matches where ALL players are missing records
- Individual missing records may not be detected if some players have data

### Thorough Mode
- Slower for large datasets (but still reasonable due to batching)
- Not recommended for real-time/automated checks on very large databases

## Future Enhancements

Potential improvements:
1. Configurable batch sizes via environment variables
2. Parallel batch processing for even faster thorough checks
3. Progressive validation (check only recent matches)
4. Export validation reports to JSON/CSV for analysis
5. Integration with monitoring/alerting systems
