"""
Data integrity check functions to detect partial match insertions.

These checks identify matches where not all related tables have been populated,
typically caused by pipeline interruptions during ingestion (e.g., redeploys).

A complete match insertion includes:
1. match_history record
2. player_match_stats records for all players
3. player_kill_stats records for all players
4. player_death_stats records for all players
5. player_victim records for all players
6. player_nemesis records for all players
7. player_count updated in match_history
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Set

import asyncpg


class IssueType(Enum):
    """Types of data integrity issues that can be detected."""
    
    MATCH_MISSING_PLAYER_STATS = "match_missing_player_stats"
    MATCH_ZERO_PLAYER_COUNT = "match_zero_player_count"
    PLAYER_MISSING_KILL_STATS = "player_missing_kill_stats"
    PLAYER_MISSING_DEATH_STATS = "player_missing_death_stats"
    PLAYER_MISSING_VICTIM_STATS = "player_missing_victim_stats"
    PLAYER_MISSING_NEMESIS_STATS = "player_missing_nemesis_stats"


@dataclass
class IntegrityIssue:
    """Represents a single data integrity issue."""
    
    issue_type: IssueType
    match_id: int
    description: str
    player_ids: Optional[List[str]] = None  # Affected player IDs, if applicable


@dataclass
class IntegrityReport:
    """Complete report of all data integrity issues found."""
    
    issues: List[IntegrityIssue] = field(default_factory=list)
    
    # Summary counts
    matches_missing_player_stats: int = 0
    matches_with_zero_player_count: int = 0
    players_missing_kill_stats: int = 0
    players_missing_death_stats: int = 0
    players_missing_victim_stats: int = 0
    players_missing_nemesis_stats: int = 0
    
    @property
    def total_issues(self) -> int:
        """Total number of issues found."""
        return len(self.issues)
    
    @property
    def affected_match_ids(self) -> Set[int]:
        """Set of all match IDs with integrity issues."""
        return {issue.match_id for issue in self.issues}
    
    @property
    def has_issues(self) -> bool:
        """Whether any issues were found."""
        return len(self.issues) > 0
    
    def print_summary(self) -> None:
        """Print a summary of the integrity report."""
        print("\n" + "=" * 60)
        print("DATA INTEGRITY CHECK RESULTS")
        print("=" * 60)
        
        if not self.has_issues:
            print("\n✓ No data integrity issues found!")
            print("  All matches have complete data in all tables.")
            return
        
        print(f"\n⚠ Found {self.total_issues} issues across {len(self.affected_match_ids)} matches:\n")
        
        if self.matches_missing_player_stats > 0:
            print(f"  • Matches missing player stats: {self.matches_missing_player_stats}")
        
        if self.matches_with_zero_player_count > 0:
            print(f"  • Matches with zero/null player count: {self.matches_with_zero_player_count}")
        
        if self.players_missing_kill_stats > 0:
            print(f"  • Players missing kill stats: {self.players_missing_kill_stats}")
        
        if self.players_missing_death_stats > 0:
            print(f"  • Players missing death stats: {self.players_missing_death_stats}")
        
        if self.players_missing_victim_stats > 0:
            print(f"  • Players missing victim stats: {self.players_missing_victim_stats}")
        
        if self.players_missing_nemesis_stats > 0:
            print(f"  • Players missing nemesis stats: {self.players_missing_nemesis_stats}")
        
        print(f"\nAffected match IDs: {sorted(self.affected_match_ids)}")


async def check_matches_missing_player_stats(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find matches in match_history that have no corresponding player_match_stats records.
    
    This indicates the pipeline was interrupted after inserting match_history
    but before inserting player statistics.
    """
    rows = await conn.fetch("""
        SELECT mh.match_id
        FROM pathfinder_stats.match_history mh
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_match_stats pms
            WHERE pms.match_id = mh.match_id
        )
        ORDER BY mh.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.MATCH_MISSING_PLAYER_STATS,
            match_id=row["match_id"],
            description=f"Match {row['match_id']} exists in match_history but has no player_match_stats records",
        )
        for row in rows
    ]


async def check_matches_with_zero_player_count(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find matches where player_count is NULL or 0, but player_match_stats exist.
    
    This indicates the pipeline was interrupted before the player_count update step,
    or the match was inserted without the final count update.
    """
    rows = await conn.fetch("""
        SELECT mh.match_id, COUNT(pms.player_id) as actual_count
        FROM pathfinder_stats.match_history mh
        LEFT JOIN pathfinder_stats.player_match_stats pms ON mh.match_id = pms.match_id
        WHERE (mh.player_count IS NULL OR mh.player_count = 0)
          AND EXISTS (
              SELECT 1 FROM pathfinder_stats.player_match_stats pms2
              WHERE pms2.match_id = mh.match_id
          )
        GROUP BY mh.match_id
        ORDER BY mh.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.MATCH_ZERO_PLAYER_COUNT,
            match_id=row["match_id"],
            description=f"Match {row['match_id']} has player_count=0/NULL but {row['actual_count']} players in player_match_stats",
        )
        for row in rows
    ]


async def check_player_stats_missing_kill_stats(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find player/match combinations that have player_match_stats but no player_kill_stats.
    
    Returns issues grouped by match_id for easier remediation.
    """
    rows = await conn.fetch("""
        SELECT pms.match_id, array_agg(pms.player_id ORDER BY pms.player_id) as player_ids
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_kill_stats pks
            WHERE pks.player_id = pms.player_id AND pks.match_id = pms.match_id
        )
        GROUP BY pms.match_id
        ORDER BY pms.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_KILL_STATS,
            match_id=row["match_id"],
            description=f"Match {row['match_id']}: {len(row['player_ids'])} players missing kill stats",
            player_ids=list(row["player_ids"]),
        )
        for row in rows
    ]


async def check_player_stats_missing_death_stats(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find player/match combinations that have player_match_stats but no player_death_stats.
    
    Returns issues grouped by match_id for easier remediation.
    """
    rows = await conn.fetch("""
        SELECT pms.match_id, array_agg(pms.player_id ORDER BY pms.player_id) as player_ids
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_death_stats pds
            WHERE pds.player_id = pms.player_id AND pds.match_id = pms.match_id
        )
        GROUP BY pms.match_id
        ORDER BY pms.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_DEATH_STATS,
            match_id=row["match_id"],
            description=f"Match {row['match_id']}: {len(row['player_ids'])} players missing death stats",
            player_ids=list(row["player_ids"]),
        )
        for row in rows
    ]


async def check_player_stats_missing_victim_stats(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find player/match combinations that have player_match_stats but no player_victim records.
    
    Note: Some players may legitimately have no victims (0 kills), so this check
    looks for matches where ALL players are missing victim records, which is more
    indicative of a partial insertion.
    """
    rows = await conn.fetch("""
        SELECT pms.match_id, array_agg(pms.player_id ORDER BY pms.player_id) as player_ids
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_victim pv
            WHERE pv.player_id = pms.player_id AND pv.match_id = pms.match_id
        )
        GROUP BY pms.match_id
        HAVING COUNT(*) = (
            SELECT COUNT(*) FROM pathfinder_stats.player_match_stats pms2
            WHERE pms2.match_id = pms.match_id
        )
        ORDER BY pms.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_VICTIM_STATS,
            match_id=row["match_id"],
            description=f"Match {row['match_id']}: all {len(row['player_ids'])} players missing victim stats (likely partial insertion)",
            player_ids=list(row["player_ids"]),
        )
        for row in rows
    ]


async def check_player_stats_missing_nemesis_stats(conn: asyncpg.Connection) -> List[IntegrityIssue]:
    """
    Find player/match combinations that have player_match_stats but no player_nemesis records.
    
    Note: Some players may legitimately have no nemesis (0 deaths), so this check
    looks for matches where ALL players are missing nemesis records, which is more
    indicative of a partial insertion.
    """
    rows = await conn.fetch("""
        SELECT pms.match_id, array_agg(pms.player_id ORDER BY pms.player_id) as player_ids
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_nemesis pn
            WHERE pn.player_id = pms.player_id AND pn.match_id = pms.match_id
        )
        GROUP BY pms.match_id
        HAVING COUNT(*) = (
            SELECT COUNT(*) FROM pathfinder_stats.player_match_stats pms2
            WHERE pms2.match_id = pms.match_id
        )
        ORDER BY pms.match_id
    """)
    
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_NEMESIS_STATS,
            match_id=row["match_id"],
            description=f"Match {row['match_id']}: all {len(row['player_ids'])} players missing nemesis stats (likely partial insertion)",
            player_ids=list(row["player_ids"]),
        )
        for row in rows
    ]


async def check_all_player_victim_stats(conn: asyncpg.Connection, batch_size: int = 1000) -> List[IntegrityIssue]:
    """
    Thorough check: Find ALL individual players missing victim stats.
    
    This check is more comprehensive than check_player_stats_missing_victim_stats,
    which only detects matches where ALL players are missing victim stats.
    
    Uses batched queries to avoid memory issues with large datasets.
    
    Args:
        conn: Database connection
        batch_size: Number of records to process per batch (default: 1000)
    
    Returns:
        List of IntegrityIssue objects, one per match with missing victim stats
    """
    # First, get total count
    total_missing = await conn.fetchval("""
        SELECT COUNT(*)
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_victim pv
            WHERE pv.player_id = pms.player_id AND pv.match_id = pms.match_id
        )
    """)
    
    if total_missing == 0:
        return []
    
    # Process in batches and group by match_id
    issues_by_match = {}
    offset = 0
    
    while offset < total_missing:
        rows = await conn.fetch("""
            SELECT pms.player_id, pms.match_id
            FROM pathfinder_stats.player_match_stats pms
            WHERE NOT EXISTS (
                SELECT 1 FROM pathfinder_stats.player_victim pv
                WHERE pv.player_id = pms.player_id AND pv.match_id = pms.match_id
            )
            ORDER BY pms.match_id, pms.player_id
            LIMIT $1 OFFSET $2
        """, batch_size, offset)
        
        for row in rows:
            match_id = row["match_id"]
            player_id = row["player_id"]
            
            if match_id not in issues_by_match:
                issues_by_match[match_id] = []
            issues_by_match[match_id].append(player_id)
        
        offset += batch_size
    
    # Convert to IntegrityIssue objects
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_VICTIM_STATS,
            match_id=match_id,
            description=f"Match {match_id}: {len(player_ids)} players missing victim stats",
            player_ids=player_ids,
        )
        for match_id, player_ids in sorted(issues_by_match.items())
    ]


async def check_all_player_nemesis_stats(conn: asyncpg.Connection, batch_size: int = 1000) -> List[IntegrityIssue]:
    """
    Thorough check: Find ALL individual players missing nemesis stats.
    
    This check is more comprehensive than check_player_stats_missing_nemesis_stats,
    which only detects matches where ALL players are missing nemesis stats.
    
    Uses batched queries to avoid memory issues with large datasets.
    
    Args:
        conn: Database connection
        batch_size: Number of records to process per batch (default: 1000)
    
    Returns:
        List of IntegrityIssue objects, one per match with missing nemesis stats
    """
    # First, get total count
    total_missing = await conn.fetchval("""
        SELECT COUNT(*)
        FROM pathfinder_stats.player_match_stats pms
        WHERE NOT EXISTS (
            SELECT 1 FROM pathfinder_stats.player_nemesis pn
            WHERE pn.player_id = pms.player_id AND pn.match_id = pms.match_id
        )
    """)
    
    if total_missing == 0:
        return []
    
    # Process in batches and group by match_id
    issues_by_match = {}
    offset = 0
    
    while offset < total_missing:
        rows = await conn.fetch("""
            SELECT pms.player_id, pms.match_id
            FROM pathfinder_stats.player_match_stats pms
            WHERE NOT EXISTS (
                SELECT 1 FROM pathfinder_stats.player_nemesis pn
                WHERE pn.player_id = pms.player_id AND pn.match_id = pms.match_id
            )
            ORDER BY pms.match_id, pms.player_id
            LIMIT $1 OFFSET $2
        """, batch_size, offset)
        
        for row in rows:
            match_id = row["match_id"]
            player_id = row["player_id"]
            
            if match_id not in issues_by_match:
                issues_by_match[match_id] = []
            issues_by_match[match_id].append(player_id)
        
        offset += batch_size
    
    # Convert to IntegrityIssue objects
    return [
        IntegrityIssue(
            issue_type=IssueType.PLAYER_MISSING_NEMESIS_STATS,
            match_id=match_id,
            description=f"Match {match_id}: {len(player_ids)} players missing nemesis stats",
            player_ids=player_ids,
        )
        for match_id, player_ids in sorted(issues_by_match.items())
    ]


async def check_all_integrity(
    conn: asyncpg.Connection,
    verbose: bool = True,
    thorough: bool = False,
    batch_size: int = 1000,
) -> IntegrityReport:
    """
    Run all data integrity checks and return a comprehensive report.
    
    Args:
        conn: Database connection
        verbose: If True, print progress information
        thorough: If True, check every individual player record (slower but more comprehensive)
        batch_size: Number of records to process per batch for thorough checks (default: 1000)
    
    Returns:
        IntegrityReport with all issues found
    """
    report = IntegrityReport()
    
    if verbose:
        mode = "THOROUGH" if thorough else "STANDARD"
        print(f"\nRunning data integrity checks ({mode} mode)...")
    
    # Check 1: Matches missing player stats
    if verbose:
        print("  Checking for matches missing player stats...")
    issues = await check_matches_missing_player_stats(conn)
    report.issues.extend(issues)
    report.matches_missing_player_stats = len(issues)
    
    # Check 2: Matches with zero/null player count
    if verbose:
        print("  Checking for matches with zero/null player count...")
    issues = await check_matches_with_zero_player_count(conn)
    report.issues.extend(issues)
    report.matches_with_zero_player_count = len(issues)
    
    # Check 3: Players missing kill stats
    if verbose:
        print("  Checking for players missing kill stats...")
    issues = await check_player_stats_missing_kill_stats(conn)
    report.issues.extend(issues)
    report.players_missing_kill_stats = sum(len(i.player_ids or []) for i in issues)
    
    # Check 4: Players missing death stats
    if verbose:
        print("  Checking for players missing death stats...")
    issues = await check_player_stats_missing_death_stats(conn)
    report.issues.extend(issues)
    report.players_missing_death_stats = sum(len(i.player_ids or []) for i in issues)
    
    # Check 5 & 6: Victim and nemesis stats
    if thorough:
        # Thorough mode: check every individual player record
        if verbose:
            print("  Checking for ALL players missing victim stats (thorough mode)...")
        issues = await check_all_player_victim_stats(conn, batch_size=batch_size)
        report.issues.extend(issues)
        report.players_missing_victim_stats = sum(len(i.player_ids or []) for i in issues)
        
        if verbose:
            print("  Checking for ALL players missing nemesis stats (thorough mode)...")
        issues = await check_all_player_nemesis_stats(conn, batch_size=batch_size)
        report.issues.extend(issues)
        report.players_missing_nemesis_stats = sum(len(i.player_ids or []) for i in issues)
    else:
        # Standard mode: only check matches where ALL players are missing
        if verbose:
            print("  Checking for matches missing victim stats...")
        issues = await check_player_stats_missing_victim_stats(conn)
        report.issues.extend(issues)
        report.players_missing_victim_stats = sum(len(i.player_ids or []) for i in issues)
        
        if verbose:
            print("  Checking for matches missing nemesis stats...")
        issues = await check_player_stats_missing_nemesis_stats(conn)
        report.issues.extend(issues)
        report.players_missing_nemesis_stats = sum(len(i.player_ids or []) for i in issues)
    
    return report


async def delete_incomplete_match_data(
    conn: asyncpg.Connection,
    match_ids: List[int],
    dry_run: bool = True,
) -> dict:
    """
    Delete all data for specified matches from all tables.
    
    This allows the matches to be re-ingested cleanly from the JSON files.
    
    Args:
        conn: Database connection
        match_ids: List of match IDs to delete
        dry_run: If True, only report what would be deleted without actually deleting
    
    Returns:
        Dictionary with deletion counts per table
    """
    if not match_ids:
        return {"total_deleted": 0}
    
    results = {}
    
    # Tables to clean, in reverse dependency order
    tables = [
        "pathfinder_stats.player_nemesis",
        "pathfinder_stats.player_victim",
        "pathfinder_stats.player_death_stats",
        "pathfinder_stats.player_kill_stats",
        "pathfinder_stats.player_match_stats",
        "pathfinder_stats.match_history",
    ]
    
    prefix = "[DRY RUN] Would delete" if dry_run else "Deleted"
    
    for table in tables:
        if dry_run:
            # Count rows that would be deleted
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE match_id = ANY($1)",
                match_ids
            )
            results[table] = count
        else:
            # Actually delete
            result = await conn.execute(
                f"DELETE FROM {table} WHERE match_id = ANY($1)",
                match_ids
            )
            # Parse "DELETE N" result
            count = 0
            if result:
                parts = result.split()
                if len(parts) >= 2 and parts[0] == "DELETE":
                    try:
                        count = int(parts[1])
                    except ValueError:
                        pass
            results[table] = count
        
        if results[table] > 0:
            print(f"  {prefix} {results[table]} rows from {table}")
    
    results["total_deleted"] = sum(results.values())
    return results
