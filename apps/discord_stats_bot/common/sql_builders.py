"""
SQL query building utilities for Discord bot commands.
"""

import re

from datetime import datetime, timedelta, timezone
from typing import List, Tuple


def escape_sql_identifier(identifier: str) -> str:
    """Escape a SQL identifier with double quotes for PostgreSQL."""
    return f'"{identifier}"'


def create_time_filter_params(over_last_days: int) -> Tuple[str, list, str]:
    """
    Build time filter SQL clause, params list, and display text.
    
    Args:
        over_last_days: Number of days to filter by (0 for all-time)
        
    Returns:
        Tuple of (sql_clause, query_params, display_text)
    """
    if over_last_days > 0:
        time_threshold = datetime.now(timezone.utc) - timedelta(days=over_last_days)
        # Convert to naive datetime for database TIMESTAMP columns
        time_threshold = time_threshold.replace(tzinfo=None)
        time_filter = "AND mh.start_time >= $1"
        query_params = [time_threshold]
        day_text = "day" if over_last_days == 1 else "days"
        time_period_text = f"  over the last {over_last_days} {day_text}"
    else:
        time_filter = ""
        query_params = []
        time_period_text = " (All Time)"

    return time_filter, query_params, time_period_text


def build_pathfinder_filter(
    table_alias: str,
    param_start: int,
    pathfinder_ids: list,
    use_and: bool = True
) -> Tuple[str, list, int]:
    """
    Build a pathfinder filter WHERE/AND clause.
    
    Args:
        table_alias: Table alias (e.g., 'pms', 'pks')
        param_start: Starting parameter number (e.g., 1 for $1)
        pathfinder_ids: List of pathfinder player IDs
        use_and: If True, prefix with AND; if False, prefix with WHERE
        
    Returns:
        Tuple of (sql_clause, params_to_add, next_param_num)
    """
    prefix = "AND" if use_and else "WHERE"
    
    if pathfinder_ids:
        clause = (
            f"{prefix} ({table_alias}.player_name ILIKE ${param_start} "
            f"OR {table_alias}.player_name ILIKE ${param_start + 1} "
            f"OR {table_alias}.player_id = ANY(${param_start + 2}::text[]))"
        )
        params = ["PFr |%", "PF |%", pathfinder_ids]
        return clause, params, param_start + 3
    else:
        clause = (
            f"{prefix} ({table_alias}.player_name ILIKE ${param_start} "
            f"OR {table_alias}.player_name ILIKE ${param_start + 1})"
        )
        params = ["PFr |%", "PF |%"]
        return clause, params, param_start + 2


def build_lateral_name_lookup(player_id_ref: str, extra_where: str = "") -> str:
    """
    Build a LATERAL JOIN subquery to get the most recent player name.
    
    Args:
        player_id_ref: Reference to player_id column (e.g., 'tp.player_id')
        extra_where: Additional WHERE clauses (should start with AND if provided)
        
    Returns:
        SQL string for the LATERAL JOIN
    """
    return f"""LEFT JOIN LATERAL (
            SELECT pms.player_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE pms.player_id = {player_id_ref}
                {extra_where}
            ORDER BY mh.start_time DESC
            LIMIT 1
        ) rn ON TRUE"""


def build_from_clause_with_time_filter(
    table: str,
    table_alias: str,
    has_time_filter: bool
) -> Tuple[str, str]:
    """
    Build FROM clause with optional JOIN to match_history for time filtering.
    
    Args:
        table: Full table name (e.g., 'pathfinder_stats.player_match_stats')
        table_alias: Alias for the table (e.g., 'pms')
        has_time_filter: Whether time filtering is needed
        
    Returns:
        Tuple of (from_clause, time_column_prefix)
    """
    if has_time_filter:
        from_clause = f"""FROM {table} {table_alias}
                INNER JOIN pathfinder_stats.match_history mh
                    ON {table_alias}.match_id = mh.match_id"""
        return from_clause, "mh."
    else:
        return f"FROM {table} {table_alias}", ""


def build_where_clause(*clauses: str, base_filter: str = "") -> str:
    """
    Combine multiple WHERE clause fragments into a single WHERE clause.
    
    Args:
        *clauses: Variable number of clause fragments (can be empty strings)
        base_filter: A filter to always include (e.g., 'pms.column > 0')
        
    Returns:
        Combined WHERE clause string
    """
    active_clauses = [c.strip() for c in clauses if c and c.strip()]
    
    if not active_clauses and not base_filter:
        return ""
    
    result_parts = []
    has_where = False
    
    for clause in active_clauses:
        if clause.upper().startswith("WHERE "):
            if has_where:
                clause = "AND " + clause[6:]
            else:
                has_where = True
        result_parts.append(clause)
    
    if base_filter:
        if result_parts:
            result_parts.append(f"AND {base_filter}")
        else:
            result_parts.append(f"WHERE {base_filter}")
    
    return " ".join(result_parts)


def format_sql_query_with_params(query: str, params: list) -> str:
    """
    Format a SQL query with PostgreSQL-style parameters for logging.
    
    Args:
        query: SQL query string with placeholders ($1, $2, etc.)
        params: List of parameter values
        
    Returns:
        Formatted SQL query string with substituted values
    """
    formatted_query = query
    param_pattern = r'\$(\d+)'
    matches = list(re.finditer(param_pattern, formatted_query))
    
    for match in reversed(matches):
        param_index = int(match.group(1)) - 1
        
        if param_index < len(params):
            param_value = params[param_index]
            
            if param_value is None:
                formatted_value = "NULL"
            elif isinstance(param_value, str):
                escaped = param_value.replace("'", "''")
                formatted_value = f"'{escaped}'"
            elif isinstance(param_value, (int, float)):
                formatted_value = str(param_value)
            elif isinstance(param_value, datetime):
                formatted_value = f"'{param_value.isoformat()}'"
            elif isinstance(param_value, list):
                if all(isinstance(x, str) for x in param_value):
                    escaped_items = [item.replace("'", "''") for item in param_value]
                    quoted_items = [f"'{item}'" for item in escaped_items]
                    formatted_value = f"ARRAY[{', '.join(quoted_items)}]"
                else:
                    formatted_value = f"ARRAY[{', '.join(str(x) for x in param_value)}]"
            else:
                escaped = str(param_value).replace("'", "''")
                formatted_value = f"'{escaped}'"
            
            formatted_query = (
                formatted_query[:match.start()] + 
                formatted_value + 
                formatted_query[match.end():]
            )
    
    return formatted_query
