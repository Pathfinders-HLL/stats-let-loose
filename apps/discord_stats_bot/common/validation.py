"""
Input validation utilities for Discord bot commands.
"""


def validate_over_last_days(over_last_days: int) -> None:
    """Raise ValueError if days is negative."""
    if over_last_days < 0:
        raise ValueError(
            f"Invalid number of days: {over_last_days}. Must be >= 0."
        )


def validate_choice_parameter(
    parameter_name: str,
    value: str,
    valid_choices: set,
    display_choices: list = None
) -> str:
    """
    Validate and normalize a choice parameter.
    
    Args:
        parameter_name: Name of the parameter for error messages
        value: The value to validate
        valid_choices: Set of valid lowercase values
        display_choices: Optional list of display names for error messages
        
    Returns:
        Normalized (lowercase, stripped) value
        
    Raises:
        ValueError: If value is not in valid_choices
    """
    normalized_value = value.lower().strip()
    if normalized_value not in valid_choices:
        display_list = display_choices or list(valid_choices)
        raise ValueError(
            f"Invalid {parameter_name}: {value}. Valid options: {', '.join(display_list)}"
        )
    return normalized_value
