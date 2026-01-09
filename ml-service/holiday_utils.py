"""
Holiday Calculation Utilities

Centralized logic for determining if a date is a holiday, bridge day,
or weekend extension of a holiday. This logic matches the TypeScript
implementation in src/common/utils/holiday.utils.ts.

Rules:
1. Direct holiday: Date is in the holiday map
2. Bridge day: Friday after Thursday holiday OR Monday before Tuesday holiday
3. Weekend extension: Saturday/Sunday after Friday holiday
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


def calculate_holiday_info(
    date: datetime,
    holiday_map: Dict[str, str],
    timezone: str = "UTC",
) -> Tuple[bool, Optional[str], bool]:
    """
    Calculate holiday information for a given date.

    Args:
        date: The date to check
        holiday_map: Dictionary of date strings (YYYY-MM-DD) to holiday names
        timezone: IANA timezone for date calculations (e.g., "Europe/Berlin")
                 Note: Currently assumes dates are already in the correct timezone

    Returns:
        Tuple of (is_holiday, holiday_name, is_bridge_day)

    Example:
        holiday_map = {
            "2025-12-25": "Christmas Day",
            "2025-12-26": "Boxing Day"
        }
        info = calculate_holiday_info(
            datetime(2025, 12, 27),
            holiday_map,
            "Europe/Berlin"
        )
        # Returns: (True, "Christmas Day", False)
    """
    # Format date as YYYY-MM-DD
    date_str = date.strftime("%Y-%m-%d")
    holiday_name = holiday_map.get(date_str)
    is_holiday = holiday_name is not None

    day_of_week = date.weekday()  # 0 = Monday, 6 = Sunday

    # Check if weekend after Friday holiday
    # Ferien gelten nur übers Wochenende, wenn freitags ein ferientag ist
    if not is_holiday and (day_of_week == 5 or day_of_week == 6):
        # Weekend (5 = Saturday, 6 = Sunday)
        # Check if Friday (4) is a holiday
        # For Saturday: go back 1 day to get Friday
        # For Sunday: go back 2 days to get Friday
        days_back = 1 if day_of_week == 5 else 2
        friday_date = date - timedelta(days=days_back)
        friday_date_str = friday_date.strftime("%Y-%m-%d")

        # Only mark weekend as holiday if Friday is a holiday
        if friday_date_str in holiday_map:
            is_holiday = True
            holiday_name = holiday_map[friday_date_str]

    # Check Bridge Day Logic
    # Friday (4) after Thursday Holiday OR Monday (0) before Tuesday Holiday
    is_bridge_day = False

    if day_of_week == 4:  # Friday
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")
        if prev_date_str in holiday_map:
            is_bridge_day = True
    elif day_of_week == 0:  # Monday
        next_date = date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        if next_date_str in holiday_map:
            is_bridge_day = True

    # Bridge day cannot be a holiday
    final_is_bridge_day = False if is_holiday else is_bridge_day

    return (is_holiday, holiday_name, final_is_bridge_day)


def calculate_holiday_info_from_string(
    date_str: str,
    holiday_map: Dict[str, str],
    timezone: str = "UTC",
) -> Tuple[bool, Optional[str], bool]:
    """
    Calculate holiday information for a date string (YYYY-MM-DD).

    Convenience function that parses a date string and calls calculate_holiday_info.

    Args:
        date_str: Date string in YYYY-MM-DD format
        holiday_map: Dictionary of date strings (YYYY-MM-DD) to holiday names
        timezone: IANA timezone for date calculations

    Returns:
        Tuple of (is_holiday, holiday_name, is_bridge_day)
    """
    # Parse date string (YYYY-MM-DD)
    date = datetime.strptime(date_str, "%Y-%m-%d")
    return calculate_holiday_info(date, holiday_map, timezone)
