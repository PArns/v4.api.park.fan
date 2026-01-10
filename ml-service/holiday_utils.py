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


def normalize_region_code(code: Optional[str]) -> Optional[str]:
    """
    Normalizes a region code by extracting the region part (after last "-").

    This ensures consistent comparison between different region code formats:
    - "DE-NW" -> "NW"
    - "NW" -> "NW"
    - "US-FL" -> "FL"
    - None -> None

    Args:
        code: Region code (can be "DE-NW", "NW", or None)

    Returns:
        Normalized region code (e.g., "NW") or None

    Example:
        normalize_region_code("DE-NW")  # "NW"
        normalize_region_code("NW")  # "NW"
        normalize_region_code("US-FL")  # "FL"
        normalize_region_code(None)  # None
    """
    if not code:
        return None
    # If code contains "-", extract the part after the last "-" (e.g., "DE-NW" -> "NW")
    # Otherwise use the code as-is (e.g., "NW" -> "NW")
    return code.split("-")[-1] if "-" in code else code


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
    # WICHTIG: Weekend-Extension gilt NUR für Schulferien, nicht für öffentliche Feiertage!
    if not is_holiday and (day_of_week == 5 or day_of_week == 6):
        # Weekend (5 = Saturday, 6 = Sunday)
        # Check if Friday (4) is a SCHOOL holiday (not public holiday)
        # For Saturday: go back 1 day to get Friday
        # For Sunday: go back 2 days to get Friday
        days_back = 1 if day_of_week == 5 else 2
        friday_date = date - timedelta(days=days_back)
        friday_date_str = friday_date.strftime("%Y-%m-%d")

        # Only mark weekend as holiday if Friday is a SCHOOL holiday
        friday_holiday = holiday_map.get(friday_date_str)
        if friday_holiday:
            # Check if it's a school holiday - only extend school holidays to weekends
            friday_type = "public"  # Default
            if isinstance(friday_holiday, dict):
                friday_type = friday_holiday.get("type", "public")
            elif friday_holiday in ("school", "public", "bank"):
                friday_type = friday_holiday

            if friday_type == "school":
                # If Friday is a school holiday, extend to weekend
                is_holiday = True
                holiday_name = (
                    friday_holiday
                    if isinstance(friday_holiday, str)
                    else friday_holiday.get("name")
                )
            # Public holidays do NOT extend to weekends - weekend after Christmas is just a normal weekend

    # Check Bridge Day Logic
    # 1. Friday (4) after Thursday Holiday
    # 2. Monday (0) before Tuesday Holiday
    # 3. Tuesday (1) between Monday and Wednesday holidays
    # 4. Wednesday (2) between Tuesday and Thursday holidays
    # 5. Thursday (3) between Wednesday and Friday holidays
    # Note: Only applies to public holidays, not school holidays
    is_bridge_day = False

    def is_public_holiday(date_str: str) -> bool:
        """Check if date is a public holiday (not school holiday)"""
        holiday_value = holiday_map.get(date_str)
        if not holiday_value:
            return False
        # If value is string, check if it's "public" or default to public
        if isinstance(holiday_value, str):
            # If value is just "public" or "school", use that
            if holiday_value == "public" or holiday_value == "bank":
                return True
            if holiday_value == "school":
                return False
            # Default: assume public if just a name string
            return True
        # If value is dict with type, check type
        if isinstance(holiday_value, dict):
            holiday_type = holiday_value.get("type", "public")
            return holiday_type in ("public", "bank")
        return True  # Default to public

    if day_of_week == 4:  # Friday
        # Check if Thursday is a public holiday
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")
        if is_public_holiday(prev_date_str):
            is_bridge_day = True
    elif day_of_week == 0:  # Monday
        # Check if Tuesday is a public holiday
        next_date = date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        if is_public_holiday(next_date_str):
            is_bridge_day = True
    elif day_of_week == 1:  # Tuesday
        # Check if both Monday and Wednesday are public holidays
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")
        next_date = date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        if is_public_holiday(prev_date_str) and is_public_holiday(next_date_str):
            is_bridge_day = True
    elif day_of_week == 2:  # Wednesday
        # Check if both Tuesday and Thursday are public holidays
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")
        next_date = date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        if is_public_holiday(prev_date_str) and is_public_holiday(next_date_str):
            is_bridge_day = True
    elif day_of_week == 3:  # Thursday
        # Check if both Wednesday and Friday are public holidays
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")
        next_date = date + timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        if is_public_holiday(prev_date_str) and is_public_holiday(next_date_str):
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
