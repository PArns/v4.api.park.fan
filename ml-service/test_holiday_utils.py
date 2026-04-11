#!/usr/bin/env python3
"""
Test script for holiday calculation utilities

Tests:
- Direct holidays
- Bridge days (Friday after Thursday, Monday before Tuesday, Tuesday between Monday/Wednesday)
- Weekend extensions (Saturday/Sunday after Friday holidays)
"""

from holiday_utils import calculate_holiday_info_from_string

# Holiday entry with type
HolidayEntry = dict  # {"name": str, "type": str}

test_cases = [
    # Test 1: Direct holiday
    {
        "name": "Direct holiday - Christmas Day",
        "date": "2025-12-25",
        "holiday_map": {"2025-12-25": "Christmas Day"},
        "expected": {
            "is_holiday": True,
            "holiday_name": "Christmas Day",
            "is_bridge_day": False,
        },
    },
    # Test 2: Friday after Thursday holiday (Bridge Day)
    {
        "name": "Bridge Day - Friday after Thursday holiday",
        "date": "2025-12-26",  # Friday
        "holiday_map": {"2025-12-25": "Christmas Day"},  # Thursday
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": True,
        },
    },
    # Test 3: Monday before Tuesday holiday (Bridge Day)
    {
        "name": "Bridge Day - Monday before Tuesday holiday",
        "date": "2025-12-22",  # Monday
        "holiday_map": {"2025-12-23": "Christmas Eve"},  # Tuesday
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": True,
        },
    },
    # Test 4: Tuesday between Monday and Wednesday holidays (Bridge Day)
    {
        "name": "Bridge Day - Tuesday between Monday and Wednesday holidays",
        "date": "2025-12-23",  # Tuesday
        "holiday_map": {
            "2025-12-22": "Holiday Monday",  # Monday
            "2025-12-24": "Holiday Wednesday",  # Wednesday
        },
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": True,
        },
    },
    # Test 5: Weekend extension - Saturday after Friday holiday
    {
        "name": "Weekend extension - Saturday after Friday holiday",
        "date": "2025-12-27",  # Saturday
        "holiday_map": {"2025-12-26": "school"},  # Friday - school holiday
        "expected": {
            "is_holiday": True,
            "holiday_name": "school",
            "is_bridge_day": False,
        },
    },
    # Test 6: Weekend extension - Sunday after Friday holiday
    {
        "name": "Weekend extension - Sunday after Friday holiday",
        "date": "2025-12-28",  # Sunday
        "holiday_map": {"2025-12-26": "school"},  # Friday - school holiday
        "expected": {
            "is_holiday": True,
            "holiday_name": "school",
            "is_bridge_day": False,
        },
    },
    # Test 7: Weekend NOT extended if Friday is not a holiday
    {
        "name": "Weekend NOT extended - Saturday but Friday is not a holiday",
        "date": "2025-12-27",  # Saturday
        "holiday_map": {"2025-12-25": "Christmas Day"},  # Thursday, not Friday
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": False,
        },
    },
    # Test 8: Regular day (no holiday, no bridge day)
    {
        "name": "Regular day - no holiday",
        "date": "2025-12-29",  # Monday
        "holiday_map": {"2025-12-25": "Christmas Day"},
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": False,
        },
    },
    # Test 9: Tuesday between Monday and Wednesday - but only one is a holiday (NOT bridge day)
    {
        "name": "Tuesday NOT bridge day - only Monday is holiday",
        "date": "2025-12-23",  # Tuesday
        "holiday_map": {"2025-12-22": "Holiday Monday"},  # Only Monday
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": False,
        },
    },
    # Test 10: Thursday and Friday both holidays - Friday should be holiday, not bridge day
    {
        "name": "Friday is holiday (not bridge day) when Thursday is also holiday",
        "date": "2025-12-26",  # Friday
        "holiday_map": {
            "2025-12-25": "Christmas Day",  # Thursday
            "2025-12-26": "Boxing Day",  # Friday
        },
        "expected": {
            "is_holiday": True,
            "holiday_name": "Boxing Day",
            "is_bridge_day": False,
        },
    },
    # Test 11: Bridge day only for public holidays, not school holidays
    {
        "name": "Bridge day NOT created for school holidays - Friday after Thursday school holiday",
        "date": "2025-12-26",  # Friday
        "holiday_map": {
            "2025-12-25": "school",  # Thursday - school holiday (type encoded in value)
        },
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": False,  # Should NOT be bridge day for school holidays
        },
    },
    # Test 12: Bridge day for public holidays
    {
        "name": "Bridge day created for public holidays - Friday after Thursday public holiday",
        "date": "2025-12-26",  # Friday
        "holiday_map": {
            "2025-12-25": "public",  # Thursday - public holiday (type encoded in value)
        },
        "expected": {
            "is_holiday": False,
            "holiday_name": None,
            "is_bridge_day": True,  # SHOULD be bridge day for public holidays
        },
    },
]


def run_tests():
    print("🧪 Testing Holiday Calculation Utilities\n")
    print("=" * 80)

    passed = 0
    failed = 0

    for test_case in test_cases:
        result = calculate_holiday_info_from_string(
            test_case["date"], test_case["holiday_map"]
        )

        is_holiday, holiday_name, is_bridge_day = result
        expected = test_case["expected"]

        success = (
            is_holiday == expected["is_holiday"]
            and holiday_name == expected["holiday_name"]
            and is_bridge_day == expected["is_bridge_day"]
        )

        if success:
            print(f"✅ {test_case['name']}")
            passed += 1
        else:
            print(f"❌ {test_case['name']}")
            print(f"   Expected: {expected}")
            print(
                f"   Got: (is_holiday={is_holiday}, holiday_name={holiday_name}, is_bridge_day={is_bridge_day})"
            )
            failed += 1

    print("\n" + "=" * 80)
    print(f"Results: {passed} passed, {failed} failed")

    if failed > 0:
        exit(1)


if __name__ == "__main__":
    run_tests()
