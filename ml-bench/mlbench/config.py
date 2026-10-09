"""Every number the benchmark fixes, in one place (BENCH-SPEC.md is the authority).

The run writes this object to ``results/<run-id>/config.json`` so a result can
always be traced back to the exact settings that produced it.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field

# Lead grid (BENCH-SPEC "Horizon"). d0 = the same service day, scored from the
# 06:00 origin and from the intraday origins.
SLOT_LEADS = [0, 1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 45, 60, 90]
# Daily-level leads for UC4 (crowd calendar / yearly outlook) are reported on this
# grid. Daily levels are cheap (one number per headliner-day), so the runner scores
# EVERY lead 1..90 (the calendar's day comparison and "Empfohlen" star need whole
# months) plus the far ones.
UC4_LEADS = [1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 45, 60, 90, 120, 180, 270, 365]
DAILY_LEADS = list(range(1, 91)) + [120, 180, 270, 365]
# Leads for the planner-optimiser simulation (D4) — the expensive decision metric.
OPTIMISER_LEADS = [1, 3, 7, 14, 30]


@dataclass
class BenchConfig:
    origin_hour_local: int = 6                  # daily origin, park-local
    intraday_hours_local: list[int] = field(default_factory=lambda: [8, 10, 12, 14, 16, 18, 20])
    slot_leads: list[int] = field(default_factory=lambda: list(SLOT_LEADS))
    daily_leads: list[int] = field(default_factory=lambda: list(DAILY_LEADS))
    optimiser_leads: list[int] = field(default_factory=lambda: list(OPTIMISER_LEADS))
    window_days: int = 56                       # profile window and warm-up
    busy_q90_min: float = 45.0                  # ex-ante busy: ride q90 over window >= 45
    min_days_daytype: int = 3                   # weekday/weekend profile: >= 3 days per slot
    min_days_all: int = 4                       # all-days profile: >= 4 days per slot
    naive_level_weeks: int = 4                  # level (a): median of the last 4 same-weekday P90s
    naive_level_min: int = 2                    # ... of which at least 2 must exist
    min_ride_day_slots: int = 8                 # a ride-day needs >= 8 truth slots for a daily P90
    # production-as-served reconstruction (/plan/day composed tier)
    prod_profile_days: int = 365                # /stats/hourly windowYears=1
    prod_min_hour_days: int = 10                # an hour needs 10 measured days
    prod_min_ride_days: int = 20                # minAttractionDays
    prod_min_lead: int = 3                      # d<3 production serves CatBoost hourly (not reconstructable)
    # crowd levels (src/common/utils/crowd-level.util.ts)
    typical_peak_min_days: int = 30            # NULL typicalDayPeak below 30 operating days
    # decision thresholds mirrored from the frontend / API
    next_best_lookahead_min: int = 120          # NEXT_RIDE_LOOKAHEAD_MIN
    next_best_min_saving: float = 10.0          # NEXT_RIDE_MIN_SAVING_MIN
    next_best_limit: int = 3                    # NEXT_RIDE_LIMIT
    live_window_min: int = 45                   # LIVE_WINDOW_MIN
    rope_worth_peak: float = 60.0               # worthPeakFloor
    rope_worth_savings: float = 45.0            # worthSavingsFloor
    idle_weight: float = 0.5                    # IDLE_WEIGHT
    optimiser_rides: int = 4                    # fixed ride set per park-day (top headliners, ex-ante)
    optimiser_max_delay_min: int = 120          # MAX_DELAY_MIN
    ride_duration_min: int = 5                  # RIDE_DURATION_MIN
    day_compare_clear: float = 0.5              # CLEAR_MARGIN
    day_compare_tie: float = 0.1                # TIE_MARGIN
    star_margin: float = 0.5                    # BEST_DAY_MARGIN
    star_min_candidates: int = 4
    # bootstrap
    bootstrap_reps: int = 1000
    bootstrap_seed: int = 827
    low_n_origin_days: int = 30
    # resources
    memory_limit: str = "6GB"
    threads: int = 6

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), indent=2, sort_keys=True)


def region_of(timezone: str | None) -> str:
    if not timezone:
        return "other"
    head = timezone.split("/", 1)[0]
    return {"Europe": "EU", "America": "NA", "Asia": "Asia", "Australia": "Asia",
            "Pacific": "Asia"}.get(head, "other")


REGION_SQL = """CASE split_part(p.timezone, '/', 1) WHEN 'Europe' THEN 'EU' WHEN 'America' THEN 'NA'
  WHEN 'Asia' THEN 'Asia' WHEN 'Australia' THEN 'Asia' WHEN 'Pacific' THEN 'Asia' ELSE 'other' END"""


def season_sql(date_expr: str) -> str:
    return (f"CASE WHEN month({date_expr}) IN (12,1,2) THEN 'winter' WHEN month({date_expr}) IN (3,4,5) "
            f"THEN 'spring' WHEN month({date_expr}) IN (6,7,8) THEN 'summer' ELSE 'autumn' END")


# Crowd ladder in percent of the typical-day peak (crowd-level.util.ts).
CROWD_LADDER = ["very_low", "low", "moderate", "high", "very_high", "extreme"]


def crowd_bucket_sql(pct_expr: str) -> str:
    """0..5 index on CROWD_LADDER, NULL when the percentage is NULL. Same comparisons as
    determineCrowdLevel (a continuous 89.5 % is 'moderate' there too)."""
    return (f"CASE WHEN {pct_expr} IS NULL THEN NULL WHEN {pct_expr} <= 60 THEN 0 "
            f"WHEN {pct_expr} <= 89 THEN 1 WHEN {pct_expr} <= 110 THEN 2 WHEN {pct_expr} <= 150 THEN 3 "
            f"WHEN {pct_expr} <= 200 THEN 4 ELSE 5 END")
