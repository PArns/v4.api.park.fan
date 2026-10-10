"""A small synthetic raw export in the exact format ``mlbench.export`` writes.

Two parks (Europe/Berlin and America/New_York — one DST regime each), four rides
per park (two headliners), ~110 service days, change-log rows written only when
the posted wait changes, a DOWN period per ride every few days, TFT/CatBoost
daily forecasts created at 03:00 UTC.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

PARKS = [
    ("p-eu", "Europe/Berlin", "DE", "DE-BW", 48.26, 7.72),
    ("p-na", "America/New_York", "US", "US-FL", 28.37, -81.55),
]
START = dt.date(2026, 2, 1)
DAYS = 110


def _us(ts: pd.Timestamp | dt.datetime) -> int:
    return int(pd.Timestamp(ts).value // 1000)


def write(raw: Path, seed: int = 7) -> dict:
    rng = np.random.default_rng(seed)
    raw.mkdir(parents=True, exist_ok=True)

    def csv(name: str, df: pd.DataFrame) -> None:
        with gzip.open(raw / f"{name}.csv.gz", "wt") as f:
            df.to_csv(f, index=False)

    csv("parks", pd.DataFrame([{
        "id": pid, "slug": pid, "name": pid, "timezone": tz, "continent": "x", "country_code": cc,
        "region_code": rc, "influencing_regions": json.dumps([{"countryCode": cc, "regionCode": None}]),
        "latitude": lat, "longitude": lng, "park_type": "THEME_PARK"} for pid, tz, cc, rc, lat, lng in PARKS]))
    rides = []
    for pid, *_ in PARKS:
        for k in range(4):
            rides.append({"id": f"{pid}-r{k}", "park_id": pid, "name": f"r{k}", "slug": f"r{k}",
                          "attraction_type": "ATTRACTION", "attraction_kind": "RIDE", "is_seasonal": False,
                          "open_with_park": False, "latitude": 48.26 + 0.001 * k, "longitude": 7.72,
                          "land": f"land{k % 2}", "retired_at_us": None,
                          "headliner_tier": "A" if k < 2 else None, "is_headliner": k < 2})
    csv("attractions", pd.DataFrame(rides))
    sched, queue, levels = [], [], []
    for pid, tz, *_ in PARKS:
        z = ZoneInfo(tz)
        for i in range(DAYS):
            d = START + dt.timedelta(days=i)
            op = dt.datetime.combine(d, dt.time(10), z)
            cl = dt.datetime.combine(d, dt.time(18 if d.weekday() < 5 else 20), z)
            sched.append({"park_id": pid, "date": d.isoformat(), "schedule_type": "OPERATING",
                          "opening_us": _us(op), "closing_us": _us(cl), "is_holiday": False,
                          "is_bridge_day": False,
                          # most days are published 20 days ahead, every third only the day before
                          "updated_us": _us(op - dt.timedelta(days=1 if i % 3 == 0 else 20))})
            if i % 4 == 0:
                # A non-OPERATING row carrying isHoliday / isBridgeDay, written LATE —
                # after the day's OPERATING row. `windows` ignores it (no times), so the
                # OPERATING mask says "schedule known" while the two flags are in fact
                # from the future. Exercises critic S11.
                sched.append({"park_id": pid, "date": d.isoformat(), "schedule_type": "SPECIAL_HOURS",
                              "opening_us": None, "closing_us": None, "is_holiday": True,
                              "is_bridge_day": True, "updated_us": _us(op + dt.timedelta(hours=12))})
            weekend = 1.5 if d.weekday() >= 5 else 1.0
            for k in range(4):
                aid = f"{pid}-r{k}"
                level = (60 if k < 2 else 25) * weekend * rng.lognormal(0, 0.2)
                levels.append((aid, d, level))
                down = (i + k) % 6 == 0
                # on some ride-days the feed DROPS the ride half way through the day and
                # production keeps writing heartbeats: the same status and wait again with
                # a fresh ts. That is what resets the 3 h staleness clock, so the truth
                # runs to closing with a frozen wait. `build --drop-heartbeats` builds the
                # other target, where it stops 3 h after the last real reading.
                dropped = (i + k) % 7 == 0
                t = op - dt.timedelta(minutes=20)
                last = None
                while t < cl + dt.timedelta(minutes=30):
                    frac = (t - op).total_seconds() / max((cl - op).total_seconds(), 1)
                    shape = 0.3 + 0.7 * np.sin(np.pi * min(max(frac, 0), 1))
                    w = int(round(level * shape / 5) * 5)
                    status = "OPERATING"
                    if t < op or t >= cl:
                        status, w = "CLOSED", None
                    elif down and 0.4 < frac < 0.5:
                        status, w = "DOWN", None
                    if dropped and 0.35 < frac and last is not None:
                        queue.append({"attraction_id": aid, "ts_us": _us(t), "status": last[0],
                                      "wait": last[1], "is_heartbeat": True,
                                      "d": t.astimezone(dt.timezone.utc).date()})
                    elif (status, w) != last:
                        queue.append({"attraction_id": aid, "ts_us": _us(t), "status": status,
                                      "wait": w, "is_heartbeat": False, "d": t.astimezone(dt.timezone.utc).date()})
                        last = (status, w)
                    t += dt.timedelta(minutes=int(rng.integers(5, 16)))
    # nullable Int64: a non-OPERATING row has no opening/closing, and a float column
    # would make the export CSV write 1.77e+15 where the real export writes an integer
    csv("schedule", pd.DataFrame(sched).astype({"opening_us": "Int64", "closing_us": "Int64",
                                                "updated_us": "Int64"}))
    csv("holidays", pd.DataFrame([{"country": "DE", "date": "2026-04-03", "region": None,
                                   "holiday_type": "public", "is_nationwide": True, "name": "Good Friday"}]))
    csv("weather", pd.DataFrame([{"park_id": "p-eu", "date": "2026-03-01", "data_type": "historical",
                                  "temp_max": 10.0, "temp_min": 1.0, "precip_sum": 0.0, "rain_sum": 0.0,
                                  "snow_sum": 0.0, "weather_code": 1, "wind_max": 5.0, "updated_us": 0}]))
    q = pd.DataFrame(queue)
    for d, g in q.groupby("d"):
        csv(f"queue_{d.isoformat()}", g.drop(columns="d"))
    fc = []
    for aid, d, level in levels:
        for lead in range(0, 31, 1):
            f = d - dt.timedelta(days=lead)
            created = dt.datetime.combine(f, dt.time(3), dt.timezone.utc)
            fc.append({"attraction_id": aid, "target_date": d.isoformat(), "forecast_date": f.isoformat(),
                       "predicted_peak": level * rng.lognormal(0, 0.1), "model_version": "s",
                       "created_us": _us(created)})
    fc = pd.DataFrame(fc)
    csv("tft_forecasts_2026-01", fc)
    csv("catboost_daily_forecasts_2026-01", fc.assign(predicted_peak=fc["predicted_peak"] * 0.8))
    return {"queue_rows": len(q)}
