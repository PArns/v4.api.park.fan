"""
Filter predictions by park operating hours with fallback to queue data
"""

from typing import List, Dict, Any
from sqlalchemy import text
import pytz
import pandas as pd
from collections import defaultdict

from db import get_db, fetch_parks_metadata


def _filter_by_active_rides(
    park_preds: List[Dict[str, Any]],
    current_wait_times: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    When no schedule is available, suppress predictions unless the park is
    demonstrably open: at least one attraction has a current wait >= 5 min.
    If current_wait_times is not provided (e.g. park-level endpoint), fall back
    to keeping all predictions to preserve existing behaviour.
    """
    if not current_wait_times:
        return park_preds

    park_attraction_ids = {p.get("attractionId") for p in park_preds}
    park_is_open = any(
        current_wait_times.get(attr_id, 0) >= 5
        for attr_id in park_attraction_ids
    )
    return park_preds if park_is_open else []


def filter_predictions_by_schedule(
    predictions: List[Dict[str, Any]],
    park_ids: List[str],
    prediction_type: str = "hourly",
    current_wait_times: Dict[str, int] = None,
) -> List[Dict[str, Any]]:
    """
    Filter predictions to only include valid operating times.

    Strategy for HOURLY predictions:
    - Remove predictions that are AFTER park closing time for each day
    - Keep predictions that fall within operating hours (including future days)
    - This allows users to see predictions for upcoming hours even when park is currently closed

    Strategy for DAILY predictions:
    1. Primary: Use schedule_entries to filter out closed days (off-season)
    2. Fallback: Keep all predictions if no schedule data

    Args:
        predictions: List of prediction dictionaries
        park_ids: List of park IDs (for metadata lookup)
        prediction_type: 'hourly' or 'daily'

    Returns:
        Filtered list of predictions
    """
    if not predictions:
        return []

    # Fetch park metadata for timezone info
    parks_metadata = fetch_parks_metadata()

    # DB returns UUID objects; predictions use string park_ids — normalise to str.
    if not parks_metadata.empty:
        parks_metadata = parks_metadata.copy()
        parks_metadata["park_id"] = parks_metadata["park_id"].astype(str)

    # Group predictions by park
    park_predictions_map = defaultdict(list)
    for pred in predictions:
        park_id = pred.get("parkId")
        if park_id:
            park_predictions_map[park_id].append(pred)

    filtered_predictions = []

    for park_id, park_preds in park_predictions_map.items():
        # Get park metadata for timezone
        park_info = parks_metadata[parks_metadata["park_id"] == park_id]
        if park_info.empty:
            # No metadata - keep all predictions
            print(f"⚠️  No metadata for park {park_id}, keeping all predictions")
            filtered_predictions.extend(park_preds)
            continue

        timezone_str = park_info.iloc[0]["timezone"]
        try:
            park_tz = pytz.timezone(timezone_str)
        except Exception as e:
            print(
                f"⚠️  Invalid timezone {timezone_str} for park {park_id}: {e}, keeping all predictions"
            )
            filtered_predictions.extend(park_preds)
            continue

        # Convert predictions to DataFrame for vectorized processing
        preds_df = pd.DataFrame(park_preds)
        if "predictedTime" not in preds_df.columns:
            continue

        # Parse prediction times to UTC and convert to park local time
        try:
            # Handle standard JS 'Z' strings if present
            preds_df["_parsed_time"] = (
                preds_df["predictedTime"].astype(str).str.replace("Z", "+00:00")
            )
            preds_df["_utc_time"] = pd.to_datetime(preds_df["_parsed_time"], utc=True)
            preds_df["_local_time"] = preds_df["_utc_time"].dt.tz_convert(park_tz)
            preds_df["_date"] = preds_df["_local_time"].dt.date
        except Exception as e:
            print(f"⚠️  Error parsing prediction times for park {park_id}: {e}")
            continue

        unique_dates = preds_df["_date"].unique()
        if len(unique_dates) == 0:
            continue

        # Query schedule for these dates
        query = text(
            """
            SELECT
                date,
                "scheduleType",
                "openingTime",
                "closingTime"
            FROM schedule_entries
            WHERE "parkId"::text = :park_id
                AND "attractionId" IS NULL
                AND date = ANY(CAST(:dates AS DATE[]))
                AND "scheduleType" IN ('OPERATING', 'CLOSED', 'UNKNOWN')
        """
        )

        with get_db() as db:
            result = db.execute(
                query,
                {
                    "park_id": park_id,
                    "dates": [d.isoformat() for d in unique_dates],
                },
            )
            schedules_raw = result.fetchall()

        if not schedules_raw:
            # FALLBACK: No schedule data.
            # Only pass through if at least one ride in this park has a current
            # wait >= 5 min — otherwise the park is likely closed.
            filtered_predictions.extend(
                _filter_by_active_rides(park_preds, current_wait_times)
            )
            continue

        # Convert schedules to DataFrame
        sched_df = pd.DataFrame(
            schedules_raw,
            columns=["date", "scheduleType", "openingTime", "closingTime"],
        )
        sched_df["date"] = pd.to_datetime(sched_df["date"]).dt.date

        operating_mask = sched_df["scheduleType"].isin(["OPERATING", "UNKNOWN"])
        operating_dates = set(sched_df.loc[operating_mask, "date"])

        if not operating_dates and prediction_type == "daily":
            # Schedule rows exist but none are OPERATING — treat like no schedule.
            filtered_predictions.extend(
                _filter_by_active_rides(park_preds, current_wait_times)
            )
            continue

        if prediction_type == "hourly":
            # Parse DB opening/closing times to local
            sched_df["openingTime"] = pd.to_datetime(
                sched_df["openingTime"], utc=True
            ).dt.tz_convert(park_tz)
            sched_df["closingTime"] = pd.to_datetime(
                sched_df["closingTime"], utc=True
            ).dt.tz_convert(park_tz)

            # Only OPERATING entries define valid time windows.
            # CLOSED entries must never leak predictions even if they carry times.
            # UNKNOWN entries (null times) are handled separately below.
            operating_sched = sched_df[sched_df["scheduleType"] == "OPERATING"]
            unknown_dates = set(
                sched_df.loc[sched_df["scheduleType"] == "UNKNOWN", "date"]
            )

            # Predictions on OPERATING days: filter to within opening hours
            if not operating_sched.empty:
                merged = preds_df.merge(
                    operating_sched[["date", "openingTime", "closingTime"]],
                    left_on="_date",
                    right_on="date",
                    how="inner",
                )
                valid_mask = (merged["_local_time"] >= merged["openingTime"]) & (
                    merged["_local_time"] < merged["closingTime"]
                )
                valid_df = merged[valid_mask].drop(
                    columns=[
                        "_parsed_time",
                        "_utc_time",
                        "_local_time",
                        "_date",
                        "date",
                        "openingTime",
                        "closingTime",
                    ]
                )
                filtered_predictions.extend(valid_df.to_dict("records"))

            # Predictions on UNKNOWN days: apply wait-time gate (no schedule → only if rides running)
            if unknown_dates:
                unknown_preds = [
                    p
                    for p, d in zip(park_preds, preds_df["_date"])
                    if d in unknown_dates
                ]
                filtered_predictions.extend(
                    _filter_by_active_rides(unknown_preds, current_wait_times)
                )
            # CLOSED dates: no predictions (implicitly excluded — not in operating_sched or unknown_dates)

        elif prediction_type == "daily":
            # Only keep predictions for operating or unknown dates
            valid_df = preds_df[preds_df["_date"].isin(operating_dates)]
            valid_df = valid_df.drop(
                columns=["_parsed_time", "_utc_time", "_local_time", "_date"]
            )
            filtered_predictions.extend(valid_df.to_dict("records"))

    # Filtering stats not logged to avoid spam (many parks × frequent requests).
    return filtered_predictions
