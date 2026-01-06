"""
Filter predictions by park operating hours with fallback to queue data
"""

from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import text
import pytz
import pandas as pd
from collections import defaultdict

from db import get_db, fetch_parks_metadata


def filter_predictions_by_schedule(
    predictions: List[Dict[str, Any]],
    park_ids: List[str],
    prediction_type: str = "hourly",
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

    # CRITICAL FIX: Convert UUID objects to strings for comparison
    # Predictions have park_id as strings, but DB returns UUID objects
    if not parks_metadata.empty:
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
            # No metadata - keep all predictions (should be rare now with UUID fix)
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

        # Get unique dates from predictions (in park's local timezone)
        date_set = set()
        for pred in park_preds:
            try:
                pred_time_str = pred["predictedTime"]
                # Parse ISO string
                if pred_time_str.endswith("Z"):
                    pred_time_str = pred_time_str[:-1] + "+00:00"
                pred_time_utc = datetime.fromisoformat(pred_time_str)

                # Convert to park's timezone
                if pred_time_utc.tzinfo is None:
                    pred_time_utc = pytz.UTC.localize(pred_time_utc)
                pred_time_local = pred_time_utc.astimezone(park_tz)
                date_set.add(pred_time_local.date())
            except Exception as e:
                print(
                    f"⚠️  Error parsing prediction time {pred.get('predictedTime')}: {e}"
                )
                continue

        if not date_set:
            continue

        # Query schedule for these dates
        query = text("""
            SELECT
                date,
                "scheduleType",
                "openingTime",
                "closingTime"
            FROM schedule_entries
            WHERE "parkId"::text = :park_id
                AND "attractionId" IS NULL
                AND date = ANY(CAST(:dates AS DATE[]))
                AND "scheduleType" = 'OPERATING'
        """)

        with get_db() as db:
            result = db.execute(
                query, {"park_id": park_id, "dates": [d.isoformat() for d in date_set]}
            )
            schedules = result.fetchall()

            # Debug: Log schedules found
            park_name = (
                park_info.iloc[0]["name"] if "name" in park_info.columns else park_id
            )
            print(
                # Park schedule entries found (logging removed to reduce spam)
            )
            for s in schedules:
                date_str = s[0]
                opening_utc = s[2]
                closing_utc = s[3]

                # Convert to local time for display
                if opening_utc and closing_utc:
                    opening_pd = pd.Timestamp(opening_utc)
                    closing_pd = pd.Timestamp(closing_utc)

                    # Ensure timezone aware
                    if opening_pd.tzinfo is None:
                        opening_pd = opening_pd.tz_localize(pytz.UTC)
                    if closing_pd.tzinfo is None:
                        closing_pd = closing_pd.tz_localize(pytz.UTC)

                    # Convert to park timezone
                    opening_local = opening_pd.tz_convert(park_tz)
                    closing_local = closing_pd.tz_convert(park_tz)

                    print(
                        f"   - {date_str}: {opening_local.strftime('%H:%M')} to {closing_local.strftime('%H:%M')} {park_tz}"
                    )
                else:
                    print(f"   - {date_str}: No schedule")

        if schedules:
            # PRIMARY LOGIC: Filter by schedule

            if prediction_type == "hourly":
                # HOURLY: Only show predictions for TODAY (in park's timezone)
                # Filter out predictions AFTER park closing time for today
                # Get current date in park's timezone
                now_park_tz = datetime.now(park_tz)
                today_park = now_park_tz.date()

                schedule_map = {}
                for schedule_row in schedules:
                    date = schedule_row[0]
                    opening = schedule_row[2]  # openingTime (timestamp with tz)
                    closing = schedule_row[3]  # closingTime (timestamp with tz)

                    if opening and closing:
                        # Convert to pandas Timestamp first to handle datetime64 from DB
                        opening = pd.Timestamp(opening)
                        closing = pd.Timestamp(closing)

                        # Ensure timezone aware
                        if opening.tzinfo is None:
                            opening = opening.tz_localize(pytz.UTC)
                        if closing.tzinfo is None:
                            closing = closing.tz_localize(pytz.UTC)

                        # Convert to park timezone
                        opening_local = opening.tz_convert(park_tz)
                        closing_local = closing.tz_convert(park_tz)

                        schedule_map[date] = {
                            "opening": opening_local,
                            "closing": closing_local,
                        }

                # Filter predictions: Keep only those for TODAY and BEFORE or AT closing time
                for pred in park_preds:
                    try:
                        pred_time_str = pred["predictedTime"]
                        if pred_time_str.endswith("Z"):
                            pred_time_str = pred_time_str[:-1] + "+00:00"
                        pred_time_utc = datetime.fromisoformat(pred_time_str)

                        if pred_time_utc.tzinfo is None:
                            pred_time_utc = pytz.UTC.localize(pred_time_utc)
                        pred_time_local = pred_time_utc.astimezone(park_tz)
                        pred_date = pred_time_local.date()

                        # Removed strict 'today only' check to allow next-day predictions
                        # if pred_date != today_park:
                        #     continue

                        if pred_date in schedule_map:
                            opening = schedule_map[pred_date]["opening"]
                            closing = schedule_map[pred_date]["closing"]

                            # Keep predictions within operating hours (from opening up to, but NOT including, closing)
                            # Example: If park closes at 20:00, show predictions for 11:00, 12:00, ..., 19:00 but NOT 20:00
                            # Convert pred_time_local to pd.Timestamp to match opening/closing types
                            pred_time_local_ts = pd.Timestamp(pred_time_local)
                            if opening <= pred_time_local_ts < closing:
                                filtered_predictions.append(pred)
                            # else: Prediction is at or after closing time, filter it out
                        # else: No schedule for this date - park might be closed today, skip prediction
                    except Exception as e:
                        print(f"⚠️  Error filtering prediction: {e}")
                        continue

            elif prediction_type == "daily":
                # DAILY: Filter by date (only days when park is open)
                # Build set of dates when park is OPERATING
                operating_dates = set()
                for schedule_row in schedules:
                    # CRITICAL FIX: PostgreSQL DATE columns may return datetime objects
                    # depending on the driver. Ensure we're comparing date objects, not datetimes.
                    date_value = schedule_row[0]
                    if isinstance(date_value, datetime):
                        date_value = date_value.date()
                    operating_dates.add(date_value)

                print(f"🗓️  Operating dates for {park_id}: {sorted(operating_dates)}")

                # Filter predictions
                for pred in park_preds:
                    try:
                        pred_time_str = pred["predictedTime"]
                        if pred_time_str.endswith("Z"):
                            pred_time_str = pred_time_str[:-1] + "+00:00"
                        pred_time_utc = datetime.fromisoformat(pred_time_str)

                        if pred_time_utc.tzinfo is None:
                            pred_time_utc = pytz.UTC.localize(pred_time_utc)
                        pred_time_local = pred_time_utc.astimezone(park_tz)
                        pred_date = pred_time_local.date()

                        # Debug logging removed to reduce log spam

                        # Only include predictions for days when park is operating
                        if pred_date in operating_dates:
                            filtered_predictions.append(pred)
                        # else: Park closed on this day (off-season), skip
                    except Exception as e:
                        print(f"⚠️  Error filtering daily prediction: {e}")
                        continue
        else:
            # FALLBACK LOGIC: No schedule data, keep all predictions
            # Logging removed to reduce spam (only log if it's a real issue)
            filtered_predictions.extend(park_preds)

    # Log filtering statistics
    original_count = len(predictions)
    filtered_count = len(filtered_predictions)
    removed_count = original_count - filtered_count

    # Filtering stats logging removed to reduce log spam
    # Only log if significant filtering occurred (>50% removed)
    if removed_count > 0:
        removal_percentage = (
            (removed_count / original_count * 100) if original_count > 0 else 0
        )
        if removal_percentage > 50:
            print(
                f"📊 Filtering: {removal_percentage:.1f}% predictions filtered out ({filtered_count}/{original_count} kept)"
            )

    return filtered_predictions
