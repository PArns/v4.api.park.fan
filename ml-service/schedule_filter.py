"""
Filter predictions by park operating hours with fallback to queue data
"""
from datetime import datetime, date as date_type
from typing import List, Dict, Any
from sqlalchemy import text
import pytz
from collections import defaultdict

from db import get_db, fetch_parks_metadata


def filter_predictions_by_schedule(
    predictions: List[Dict[str, Any]],
    park_ids: List[str],
    prediction_type: str = "hourly"
) -> List[Dict[str, Any]]:
    """
    Filter predictions to only include valid operating times.
    
    Strategy for HOURLY predictions:
    1. Primary: Use schedule_entries for precise operating hours
    2. Fallback: If no schedule data, use queue_data to infer park status (≥50% rule)
    
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
    
    # Group predictions by park
    park_predictions_map = defaultdict(list)
    for pred in predictions:
        park_id = pred.get('parkId')
        if park_id:
            park_predictions_map[park_id].append(pred)
    
    filtered_predictions = []
    
    for park_id, park_preds in park_predictions_map.items():
        # Get park metadata for timezone
        park_info = parks_metadata[parks_metadata['park_id'] == park_id]
        if park_info.empty:
            # No metadata - keep all predictions
            print(f"⚠️  No metadata for park {park_id}, keeping all predictions")
            filtered_predictions.extend(park_preds)
            continue
        
        timezone_str = park_info.iloc[0]['timezone']
        try:
            park_tz = pytz.timezone(timezone_str)
        except Exception as e:
            print(f"⚠️  Invalid timezone {timezone_str} for park {park_id}: {e}, keeping all predictions")
            filtered_predictions.extend(park_preds)
            continue
        
        # Get unique dates from predictions (in park's local timezone)
        date_set = set()
        for pred in park_preds:
            try:
                pred_time_str = pred['predictedTime']
                # Parse ISO string
                if pred_time_str.endswith('Z'):
                    pred_time_str = pred_time_str[:-1] + '+00:00'
                pred_time_utc = datetime.fromisoformat(pred_time_str)
                
                # Convert to park's timezone
                if pred_time_utc.tzinfo is None:
                    pred_time_utc = pytz.UTC.localize(pred_time_utc)
                pred_time_local = pred_time_utc.astimezone(park_tz)
                date_set.add(pred_time_local.date())
            except Exception as e:
                print(f"⚠️  Error parsing prediction time {pred.get('predictedTime')}: {e}")
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
                AND date = ANY(:dates)
                AND "scheduleType" = 'OPERATING'
        """)
        
        with get_db() as db:
            result = db.execute(query, {
                "park_id": park_id,
                "dates": [d.isoformat() for d in date_set]
            })
            schedules = result.fetchall()
        
        if schedules:
            # PRIMARY LOGIC: Filter by schedule
            
            if prediction_type == "hourly":
                # HOURLY: Filter by time of day (within opening-closing hours)
                schedule_map = {}
                for schedule_row in schedules:
                    date = schedule_row[0]
                    opening = schedule_row[2]  # openingTime (timestamp with tz)
                    closing = schedule_row[3]  # closingTime (timestamp with tz)
                    
                    if opening and closing:
                        # Ensure timezone aware
                        if opening.tzinfo is None:
                            opening = pytz.UTC.localize(opening)
                        if closing.tzinfo is None:
                            closing = pytz.UTC.localize(closing)
                        
                        # Convert to park timezone
                        opening_local = opening.astimezone(park_tz)
                        closing_local = closing.astimezone(park_tz)
                        
                        schedule_map[date] = {
                            'opening': opening_local,
                            'closing': closing_local
                        }
                
                # Filter predictions
                for pred in park_preds:
                    try:
                        pred_time_str = pred['predictedTime']
                        if pred_time_str.endswith('Z'):
                            pred_time_str = pred_time_str[:-1] + '+00:00'
                        pred_time_utc = datetime.fromisoformat(pred_time_str)
                        
                        if pred_time_utc.tzinfo is None:
                            pred_time_utc = pytz.UTC.localize(pred_time_utc)
                        pred_time_local = pred_time_utc.astimezone(park_tz)
                        pred_date = pred_time_local.date()
                        
                        if pred_date in schedule_map:
                            opening = schedule_map[pred_date]['opening']
                            closing = schedule_map[pred_date]['closing']
                            
                            # Check if prediction time is within operating hours
                            if opening <= pred_time_local <= closing:
                                filtered_predictions.append(pred)
                        # else: No schedule for this date - park might be closed, skip prediction
                    except Exception as e:
                        print(f"⚠️  Error filtering prediction: {e}")
                        continue
                        
            elif prediction_type == "daily":
                # DAILY: Filter by date (only days when park is open)
                # Build set of dates when park is OPERATING
                operating_dates = set()
                for schedule_row in schedules:
                    operating_dates.add(schedule_row[0])
                
                # Filter predictions
                for pred in park_preds:
                    try:
                        pred_time_str = pred['predictedTime']
                        if pred_time_str.endswith('Z'):
                            pred_time_str = pred_time_str[:-1] + '+00:00'
                        pred_time_utc = datetime.fromisoformat(pred_time_str)
                        
                        if pred_time_utc.tzinfo is None:
                            pred_time_utc = pytz.UTC.localize(pred_time_utc)
                        pred_time_local = pred_time_utc.astimezone(park_tz)
                        pred_date = pred_time_local.date()
                        
                        # Only include predictions for days when park is operating
                        if pred_date in operating_dates:
                            filtered_predictions.append(pred)
                        # else: Park closed on this day (off-season), skip
                    except Exception as e:
                        print(f"⚠️  Error filtering daily prediction: {e}")
                        continue
        else:
            # FALLBACK LOGIC: No schedule data
            
            if prediction_type == "hourly":
                # For hourly: use queue data to infer park status
                print(f"⚠️  No schedule data for park {park_id}, using queue data fallback")
                
                # Query current queue data
                queue_query = text("""
                    SELECT
                        COUNT(DISTINCT q."attractionId") as total_attractions,
                        COUNT(DISTINCT CASE WHEN q.status = 'OPERATING' THEN q."attractionId" END) as operating_attractions
                    FROM queue_data q
                    INNER JOIN attractions a ON q."attractionId"::text = a.id::text
                    WHERE a."parkId"::text = :park_id
                        AND q.timestamp >= NOW() - INTERVAL '1 hour'
                """)
                
                with get_db() as db:
                    result = db.execute(queue_query, {"park_id": park_id})
                    queue_stats = result.fetchone()
                
                if queue_stats and queue_stats[0] > 0:
                    total = queue_stats[0]
                    operating = queue_stats[1] or 0
                    operating_percentage = operating / total if total > 0 else 0
                    
                    if operating_percentage >= 0.5:
                        # Park is considered OPERATING (≥50% attractions operating)
                        print(f"✓ Park {park_id} inferred as OPERATING ({operating}/{total} = {operating_percentage:.0%})")
                        filtered_predictions.extend(park_preds)
                    else:
                        # Park is considered CLOSED (<50% attractions operating)
                        print(f"✗ Park {park_id} inferred as CLOSED ({operating}/{total} = {operating_percentage:.0%})")
                        # Skip all predictions for this park
                else:
                    # No queue data either - default to keeping predictions
                    print(f"⚠️  No queue data for park {park_id}, keeping all predictions")
                    filtered_predictions.extend(park_preds)
                    
            elif prediction_type == "daily":
                # For daily: without schedule data, keep all predictions
                # (We can't infer off-season from queue data)
                print(f"⚠️  No schedule data for park {park_id}, keeping all daily predictions")
                filtered_predictions.extend(park_preds)
    
    # Log filtering statistics
    original_count = len(predictions)
    filtered_count = len(filtered_predictions)
    removed_count = original_count - filtered_count
    
    if removed_count > 0:
        removal_percentage = (removed_count / original_count * 100) if original_count > 0 else 0
        print(f"📊 Filtering stats: Kept {filtered_count}/{original_count} predictions ({removal_percentage:.1f}% filtered out)")
    
    return filtered_predictions
