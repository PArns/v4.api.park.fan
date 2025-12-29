
import pandas as pd
from datetime import datetime
import numpy as np

# Mocking db functions to avoid DB dependency in verification
import sys
from unittest.mock import MagicMock

# Mock db module
db_mock = MagicMock()
sys.modules['db'] = db_mock

# Mock features module dependencies
# Add local directory to path
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))
import features

def test_feature_engineering():
    print("🧪 Testing Feature Engineering...")
    
    # 1. Create dummy input data
    # Create timestamps covering a full day to check cyclical features
    timestamps = pd.date_range(start='2025-12-01 00:00', end='2025-12-01 23:00', freq='H')
    df = pd.DataFrame({
        'parkId': ['p1'] * len(timestamps),
        'attractionId': ['a1'] * len(timestamps),
        'timestamp': timestamps,
        'waitTime': [10 + i for i in range(len(timestamps))], # Increasing wait time
        'status': ['OPERATING'] * len(timestamps),
        'queueType': ['STANDBY'] * len(timestamps)
    })
    
    # Mock parks metadata
    parks_metadata = pd.DataFrame({
        'park_id': ['p1'],
        'country': ['DE'],
        'timezone': ['Europe/Berlin'],
        'influencingRegions': [[{'countryCode': 'FR', 'regionCode': None}, {'countryCode': 'NL', 'regionCode': None}]],
        'latitude': [50.0],
        'longitude': [10.0]
    })
    db_mock.fetch_parks_metadata.return_value = parks_metadata
    
    # Mock holidays
    db_mock.fetch_holidays.return_value = pd.DataFrame({
        'country': ['DE', 'FR'],
        'date': [pd.to_datetime('2025-12-01'), pd.to_datetime('2025-12-01')],
        'holiday_type': ['school', 'public'], # DE=school, FR=public
        'is_nationwide': [True, True]
    })
    
    # Mock schedules
    db_mock.fetch_park_schedules.return_value = pd.DataFrame() # No special schedule
    
    # Mock pytz for timezone conversion
    try:
        import pytz
    except ImportError:
        print("Scipy/pytz might be missing in this env, but logic should handle it.")
        
    # 2. Run feature engineering
    # Using internal functions to test specific logic
    print("   Adding time features...")
    df = features.add_time_features(df, parks_metadata)
    
    print("   Adding holiday features...")
    df = features.add_holiday_features(df, parks_metadata, datetime(2025,12,1), datetime(2025,12,2))
    
    print("   Adding historical features...")
    df['waitTime'] = df['waitTime'].astype(float)
    df = features.add_historical_features(df)

    # 3. Verify Output
    print("\n🔍 Verification Results:")
    
    # Check Cyclical Features
    print(f"   Shape: {df.shape}")
    if 'hour_sin' in df.columns:
        print("   ✅ Cyclical features (hour_sin/cos) present")
        # Check values for 00:00 (hour 0) -> sin(0) = 0, cos(0) = 1
        row0 = df.iloc[0]
        # sin(0) shoud be 0
        if abs(row0['hour_sin']) < 0.01 and abs(row0['hour_cos'] - 1.0) < 0.01:
             print("   ✅ Cyclical values correct for 00:00")
        else:
             print(f"   ❌ Cyclical values INCORRECT for 00:00: sin={row0['hour_sin']}, cos={row0['hour_cos']}")
    else:
        print("   ❌ Cyclical features MISSING")

    # Check Holiday Split
    # We mocked DE (primary) as 'school' and FR (neighbor) as 'public'
    # park p1 is in DE.
    # So is_school_holiday_primary should be 1, is_holiday_primary should be 0.
    # is_holiday_neighbor_1 (FR) should be 1 (public).
    
    row0 = df.iloc[0]
    if row0['is_school_holiday_primary'] == 1 and row0['is_holiday_primary'] == 0:
        print("   ✅ Primary Holiday split correct (School=1, Public=0)")
    else:
        print(f"   ❌ Primary Holiday split INCORRECT: School={row0['is_school_holiday_primary']}, Public={row0['is_holiday_primary']}")
        
    if row0['is_holiday_neighbor_1'] == 1:
        print("   ✅ Neighbor Public Holiday detected")
    else: 
        print(f"   ❌ Neighbor Public Holiday NOT detected")

    # Check 1h Trend
    # We mocked increasing wait times: 10, 11, 12...
    # avg_wait_last_1h should be computed
    if 'avg_wait_last_1h' in df.columns:
        print("   ✅ avg_wait_last_1h present")
        # Check logic: row 2 (hour 2) -> avg of last 1h (window) -> should be close to previous values
        # logic is rolling mean shifted.
        print(f"   Sample avg_wait_last_1h: {df['avg_wait_last_1h'].iloc[5]}")
    else:
        print("   ❌ avg_wait_last_1h MISSING")

if __name__ == "__main__":
    test_feature_engineering()
