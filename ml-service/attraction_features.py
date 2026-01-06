"""
Attraction-specific feature engineering
Uses only data that is actually available in the database
"""

import pandas as pd
from db import get_db
from sqlalchemy import text


def add_attraction_type_feature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add attraction type feature from database

    Only adds if attractionType is available in the data.
    Handles missing values gracefully (defaults to 'UNKNOWN').

    Args:
        df: DataFrame with 'attractionId' column

    Returns:
        DataFrame with 'attraction_type' column added
    """
    df = df.copy()

    # Check if attractionType is already in DataFrame (from training data fetch)
    if "attractionType" in df.columns:
        df["attraction_type"] = df["attractionType"].fillna("UNKNOWN").astype(str)
        return df

    # Otherwise, fetch from database (for inference)
    attraction_ids = df["attractionId"].unique().tolist()
    if not attraction_ids:
        df["attraction_type"] = "UNKNOWN"
        return df

    try:
        query = text("""
            SELECT
                id::text as "attractionId",
                COALESCE("attractionType", 'UNKNOWN') as "attraction_type"
            FROM attractions
            WHERE id::text = ANY(:attraction_ids)
        """)

        with get_db() as db:
            result = db.execute(query, {"attraction_ids": attraction_ids})
            type_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if not type_df.empty:
            # Merge attraction types
            df = df.merge(type_df, on="attractionId", how="left")
            df["attraction_type"] = df["attraction_type"].fillna("UNKNOWN").astype(str)
        else:
            df["attraction_type"] = "UNKNOWN"
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(
            f"Failed to fetch attraction types: {e}. Using default 'UNKNOWN'."
        )
        df["attraction_type"] = "UNKNOWN"

    return df


def add_park_attraction_count_feature(
    df: pd.DataFrame, parks_metadata: pd.DataFrame
) -> pd.DataFrame:
    """
    Add park attraction count feature

    Uses pre-fetched parks_metadata which includes attraction_count.
    If not available, fetches from database.

    Args:
        df: DataFrame with 'parkId' column
        parks_metadata: DataFrame with park metadata (may include attraction_count)

    Returns:
        DataFrame with 'park_attraction_count' column added
    """
    df = df.copy()

    # Check if attraction_count is in parks_metadata
    if "attraction_count" in parks_metadata.columns:
        park_counts = parks_metadata.set_index("park_id")["attraction_count"].to_dict()
        df["park_attraction_count"] = (
            df["parkId"].map(lambda x: park_counts.get(str(x), 0)).fillna(0).astype(int)
        )
        return df

    # Otherwise, fetch from database
    park_ids = df["parkId"].unique().tolist()
    if not park_ids:
        df["park_attraction_count"] = 0
        return df

    try:
        query = text("""
            SELECT
                p.id::text as "parkId",
                COUNT(DISTINCT a.id) as attraction_count
            FROM parks p
            LEFT JOIN attractions a ON a."parkId" = p.id
            WHERE p.id::text = ANY(:park_ids)
            GROUP BY p.id
        """)

        with get_db() as db:
            result = db.execute(query, {"park_ids": park_ids})
            count_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if not count_df.empty:
            df = df.merge(count_df, on="parkId", how="left")
            df["park_attraction_count"] = df["attraction_count"].fillna(0).astype(int)
            df = df.drop(columns=["attraction_count"], errors="ignore")
        else:
            df["park_attraction_count"] = 0
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to fetch park attraction counts: {e}. Using default 0.")
        df["park_attraction_count"] = 0

    return df
