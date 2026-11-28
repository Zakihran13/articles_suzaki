"""Deduplication logic for selecting one record per article ID."""
from typing import List
from loguru import logger
import pandas as pd


def deduplicate_articles(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Select one record per article ID using source_modified_at as tiebreaker.
    
    When multiple records have the same ID:
    1. Choose the one with the newest source_modified_at
    2. If timestamps are equal, use deterministic tie-breaker (title lexicographically)
    
    Args:
        df: DataFrame of normalized articles
        
    Returns:
        Tuple of (deduplicated_df, duplicate_count)
    """
    if df.empty:
        return df, 0
    
    total_records = len(df)
    
    # Sort by ID, then by source_modified_at (newest first), then by title (deterministic)
    df_sorted = df.sort_values(
        by=['id', 'source_modified_at', 'title'],
        ascending=[True, False, True],
        na_position='last'
    )
    
    # Keep only the first occurrence (newest/highest) per ID
    df_dedup = df_sorted.drop_duplicates(subset=['id'], keep='first')
    
    duplicate_count = total_records - len(df_dedup)
    
    if duplicate_count > 0:
        logger.info(f"Deduplicated {duplicate_count} duplicate records from {total_records} total")
    
    return df_dedup.reset_index(drop=True), duplicate_count
