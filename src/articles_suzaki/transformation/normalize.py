"""Normalize raw articles from MongoDB into warehouse schema."""
import hashlib
from datetime import datetime, timezone
from typing import Any
from loguru import logger
import pandas as pd


def normalize_timestamp(value: Any) -> datetime | None:
    """Convert various timestamp formats to UTC datetime.
    
    Args:
        value: Timestamp in various formats (string, datetime, None)
        
    Returns:
        datetime in UTC or None if conversion fails
    """
    if value is None:
        return None
    
    if isinstance(value, datetime):
        # Ensure UTC timezone
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    
    if isinstance(value, str):
        try:
            # Try ISO format first
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc)
        except (ValueError, AttributeError):
            logger.warning(f"Could not parse timestamp: {value}")
            return None
    
    return None


def calculate_checksum(record: dict) -> str:
    """Calculate SHA256 hash of normalized business fields.
    
    Args:
        record: Article record dict
        
    Returns:
        Hex-encoded SHA256 checksum
    """
    # Fields that constitute business content
    content = {
        'title': record.get('title', ''),
        'content': record.get('content', ''),
        'published_at': str(record.get('published_at', '')),
        'author_id': record.get('author_id'),
    }
    
    # Create deterministic string representation
    content_str = str(sorted(content.items()))
    return hashlib.sha256(content_str.encode()).hexdigest()


def normalize_article(raw_article: dict, batch_id: str, now: datetime) -> dict | None:
    """Transform raw MongoDB article into warehouse schema.
    
    Args:
        raw_article: Raw article document from MongoDB
        batch_id: Batch identifier for this ingestion
        now: Current timestamp in UTC
        
    Returns:
        Normalized article dict or None if validation fails
    """
    # Required fields validation
    if not raw_article.get('id'):
        logger.error("Article missing required 'id' field")
        return None
    
    if not raw_article.get('webTitle'):
        logger.error(f"Article {raw_article.get('id')} missing required 'webTitle'")
        return None
    
    published_at = normalize_timestamp(raw_article.get('webPublicationDate'))
    if not published_at:
        logger.error(f"Article {raw_article.get('id')} missing/invalid 'webPublicationDate'")
        return None
    
    # Optional fields with safe extraction
    fields = raw_article.get('fields') or {}
    body = fields.get('bodyText')
    content = "\n".join(body) if isinstance(body, list) else body
    author_id = None  # contributor tags are not stored by ingestion
    source = 'guardian'
    source_modified_at = normalize_timestamp(fields.get('lastModified'))
    
    # Build normalized record
    normalized = {
        'id': f"{source}:{raw_article['id']}",
        'title': raw_article['webTitle'],
        'content': content,
        'published_at': published_at,
        'author_id': author_id,
        'source': source,
        'source_modified_at': source_modified_at,
        'ingestion_timestamp': now,
        'batch_id': batch_id,
        'created_at': now,
        'updated_at': now,
        'deleted_at': None,
    }
    
    # Calculate business content checksum
    normalized['checksum'] = calculate_checksum(normalized)
    
    return normalized


def normalize_batch(
    raw_articles: list[dict],
    batch_id: str,
    now: datetime | None = None,
) -> pd.DataFrame:
    """Normalize a batch of raw articles.
    
    Args:
        raw_articles: List of raw article documents from MongoDB
        batch_id: Batch identifier
        now: Current timestamp (defaults to now in UTC)
        
    Returns:
        DataFrame of normalized records
    """
    if now is None:
        now = datetime.now(timezone.utc)
    
    logger.info(f"Normalizing {len(raw_articles)} articles for batch {batch_id}")
    
    normalized_records = []
    for raw_article in raw_articles:
        normalized = normalize_article(raw_article, batch_id, now)
        if normalized:
            normalized_records.append(normalized)
        else:
            logger.warning(f"Skipped invalid article: {raw_article.get('id', 'unknown')}")
    
    logger.info(f"Successfully normalized {len(normalized_records)} articles")
    
    return pd.DataFrame(normalized_records)
