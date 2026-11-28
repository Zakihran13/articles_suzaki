"""
Load testing utilities for testing ELT pipelines with large mock datasets.

This module provides tools to:
1. Generate metadata records for 1M+ articles without loading everything in memory
2. Stream articles in configurable batches
3. Measure performance and resource usage
"""

import asyncio
import time
from datetime import datetime, timezone
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorCollection
from typing import AsyncGenerator

from articles_suzaki.configuration.config import config
from articles_suzaki.ingestion.data_mock.mock_data_generator import MockArticleGenerator
from articles_suzaki.warehouse.statements import upsert_data


async def generate_mock_metadata_batch(
    collection: AsyncIOMotorCollection,
    total_records: int = 1_000_000,
    batch_size: int = 10_000,
) -> None:
    """
    Generate and insert mock metadata records for load testing.

    Args:
        collection: MongoDB metadata collection
        total_records: Total number of metadata records to create
        batch_size: Number of records to insert per batch

    This function streams records in batches to avoid memory overload.
    """
    logger.info(f"Starting metadata generation: {total_records:,} records")

    generator = MockArticleGenerator()
    inserted = 0

    for batch_num in range(0, total_records, batch_size):
        batch = []
        for i in range(batch_size):
            if batch_num + i >= total_records:
                break

            article_id = f"mock/{(batch_num + i):08d}"
            batch.append(
                {
                    "id": article_id,
                    "is_pull": False,
                    "created_at": config.current_time,
                }
            )

        if batch:
            try:
                await collection.insert_many(batch, ordered=False)
                inserted += len(batch)
                if (batch_num + batch_size) % (batch_size * 10) == 0:
                    logger.info(
                        f"Inserted {inserted:,} / {total_records:,} metadata records "
                        f"({100*inserted/total_records:.1f}%)"
                    )
            except Exception as e:
                logger.warning(f"Batch insert encountered duplicates or error: {e}")
                # Continue with next batch if some records duplicate
                continue

    logger.info(f"Metadata generation complete: {inserted:,} records inserted")


async def stream_mock_articles(
    batch_size: int = 1_000,
    total_count: int = 1_000_000,
    seed: int | None = None,
) -> AsyncGenerator[dict, None]:
    """
    Stream mock articles one at a time for processing.

    Args:
        batch_size: Articles per conceptual batch (for logging)
        total_count: Total articles to generate
        seed: Random seed for reproducibility

    Yields:
        Individual article dictionaries
    """
    generator = MockArticleGenerator(seed=seed)

    for i in range(total_count):
        article_id = f"mock/{i:08d}"
        article = generator.generate_article(article_id=article_id)
        yield article

        if (i + 1) % (batch_size * 10) == 0:
            logger.info(f"Streamed {i + 1:,} / {total_count:,} articles ({100*(i+1)/total_count:.1f}%)")


async def load_test_ingestion(
    metadata_collection: AsyncIOMotorCollection,
    news_collection: AsyncIOMotorCollection,
    total_articles: int = 1_000_000,
    batch_size: int = 1_000,
    sample_extraction: int = 100,
) -> dict:
    """
    Run a load test: insert metadata, then extract and load a sample.

    Args:
        metadata_collection: MongoDB metadata collection
        news_collection: MongoDB news collection
        total_articles: Total mock articles to generate metadata for
        batch_size: Batch size for operations
        sample_extraction: Number of articles to actually extract/transform

    Returns:
        Dictionary with performance metrics
    """
    metrics = {
        "metadata_generation_start": datetime.now(timezone.utc),
        "metadata_records": 0,
        "metadata_duration_seconds": 0,
        "extraction_start": None,
        "extraction_duration_seconds": 0,
        "articles_extracted": 0,
        "articles_loaded": 0,
        "total_duration_seconds": 0,
    }

    start_time = time.monotonic()

    # Phase 1: Generate metadata
    logger.info(f"=== Load Test Phase 1: Metadata Generation ({total_articles:,} records) ===")
    await generate_mock_metadata_batch(
        metadata_collection,
        total_records=total_articles,
        batch_size=batch_size,
    )
    metadata_records = await metadata_collection.count_documents({})
    metrics["metadata_records"] = metadata_records
    metrics["metadata_duration_seconds"] = time.monotonic() - start_time

    logger.info(
        f"Phase 1 complete: {metadata_records:,} metadata records in "
        f"{metrics['metadata_duration_seconds']:.2f}s"
    )

    # Phase 2: Extract sample articles
    logger.info(f"=== Load Test Phase 2: Extract & Load Sample ({sample_extraction} articles) ===")
    extraction_start = time.monotonic()
    metrics["extraction_start"] = datetime.now(timezone.utc)

    generator = MockArticleGenerator()
    articles = []
    article_ids = []

    async for article in stream_mock_articles(
        batch_size=batch_size,
        total_count=sample_extraction,
    ):
        articles.append(article)
        article_ids.append(article["id"])

    metrics["articles_extracted"] = len(articles)

    # Build update statements
    updates = [
        {
            "filter": {"id": a["id"]},
            "update": {
                "$set": {
                    "webTitle": a["webTitle"],
                    "webUrl": a["webUrl"],
                    "sectionId": a["sectionId"],
                    "webPublicationDate": a["webPublicationDate"],
                    "fields": a.get("fields"),
                    "updated_at": config.current_time,
                },
                "$setOnInsert": {
                    "pull_at": config.current_time,
                },
            },
        }
        for a in articles
    ]

    await upsert_data(news_collection, updates, batch_size=batch_size)
    metrics["articles_loaded"] = len(articles)
    metrics["extraction_duration_seconds"] = time.monotonic() - extraction_start

    logger.info(
        f"Phase 2 complete: Extracted & loaded {metrics['articles_loaded']} articles in "
        f"{metrics['extraction_duration_seconds']:.2f}s"
    )

    # Total metrics
    metrics["total_duration_seconds"] = time.monotonic() - start_time

    logger.info("=== Load Test Summary ===")
    logger.info(f"Metadata records generated: {metrics['metadata_records']:,}")
    logger.info(f"Metadata generation time: {metrics['metadata_duration_seconds']:.2f}s")
    logger.info(f"Articles extracted: {metrics['articles_extracted']:,}")
    logger.info(f"Articles loaded: {metrics['articles_loaded']:,}")
    logger.info(f"Extraction time: {metrics['extraction_duration_seconds']:.2f}s")
    logger.info(f"Total duration: {metrics['total_duration_seconds']:.2f}s")

    if metrics["metadata_duration_seconds"] > 0:
        metadata_rate = metrics["metadata_records"] / metrics["metadata_duration_seconds"]
        logger.info(f"Metadata generation rate: {metadata_rate:,.0f} records/second")

    if metrics["extraction_duration_seconds"] > 0:
        extraction_rate = metrics["articles_extracted"] / metrics["extraction_duration_seconds"]
        logger.info(f"Article extraction rate: {extraction_rate:,.0f} articles/second")

    return metrics
