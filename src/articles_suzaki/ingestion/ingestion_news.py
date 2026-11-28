import asyncio
from datetime import datetime
from loguru import logger
import pandas as pd
from typing import List, Any, Callable, AsyncGenerator
from motor.motor_asyncio import AsyncIOMotorCollection


from articles_suzaki.configuration.config import config
from articles_suzaki.ingestion.data_mock.source import create_source, DataSource
from articles_suzaki.warehouse.client import get_async_mongodb
from articles_suzaki.warehouse.statements import upsert_data, get_news_ids


async def store_data_batch(
    coll: AsyncIOMotorCollection,
    data: List[Any],
    updates_statement: Callable,
    batch_number: int | None = None,
):
    """Store a batch of data with optional logging."""
    if not data:
        return

    batch_info = f" (batch {batch_number})" if batch_number else ""
    logger.info(f"Storing {len(data)} records{batch_info}")

    data_update = updates_statement(
        data,
        config.current_time,
    )

    await upsert_data(coll, data_update)


def build_news_updates(
    data: List[dict],
    now: datetime,
) -> List[dict]:

    return [
        {
            "filter": {"id": n["id"]},
            "update": {
                "$set": {
                    "webTitle": n["webTitle"],
                    "webUrl": n["webUrl"],
                    "sectionId": n["sectionId"],
                    "webPublicationDate": n["webPublicationDate"],
                    "fields": n.get("fields"),
                    "updated_at": now,
                    "deleted_at": None,  # For future recheck of deleted articles
                },
                "$setOnInsert": {
                    "pull_at": now,
                },
            },
        }
        for n in data
    ]


def build_metadata_updates(data: List[Any], now: datetime):

    return [
        {
            "filter": {"id": n["id"]},
            "update": {
                "$set": {
                    "is_pull": True,
                    "updated_at": now,
                }
            },
        }
        for n in data
    ]


async def store_news_batch(
    coll_news: AsyncIOMotorCollection,
    coll_meta: AsyncIOMotorCollection,
    news_batch: List[dict],
    ids_batch: List[dict],
    batch_number: int,
) -> None:
    await store_data_batch(coll_news, news_batch, build_news_updates, batch_number)
    await store_data_batch(coll_meta, ids_batch, build_metadata_updates, batch_number)


async def extract_news_stream(
    source: DataSource,
    news_ids: List[Any],
    batch_size: int = 100,
) -> AsyncGenerator[tuple[List[dict], List[dict]], None]:
    """Stream news data in batches (async generator).

    This yields pairs of (news_data, news_ids) as they're fetched,
    allowing concurrent processing without memory buildup.

    Args:
        source: DataSource instance (API or mock)
        news_ids: List of metadata IDs to fetch
        batch_size: Records per batch yield

    Yields:
        Tuples of (news_list, news_ids_list) ready for insertion
    """
    pending = [
        news_metadata
        for news_metadata in news_ids
        if not news_metadata.get("is_pull", False)
    ]

    logger.info(
        f"Found {len(pending)} metadata entries to pull (batch_size={batch_size})"
    )

    batch_news = []
    batch_ids = []

    for i in range(0, len(pending), batch_size):
        batch_pending = pending[i : i + batch_size]

        # Fetch this batch concurrently
        results = await asyncio.gather(
            *(source.query(meta["id"]) for meta in batch_pending),
            return_exceptions=True,
        )

        for meta, result in zip(batch_pending, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to pull {meta['id']}: {result}")
                continue

            content = result.get("response", {}).get("content")

            if not content:
                logger.error(f"No content returned for {meta['id']}")
                continue

            batch_news.append(content)
            batch_ids.append(meta)

        # Yield batch
        if batch_news:
            news_df = pd.DataFrame(batch_news)
            news_df["pull_at"] = config.current_time
            news_df["updated_at"] = config.current_time

            yield news_df.to_dict("records"), batch_ids

            logger.info(f"Yielded {len(batch_news)} articles")
            batch_news = []
            batch_ids = []


async def main(
    api_key: str = config.guardian_api_key,
    requests_per_second: int = 1,
    batch_size: int = 100,
    max_concurrent_batches: int = 3,
    source_type: str | None = None,
):
    """Main ingestion process with concurrent batch processing.

    Args:
        api_key: Guardian API key
        requests_per_second: Rate limiting for API
        batch_size: Records per batch yield/insert
        max_concurrent_batches: Max concurrent batch inserts (prevents memory buildup)
        source_type: 'mock' or 'api'
    """
    logger.info("Starting news ingestion process...")

    # Use config source type if not explicitly provided
    if source_type is None:
        source_type = config.data_source

    logger.info(f"Using data source: {source_type}")

    # Create the data source
    if source_type == "api":
        source = create_source(
            source_type="api",
            api_key=api_key,
            requests_per_second=requests_per_second,
        )
    elif source_type == "mock":
        source = create_source(
            source_type="mock",
            total_articles=config.mock_total_articles,
            batch_size=config.mock_batch_size,
            seed=config.mock_seed,
        )
    else:
        raise ValueError(f"Unknown source type: {source_type}")

    logger.info("Initializing async database connection...")
    db = get_async_mongodb(config.mongo_db_name)
    coll_meta = db[config.mongo_coll_metadata]
    coll_news = db[config.mongo_coll_news]

    logger.info("Preparing MongoDB news collection!")
    await coll_news.create_index(
        [("id", 1), ("webPublicationDate", 1)],
        unique=1,
        name="news_raw_unique",
    )

    pending_tasks: set[asyncio.Task[None]] = set()
    try:
        logger.info("Fetching unprocessed news IDs...")
        news_ids = await get_news_ids(coll_meta)

        if not news_ids:
            logger.error("No news metadata IDs found! Stopping the process...")
            return

        logger.info(
            f"Processing {len(news_ids)} news items (concurrent batch streaming)..."
        )

        batch_number = 0

        async for news_batch, ids_batch in extract_news_stream(
            source, news_ids, batch_size
        ):
            batch_number += 1

            if not news_batch:
                logger.warning("Empty batch received, skipping...")
                continue

            task = asyncio.create_task(
                store_news_batch(
                    coll_news, coll_meta, news_batch, ids_batch, batch_number
                )
            )
            pending_tasks.add(task)

            # Keep max_concurrent_batches tasks running concurrently
            # When we reach the limit, wait for some to complete
            if len(pending_tasks) >= max_concurrent_batches:
                # Wait for at least one task to complete
                done, pending_tasks = await asyncio.wait(
                    pending_tasks, return_when=asyncio.FIRST_COMPLETED
                )

                await asyncio.gather(*done)

        # Wait for all remaining tasks to complete
        if pending_tasks:
            logger.info(
                f"Waiting for {len(pending_tasks)} remaining batch tasks to complete..."
            )
            await asyncio.gather(*pending_tasks)

        logger.info(f"Successfully processed {batch_number} batches of news articles")

    finally:
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
