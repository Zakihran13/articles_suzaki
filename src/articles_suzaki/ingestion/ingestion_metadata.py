import asyncio
from datetime import datetime, timedelta, date
from loguru import logger
import pandas as pd
from typing import List, Any, AsyncGenerator
from motor.motor_asyncio import AsyncIOMotorCollection


from articles_suzaki.configuration.config import config
from articles_suzaki.ingestion.client import GuardianClient
from articles_suzaki.ingestion.data_mock.mock_data_generator import MockArticleGenerator
from articles_suzaki.logging.flows import logged_async_flow
from articles_suzaki.warehouse.client import get_async_mongodb
from articles_suzaki.warehouse.statements import upsert_data


def build_metadata_updates(data: List[Any], now: datetime):
    """Build update statements for metadata records."""
    return [
        {
            "filter": {"id": n["id"]},
            "update": {
                "$set": {
                    "webTitle": n["webTitle"],
                    "webUrl": n["webUrl"],
                    "sectionId": n["sectionId"],
                    "webPublicationDate": n["webPublicationDate"],
                    "type": n.get("type"),
                    "sectionName": n.get("sectionName"),
                    "created_at": n.get("created_at", now),
                    "updated_at": now,
                    "deleted_at": None,  # For future recheck of deleted articles
                    **({"ingestion_source": n["ingestion_source"]} if "ingestion_source" in n else {}),
                },
                "$setOnInsert": {
                    "is_pull": n.get("is_pull", False),
                }
            },
        }
        for n in data
    ]


async def store_data_batch(
    coll: AsyncIOMotorCollection,
    data: List[Any],
    batch_number: int | None = None,
):
    """Store a batch of data with optional logging."""
    if not data:
        return

    batch_info = f" (batch {batch_number})" if batch_number else ""
    logger.info(f"storing {len(data)} records{batch_info}")

    data_update = build_metadata_updates(
        data,
        config.current_time,
    )

    await upsert_data(coll, data_update)


async def extract_metadata_from_api_stream(
    guardian: GuardianClient,
    q: str | None = None,
    current_time: datetime = config.current_time,
    page_size: int = 200,
    cur_page: int = 1,
    max_roll: int = None,
    from_date: date | None = None,
    to_date: date | None = None,
    batch_size: int = 200,
) -> AsyncGenerator[List[dict], None]:
    """Stream metadata from Guardian API in batches (async generator).

    This yields batches of data as they're fetched, avoiding memory buildup
    when processing large amounts of historical data.

    Args:
        guardian: GuardianClient instance
        q: Query string
        current_time: Current datetime for timestamps
        page_size: Page size for API requests
        cur_page: Starting page for API requests
        max_roll: Max pages to fetch
        from_date: Start of date range (inclusive)
        to_date: End of date range (inclusive)
        batch_size: Number of records per batch yield

    Yields:
        Lists of metadata records (each list <= batch_size)
    """
    logger.info(
        f"Starting metadata extraction from Guardian API (batch_size={batch_size})"
    )

    batch = []
    total_fetched = 0

    res = await guardian.search(
        query=q,
        page=cur_page,
        page_size=page_size,
        from_date=from_date,
        to_date=to_date,
    )

    articles = res["response"]["results"]
    total_pages = res["response"]["pages"]

    for article in articles:
        batch.append(article)
        total_fetched += 1

        if len(batch) >= batch_size:
            # Yield batch when it reaches batch_size
            df = pd.DataFrame(batch)
            df["created_at"] = current_time
            df["updated_at"] = current_time
            df["is_pull"] = False

            yield df.to_dict("records")
            logger.info(f"Fetched {total_fetched} records from API")
            batch = []

    # Fetch remaining pages
    for page in range(cur_page + 1, total_pages + 1):
        if max_roll is not None and page > max_roll:
            break

        res = await guardian.search(
            query=q,
            page=page,
            page_size=page_size,
            from_date=from_date,
            to_date=to_date,
        )

        articles = res["response"]["results"]

        for article in articles:
            batch.append(article)
            total_fetched += 1

            if len(batch) >= batch_size:
                # Yield batch when it reaches batch_size
                df = pd.DataFrame(batch)
                df["created_at"] = current_time
                df["updated_at"] = current_time
                df["is_pull"] = False

                yield df.to_dict("records")
                logger.info(f"Fetched {total_fetched} records from API")
                batch = []

    # Yield remaining records
    if batch:
        df = pd.DataFrame(batch)
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["is_pull"] = False

        yield df.to_dict("records")
        logger.info(f"Fetched {total_fetched} records total from API")


async def extract_metadata_from_mock_stream(
    total_records: int = 1000,
    batch_size: int = 1000,
    seed: int | None = None,
    current_time: datetime = config.current_time,
    from_date: date | None = None,
    to_date: date | None = None,
) -> AsyncGenerator[List[dict], None]:
    """Stream metadata from mock generator in batches (async generator).

    This yields batches of data immediately after generation, avoiding
    memory buildup when processing large amounts of data (e.g., 1M records).

    Args:
        total_records: Total number of metadata records to generate
        batch_size: Batch size for generation and yielding
        seed: Random seed for deterministic generation
        current_time: Current datetime for timestamps
        from_date: Start of date range for generated articles (inclusive)
        to_date: End of date range for generated articles (inclusive)

    Yields:
        Lists of metadata records (each list <= batch_size)
    """
    logger.info(
        f"Generating {total_records} mock metadata records (batch_size={batch_size})"
    )
    if from_date and to_date:
        logger.info(f"Date range: {from_date} to {to_date}")

    generator = MockArticleGenerator(seed=seed, from_date=from_date, to_date=to_date)

    for batch_start in range(0, total_records, batch_size):
        batch_count = min(batch_size, total_records - batch_start)
        batch = generator.generate_metadata_batch(batch_count)

        # Add timestamps
        df = pd.DataFrame(batch)
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["is_pull"] = False

        yield df.to_dict("records")

        if (batch_start + batch_count) % (batch_size * 10) == 0:
            logger.info(
                f"Generated {batch_start + batch_count} / {total_records} metadata records"
            )

    logger.info(f"Finished generating {total_records} mock metadata records")


async def extract_metadata_stream(
    guardian: GuardianClient | None = None,
    q: str | None = None,
    current_time: datetime = config.current_time,
    page_size: int = 200,
    cur_page: int = 1,
    max_roll: int = None,
    from_date: date | None = None,
    to_date: date | None = None,
    batch_size: int = 1000,
    source_type: str | None = None,
) -> AsyncGenerator[List[dict], None]:
    """Stream metadata from either API or mock based on config (async generator).

    Args:
        guardian: GuardianClient instance (required for API source)
        q: Query string
        current_time: Current datetime for timestamps
        page_size: Page size for API requests
        cur_page: Starting page for API requests
        max_roll: Max pages to fetch (for API)
        from_date: Start of date range (inclusive)
        to_date: End of date range (inclusive)
        batch_size: Records per batch yield
        source_type: 'mock' or 'api' (uses config if not specified)

    Yields:
        Lists of metadata records (each list <= batch_size)
    """
    if source_type is None:
        source_type = config.data_source

    if source_type == "mock":
        async for batch in extract_metadata_from_mock_stream(
            total_records=config.mock_total_articles,
            batch_size=batch_size or config.mock_batch_size,
            seed=config.mock_seed,
            current_time=current_time,
            from_date=from_date,
            to_date=to_date,
        ):
            yield batch
    else:
        async for batch in extract_metadata_from_api_stream(
            guardian=guardian,
            q=q,
            current_time=current_time,
            page_size=page_size,
            cur_page=cur_page,
            max_roll=max_roll,
            from_date=from_date,
            to_date=to_date,
            batch_size=batch_size or page_size,
        ):
            yield batch


@logged_async_flow("ingestion_metadata")
async def main(
    api_key: str = config.guardian_api_key,
    requests_per_second: int = 1,
    page_size: int = 200,
    batch_size: int = 1000,
    max_concurrent_batches: int = 3,
    from_date: date = config.prior_time,
    to_date: date = config.current_time,
    source_type: str | None = None,
    max_roll: int | None = 2,
    tag_source: bool = False,
):
    """Main ingestion process with concurrent batch processing.

    Args:
        api_key: Guardian API key
        requests_per_second: Rate limiting for API
        page_size: API page size
        batch_size: Records per batch yield/insert
        max_concurrent_batches: Max concurrent batch inserts (prevents memory buildup)
        from_date: Start date for filtering
        to_date: End date for filtering
        source_type: 'mock' or 'api'
    """
    logger.info("Starting the metadata ingestion process...")

    if source_type is None:
        source_type = config.data_source

    logger.info(f"Using data source: {source_type}")

    # Guardian client only needed for API source
    guardian = None
    if source_type == "api":
        guardian = GuardianClient(
            api_key=api_key,
            requests_per_second=requests_per_second,
        )

    logger.info("Initializing async database connection...")
    db = get_async_mongodb(config.mongo_db_name)
    collection = db[config.mongo_coll_metadata]

    logger.info("Preparing MongoDB collection!")
    await collection.create_index(
        [("id", 1), ("webPublicationDate", 1)],
        unique=1,
        name="news_meta_unique",
    )

    pending_tasks: set[asyncio.Task[None]] = set()
    try:
        logger.info(
            "Processing metadata news retrieval (concurrent batch streaming)..."
        )

        batch_number = 0

        async for batch in extract_metadata_stream(
            guardian=guardian,
            q=None,
            current_time=config.current_time,
            page_size=page_size,
            cur_page=1,
            max_roll=max_roll,
            from_date=from_date.date() if isinstance(from_date, datetime) else from_date,
            to_date=to_date.date() if isinstance(to_date, datetime) else to_date,
            batch_size=batch_size,
            source_type=source_type,
        ):
            batch_number += 1
            if tag_source:
                for record in batch:
                    record["ingestion_source"] = source_type

            if not batch:
                logger.warning("Empty batch received, skipping...")
                continue

            # Create a task for storing this batch (don't await, just create task)
            task = asyncio.create_task(
                store_data_batch(collection, batch, batch_number)
            )
            pending_tasks.add(task)

            # Keep max_concurrent_batches tasks running concurrently
            # When we reach the limit, wait for one to complete
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

        logger.info(f"Successfully processed {batch_number} batches of metadata")

    finally:
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        if guardian:
            await guardian.close()


if __name__ == "__main__":
    asyncio.run(
        main(
            from_date=datetime(2016, 1, 1),
            to_date=datetime(2026, 11, 27),
        )
    )
