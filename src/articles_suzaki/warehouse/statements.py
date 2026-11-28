from motor.motor_asyncio import AsyncIOMotorCollection
import pandas as pd
from loguru import logger
import asyncio
from pymongo import UpdateOne
from typing import List
from datetime import date, timedelta

from articles_suzaki.configuration.helper import split_batch


async def process_chunk(
    coll: AsyncIOMotorCollection,
    chunk: list[dict],
    semaphore: asyncio.Semaphore,
):

    batch_operations = [
        UpdateOne(
            record["filter"],
            record["update"],
            upsert=True,
        )
        for record in chunk
    ]

    async with semaphore:
        return await coll.bulk_write(batch_operations, ordered=False)


async def upsert_data(
    coll: AsyncIOMotorCollection,
    data: List[dict],
    batch_size: int = 2_000,
    max_concurrency: int = 4,
):
    if not data:
        logger.info("Data is empty. Nothing to upsert.")
        return

    logger.info(f"inserting data: **{len(data)}**")
    batch_data = split_batch(data, batch_size)
    semaphore = asyncio.Semaphore(max_concurrency)
    operation_tasks = [process_chunk(coll, chunk, semaphore) for chunk in batch_data]

    results = await asyncio.gather(*operation_tasks, return_exceptions=True)
    failures = []
    for result in results:
        if isinstance(result, BaseException):
            logger.error(f"Bulk write failed: {result}")
            failures.append(result)
        else:
            logger.info(
                f"matched={result.matched_count}, "
                f"modified={result.modified_count}, "
                f"upserted={result.upserted_count}"
            )
    if failures:
        raise failures[0]


async def get_news_ids(
    coll: AsyncIOMotorCollection,
    from_date: date | None = None,
    to_date: date | None = None,
    source_type: str | None = None,
):
    query = {"is_pull": False}
    if source_type is not None:
        query["ingestion_source"] = source_type
    if from_date is not None and to_date is not None:
        query["webPublicationDate"] = {
            "$gte": f"{from_date.isoformat()}T00:00:00",
            "$lt": f"{(to_date + timedelta(days=1)).isoformat()}T00:00:00",
        }
    return await (
        coll.find(query, {"id": 1, "webPublicationDate": 1})
        .sort("webPublicationDate", -1)
        .to_list(length=None)
    )
