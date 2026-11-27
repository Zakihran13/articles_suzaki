from motor.motor_asyncio import AsyncIOMotorCollection
import pandas as pd
from loguru import logger
import asyncio
from pymongo import UpdateOne
from typing import List

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

    if operation_tasks:
        try:
            results = await asyncio.gather(*operation_tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Bulk write failed: {result}")
                else:
                    logger.info(
                        f"matched={result.matched_count}, "
                        f"modified={result.modified_count}, "
                        f"upserted={result.upserted_count}"
                    )
        except Exception as e:
            logger.error(f"An error occurred during bulk write: {e}")
