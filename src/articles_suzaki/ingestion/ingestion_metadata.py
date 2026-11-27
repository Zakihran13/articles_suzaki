import asyncio
from datetime import datetime, timedelta, date
from loguru import logger
import pandas as pd
from typing import List, Any
from motor.motor_asyncio import AsyncIOMotorCollection


from articles_suzaki.configuration.config import config
from articles_suzaki.ingestion.client import GuardianClient
from articles_suzaki.warehouse.client import get_async_mongodb
from articles_suzaki.warehouse.statements import upsert_data


def build_metadata_updates(
    metadata_list: List[dict],
    now: datetime,
) -> List[dict]:

    return [
        {
            "filter": {"id": metadata["id"]},
            "update": {
                "$set": {
                    "webTitle": metadata["webTitle"],
                    "webUrl": metadata["webUrl"],
                    "sectionId": metadata["sectionId"],
                    "webPublicationDate": metadata["webPublicationDate"],
                    "fields": metadata.get("fields"),
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "is_pull": False,
                },
            },
        }
        for metadata in metadata_list
    ]


async def store_data(coll: AsyncIOMotorCollection, data: List[Any]):
    logger.info("storing data!")

    data_update = build_metadata_updates(
        data,
        config.current_time,
    )

    await upsert_data(coll, data_update)


async def extract_metadata(
    guardian: GuardianClient,
    q: str | None = None,
    current_time: datetime = config.current_time,
    page_size: int = 200,
    cur_page: int = 1,
    max_roll: int = None,
    from_date: date | None = None,
    to_date: date | None = None,
):
    articles_list = []

    res = await guardian.search(
        query=q,
        page=cur_page,
        page_size=page_size,
        from_date=from_date,
        to_date=to_date,
    )

    articles = res["response"]["results"]

    if articles:
        articles_list.extend(articles)

    total_pages = res["response"]["pages"]

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

        articles_list.extend(res["response"]["results"])

    articles_list = pd.DataFrame(articles_list)
    articles_list["created_at"] = current_time
    articles_list["updated_at"] = current_time
    articles_list["is_pull"] = False

    return articles_list.to_dict("records")


async def main(
    api_key: str = config.guardian_api_key,
    requests_per_second: int = 1,
    page_size: int = 200,
    from_date: date = config.prior_time,
    to_date: date = config.current_time,
):
    logger.info("Starting the process...")
    guardian = GuardianClient(
        api_key=api_key,
        requests_per_second=requests_per_second,
    )

    logger.info("Initializing async database connection...")
    db = get_async_mongodb("articles_raw")
    collection = db["news_metadata"]

    logger.info("preparing mongodb collection!")
    await collection.create_index(
        [("id", 1), ("webPublicationDate", 1)],
        unique=1,
        name="news_meta_unique",
    )

    try:
        # logger.info("no metadata found, fetching it")
        metadata_list = await extract_metadata(
            guardian,
            q=None,
            current_time=config.current_time,
            page_size=page_size,
            from_date=from_date.date(),
            to_date=to_date.date(),
            max_roll=2,  # testing only
        )

        if not metadata_list:
            logger.warning("no metadata is found!")
            return

        await store_data(collection, metadata_list)

    finally:
        await guardian.close()


if __name__ == "__main__":
    asyncio.run(main())
