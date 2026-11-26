import asyncio
import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import httpx
from loguru import logger

from articles_suzaki.config import config

METADATA_PATH = Path(__file__).with_name("metadata.json")
NEWS_PATH = Path(__file__).with_name("news.json")


class GuardianClient:
    BASE_URL = "https://content.guardianapis.com/"

    def __init__(
        self,
        api_key: str = "test",
        daily_limit: int = 500,
        requests_per_second: float = 1,
    ):
        self.api_key = api_key
        self.daily_limit = daily_limit
        self.min_interval = 1 / requests_per_second

        self._request_count = 0
        self._counter_date = date.today()
        self._last_request_time = 0.0
        self._rate_lock = asyncio.Lock()

        self.client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))

    async def close(self):
        await self.client.aclose()

    def _check_daily_limit(self):
        today = date.today()

        if today != self._counter_date:
            self._counter_date = today
            self._request_count = 0

        if self._request_count >= self.daily_limit:
            raise RuntimeError("Guardian API daily request limit exceeded")

    async def _rate_limit(self):
        # The lock serializes concurrent gather() calls so they don't all
        # wake up and fire at the same time.
        async with self._rate_lock:
            elapsed = time.monotonic() - self._last_request_time

            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)

            self._last_request_time = time.monotonic()

    async def search(
        self,
        query: str | None = None,
        page: int = 1,
        page_size: int = 10,
    ) -> dict:

        self._check_daily_limit()
        await self._rate_limit()

        params = {
            "api-key": self.api_key,
            "page": page,
            "page-size": page_size,
            "order-by": "newest",
            "show-fields": "headline,trailText,byline,thumbnail",
        }

        if query:
            params["q"] = query

        response = await self.client.get(
            urljoin(self.BASE_URL, "search"),
            params=params,
        )

        response.raise_for_status()

        self._request_count += 1

        return response.json()

    async def query(
        self,
        news_id: str,
        params: dict,
    ):
        self._check_daily_limit()
        await self._rate_limit()

        response = await self.client.get(urljoin(self.BASE_URL, news_id), params=params)

        response.raise_for_status()

        self._request_count += 1

        return response.json()


def read_json(path: Path, default):
    if not path.exists() or path.stat().st_size == 0:
        return default

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    """Write JSON atomically so a crash can't leave a half-written file."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    os.replace(tmp_path, path)


async def extract_metadata(
    guardian: GuardianClient, current_time: datetime, page_size: int
):
    result = await guardian.search(
        query="nvidia",
        page_size=page_size,
    )

    articles = result["response"]["results"]
    now = current_time.isoformat()

    for data in articles:
        data["created_at"] = now
        data["updated_at"] = now
        data["is_pull"] = False

    return articles


def read_metadata(metadata_path: Path | None = None) -> list[dict]:
    data = read_json(metadata_path or METADATA_PATH, default=[])

    # metadata.json may contain the raw /search response, like the file
    # currently does, or just the list of results.
    if isinstance(data, dict):
        data = data.get("response", {}).get("results", [])

    for article in data:
        article.setdefault("is_pull", False)

    return data


def read_news(news_path: Path | None = None) -> list[dict]:
    return read_json(news_path or NEWS_PATH, default=[])


async def extract_each_news(
    guardian: GuardianClient,
    metadata_list: list[dict],
) -> list[dict]:
    params = {
        "api-key": guardian.api_key,
        "order-by": "newest",
        "show-fields": "all",
        "show-tags": "keyword,contributor",
        "type": "article",
    }

    pending = [
        news_metadata
        for news_metadata in metadata_list
        if not news_metadata.get("is_pull", False)
    ]

    logger.info(f"found {len(pending)} metadata entries to pull")

    results = await asyncio.gather(
        *(guardian.query(meta["id"], params) for meta in pending),
        return_exceptions=True,
    )

    news_list = []

    for meta, result in zip(pending, results):
        if isinstance(result, Exception):
            logger.error(f"failed to pull {meta['id']}: {result}")
            continue

        content = result.get("response", {}).get("content")

        if not content:
            logger.error(f"no content returned for {meta['id']}")
            continue

        news_list.append(content)

    return news_list


def store_data(news_list: list[dict], metadata_list: list[dict]) -> None:
    """Save the new articles and mark their metadata as pulled."""
    # Merge by Guardian id so a later batch does not overwrite earlier news.
    news_by_id = {item["id"]: item for item in read_news()}
    news_by_id.update({item["id"]: item for item in news_list})
    write_json(NEWS_PATH, list(news_by_id.values()))

    # Only flip is_pull after news.json has been written successfully.
    pulled_ids = {item["id"] for item in news_list}
    now = datetime.now(timezone.utc).isoformat()

    for meta in metadata_list:
        if meta["id"] in pulled_ids:
            meta["is_pull"] = True
            meta["updated_at"] = now

    write_json(METADATA_PATH, metadata_list)


async def main(api_key: str = config.guardian_api_key, requests_per_second: int = 1):
    guardian = GuardianClient(
        api_key=api_key,
        requests_per_second=requests_per_second,
    )

    try:
        logger.info("checking metadata")
        metadata_list = read_metadata()

        if not metadata_list:
            logger.info("no metadata found, fetching it")
            metadata_list = await extract_metadata(
                guardian,
                datetime.now(timezone.utc),
                page_size=5,
            )

            if not metadata_list:
                logger.warning("no metadata is found!")
                return

            write_json(METADATA_PATH, metadata_list)

        news_list = await extract_each_news(guardian, metadata_list)

        if not news_list:
            logger.info("no new news to pull")
            return

        store_data(news_list, metadata_list)
        logger.info(f"saved {len(news_list)} news articles")

    finally:
        await guardian.close()


if __name__ == "__main__":
    asyncio.run(main())
