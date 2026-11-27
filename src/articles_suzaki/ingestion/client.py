import asyncio
import time
from datetime import date
from urllib.parse import urljoin
import httpx

from articles_suzaki.configuration.config import config


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
        from_date: date | None = None,
        to_date: date | None = None
    ) -> dict:

        self._check_daily_limit()
        await self._rate_limit()

        params = {
            "api-key": self.api_key,
            "page": page,
            "page-size": page_size,
            "order-by": "newest",
            "show-fields": "all",
            "show-tags": "all",
            "type": "article",
        }

        if query:
            params["q"] = query
        if from_date:
            params["from-date"] =  from_date
        if to_date:
            params["to-date"] = to_date

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

