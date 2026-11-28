"""
Data source abstraction for flexible ingestion.

Supports both real API sources (Guardian) and mock data sources.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any
from loguru import logger

from articles_suzaki.ingestion.data_mock.mock_data_generator import MockArticleGenerator


class DataSource(ABC):
    """Abstract base class for data sources."""

    @abstractmethod
    async def search(
        self,
        query: str | None = None,
        page: int = 1,
        page_size: int = 10,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict:
        """Search for articles."""
        pass

    @abstractmethod
    async def query(self, news_id: str) -> dict:
        """Query a specific article by ID."""
        pass

    @abstractmethod
    async def close(self):
        """Clean up resources."""
        pass


class GuardianAPISource(DataSource):
    """Real Guardian API source."""

    def __init__(self, client):
        """Initialize with a GuardianClient instance."""
        self.client = client

    async def search(
        self,
        query: str | None = None,
        page: int = 1,
        page_size: int = 10,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict:
        """Delegate to Guardian client."""
        return await self.client.search(
            query=query,
            page=page,
            page_size=page_size,
            from_date=from_date,
            to_date=to_date,
        )

    async def query(self, news_id: str) -> dict:
        """Delegate to Guardian client."""
        return await self.client.query(news_id)

    async def close(self):
        """Close the Guardian client."""
        await self.client.close()


class MockDataSource(DataSource):
    """Mock data source for testing and load testing."""

    def __init__(
        self,
        total_articles: int = 1_000_000,
        batch_size: int = 10,
        seed: int | None = None,
    ):
        """
        Initialize mock data source.

        Args:
            total_articles: Total number of articles to generate.
            batch_size: Number of articles per search result page.
            seed: Random seed for reproducibility.
        """
        self.total_articles = total_articles
        self.batch_size = batch_size
        self.generator = MockArticleGenerator(seed=seed)
        self._articles_cache = {}
        logger.info(
            f"Initialized MockDataSource: {total_articles} articles, "
            f"batch_size={batch_size}, seed={seed}"
        )

    async def search(
        self,
        query: str | None = None,
        page: int = 1,
        page_size: int = 10,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict:
        """
        Return mock search results.

        Always returns the same number of results regardless of query/filters
        for consistency. In production, filters would be applied.
        """
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, self.total_articles)

        if end_idx <= start_idx:
            return {
                "response": {
                    "results": [],
                    "pages": page,
                    "currentPage": page,
                    "total": self.total_articles,
                    "pageSize": page_size,
                    "startIndex": start_idx,
                }
            }

        # Generate articles for this page
        articles = []
        for i in range(start_idx, end_idx):
            # Generate deterministic IDs based on index
            article_id = f"mock/{i:08d}"
            article = self.generator.generate_article(article_id=article_id)
            articles.append({"id": article["id"]})
            self._articles_cache[article["id"]] = article

        total_pages = (self.total_articles + page_size - 1) // page_size

        return {
            "response": {
                "results": articles,
                "pages": total_pages,
                "currentPage": page,
                "total": self.total_articles,
                "pageSize": page_size,
                "startIndex": start_idx,
            }
        }

    async def query(self, news_id: str) -> dict:
        """Return a full article by ID."""
        # Check cache first
        if news_id in self._articles_cache:
            article = self._articles_cache[news_id]
        else:
            # Generate on-the-fly if not in cache
            article = self.generator.generate_article(article_id=news_id)
            self._articles_cache[news_id] = article

        return {
            "response": {
                "content": article,
            }
        }

    async def close(self):
        """Clean up mock source."""
        self._articles_cache.clear()
        logger.info("MockDataSource closed")


def create_source(
    source_type: str = "api",
    **kwargs,
) -> DataSource:
    """
    Factory function to create a data source.

    Args:
        source_type: Type of source ("api" or "mock").
        **kwargs: Source-specific arguments.
                 For "api": api_key, requests_per_second
                 For "mock": total_articles, batch_size, seed

    Returns:
        A DataSource instance.
    """
    if source_type == "api":
        from articles_suzaki.ingestion.client import GuardianClient

        api_key = kwargs.get("api_key", "test")
        requests_per_second = kwargs.get("requests_per_second", 1)
        client = GuardianClient(
            api_key=api_key,
            requests_per_second=requests_per_second,
        )
        return GuardianAPISource(client)

    elif source_type == "mock":
        total_articles = kwargs.get("total_articles", 1_000_000)
        batch_size = kwargs.get("batch_size", 10)
        seed = kwargs.get("seed", None)
        return MockDataSource(
            total_articles=total_articles,
            batch_size=batch_size,
            seed=seed,
        )

    else:
        raise ValueError(
            f"Unknown source type: {source_type}. "
            f"Choose 'api' or 'mock'."
        )
