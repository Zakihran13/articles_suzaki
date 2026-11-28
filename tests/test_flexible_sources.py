"""
Tests for the flexible data source architecture.
"""

import pytest
from articles_suzaki.ingestion.data_mock.mock_data_generator import MockArticleGenerator
from articles_suzaki.ingestion.data_mock.source import create_source, MockDataSource, GuardianAPISource


class TestMockArticleGenerator:
    """Test mock article generation."""

    def test_generate_single_article(self):
        """Test generating a single article."""
        gen = MockArticleGenerator()
        article = gen.generate_article()

        assert article["id"]
        assert article["webTitle"]
        assert article["webUrl"]
        assert article["sectionId"]
        assert article["fields"]["bodyText"]

    def test_deterministic_with_seed(self):
        """Test that same seed produces same articles."""
        gen1 = MockArticleGenerator(seed=42)
        article1 = gen1.generate_article(article_id="test/0001")

        gen2 = MockArticleGenerator(seed=42)
        article2 = gen2.generate_article(article_id="test/0001")

        # Same ID, same seed should produce same title
        assert article1["webTitle"] == article2["webTitle"]

    def test_generate_batch(self):
        """Test batch generation."""
        gen = MockArticleGenerator()
        batch = gen.generate_batch(count=5)

        assert len(batch) == 5
        assert all("id" in a for a in batch)
        assert all("webTitle" in a for a in batch)

    def test_article_id_format(self):
        """Test that generated article IDs are unique."""
        gen = MockArticleGenerator()
        ids = [gen.generate_article_id() for _ in range(10)]

        # All should be unique
        assert len(ids) == len(set(ids))

    def test_custom_article_id(self):
        """Test custom article ID."""
        gen = MockArticleGenerator()
        article = gen.generate_article(article_id="custom/id/123")

        assert article["id"] == "custom/id/123"


class TestDataSourceFactory:
    """Test data source creation."""

    def test_create_mock_source(self):
        """Test creating mock data source."""
        source = create_source(
            source_type="mock",
            total_articles=100,
            batch_size=10,
            seed=42,
        )

        assert isinstance(source, MockDataSource)
        assert source.total_articles == 100
        assert source.batch_size == 10

    def test_create_api_source(self):
        """Test creating API source."""
        source = create_source(
            source_type="api",
            api_key="test_key",
            requests_per_second=1,
        )

        assert isinstance(source, GuardianAPISource)

    def test_invalid_source_type(self):
        """Test that invalid source type raises error."""
        with pytest.raises(ValueError, match="Unknown source type"):
            create_source(source_type="invalid")


@pytest.mark.asyncio
class TestMockDataSource:
    """Test mock data source async operations."""

    async def test_search_returns_results(self):
        """Test search returns paginated results."""
        source = MockDataSource(total_articles=100, batch_size=10)

        result = await source.search(page=1, page_size=10)

        assert "response" in result
        assert "results" in result["response"]
        assert len(result["response"]["results"]) == 10
        assert result["response"]["total"] == 100

    async def test_search_pagination(self):
        """Test pagination works correctly."""
        source = MockDataSource(total_articles=50, batch_size=10)

        # First page
        page1 = await source.search(page=1, page_size=10)
        assert len(page1["response"]["results"]) == 10

        # Second page
        page2 = await source.search(page=2, page_size=10)
        assert len(page2["response"]["results"]) == 10

        # Last page (partial)
        page5 = await source.search(page=5, page_size=10)
        assert len(page5["response"]["results"]) == 10

    async def test_search_beyond_total(self):
        """Test search beyond total returns empty."""
        source = MockDataSource(total_articles=10, batch_size=10)

        result = await source.search(page=5, page_size=10)
        assert len(result["response"]["results"]) == 0

    async def test_query_returns_full_article(self):
        """Test query returns full article."""
        source = MockDataSource(total_articles=100)

        result = await source.query("mock/00000000")

        assert "response" in result
        assert "content" in result["response"]
        article = result["response"]["content"]
        assert article["id"] == "mock/00000000"
        assert article["webTitle"]
        assert article["fields"]["bodyText"]

    async def test_close(self):
        """Test closing the source."""
        source = MockDataSource(total_articles=100)

        # Should not raise
        await source.close()

        # Cache should be cleared
        assert len(source._articles_cache) == 0


class TestArticleSchema:
    """Test article schema completeness."""

    def test_article_has_required_fields(self):
        """Test that generated articles have all required fields."""
        gen = MockArticleGenerator()
        article = gen.generate_article()

        required_fields = [
            "id",
            "type",
            "sectionId",
            "sectionName",
            "webPublicationDate",
            "webTitle",
            "webUrl",
            "apiUrl",
            "fields",
        ]

        for field in required_fields:
            assert field in article, f"Missing field: {field}"

    def test_article_fields_completeness(self):
        """Test article fields object completeness."""
        gen = MockArticleGenerator()
        article = gen.generate_article()

        fields = article["fields"]
        expected_fields = [
            "bodyText",
            "wordcount",
            "firstPublicationDate",
            "lastModified",
        ]

        for field in expected_fields:
            assert field in fields, f"Missing field in 'fields': {field}"

    def test_article_tags_structure(self):
        """Test article tags structure."""
        gen = MockArticleGenerator()
        article = gen.generate_article()

        tags = article["tags"]
        assert isinstance(tags, list)
        assert len(tags) > 0

        for tag in tags:
            assert "id" in tag
            assert "type" in tag
            assert "webTitle" in tag


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestDateRangeFiltering:
    """Test date range filtering in mock data generation."""

    def test_metadata_respects_date_range(self):
        """Test that metadata respects date range."""
        from datetime import date, datetime
        
        from_date = date(2024, 1, 1)
        to_date = date(2024, 12, 31)
        
        gen = MockArticleGenerator(seed=42, from_date=from_date, to_date=to_date)
        articles = gen.generate_metadata_batch(50)
        
        # Verify all articles are within range
        for article in articles:
            pub_date = datetime.fromisoformat(article['webPublicationDate']).date()
            assert from_date <= pub_date <= to_date, \
                f"Article {article['id']} dated {pub_date} outside range {from_date} to {to_date}"

    def test_article_respects_date_range(self):
        """Test that full articles respect date range."""
        from datetime import date, datetime
        
        from_date = date(2024, 6, 1)
        to_date = date(2024, 6, 30)
        
        gen = MockArticleGenerator(seed=123, from_date=from_date, to_date=to_date)
        articles = gen.generate_article_batch(50)
        
        # Verify all articles are within range
        for article in articles:
            pub_date = datetime.fromisoformat(article['webPublicationDate']).date()
            assert from_date <= pub_date <= to_date, \
                f"Article dated {pub_date} outside range {from_date} to {to_date}"

    def test_date_range_with_seed_is_deterministic(self):
        """Test that same seed and date range produces same results."""
        from datetime import date
        
        from_date = date(2024, 3, 1)
        to_date = date(2024, 3, 31)
        
        # First run
        gen1 = MockArticleGenerator(seed=999, from_date=from_date, to_date=to_date)
        articles1 = gen1.generate_article_batch(10)
        
        # Second run with same seed and date range
        gen2 = MockArticleGenerator(seed=999, from_date=from_date, to_date=to_date)
        articles2 = gen2.generate_article_batch(10)
        
        # Should produce identical results
        for a1, a2 in zip(articles1, articles2):
            assert a1['webTitle'] == a2['webTitle']
            assert a1['webPublicationDate'] == a2['webPublicationDate']

    def test_single_day_range(self):
        """Test generating data for a single day."""
        from datetime import date, datetime
        
        target_date = date(2024, 7, 15)
        
        gen = MockArticleGenerator(seed=42, from_date=target_date, to_date=target_date)
        articles = gen.generate_metadata_batch(20)
        
        # All articles should be on the target date
        for article in articles:
            pub_date = datetime.fromisoformat(article['webPublicationDate']).date()
            assert pub_date == target_date

    def test_cross_year_date_range(self):
        """Test date range spanning multiple years."""
        from datetime import date, datetime
        
        from_date = date(2023, 12, 1)
        to_date = date(2024, 1, 31)
        
        gen = MockArticleGenerator(seed=42, from_date=from_date, to_date=to_date)
        articles = gen.generate_article_batch(100)
        
        # Verify all in range
        for article in articles:
            pub_date = datetime.fromisoformat(article['webPublicationDate']).date()
            assert from_date <= pub_date <= to_date

    def test_no_date_range_uses_last_30_days(self):
        """Test that without date range, uses last 30 days."""
        from datetime import date, datetime, timedelta
        
        gen = MockArticleGenerator(seed=42)
        articles = gen.generate_metadata_batch(10)
        
        now = datetime.now().date()
        thirty_days_ago = now - timedelta(days=30)
        
        # Articles should be roughly in the last 30 days (some randomness)
        for article in articles:
            pub_date = datetime.fromisoformat(article['webPublicationDate']).date()
            # Allow some flexibility
            assert thirty_days_ago <= pub_date <= now

    def test_datetime_input_accepted(self):
        """Test that datetime objects are accepted for date range."""
        from datetime import datetime
        
        from_dt = datetime(2024, 1, 1, 0, 0, 0)
        to_dt = datetime(2024, 1, 31, 23, 59, 59)
        
        gen = MockArticleGenerator(seed=42, from_date=from_dt, to_date=to_dt)
        articles = gen.generate_metadata_batch(10)
        
        # Should work without errors
        assert len(articles) == 10
        for article in articles:
            assert article['webPublicationDate']
