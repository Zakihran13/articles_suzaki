import random
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Generator
from faker import Faker
from loguru import logger

fake = Faker()


class MockArticleGenerator:
    """Generate realistic mock article data for testing at scale."""

    SECTIONS = ["technology", "business", "science", "politics", "culture"]
    TYPES = ["article", "feature", "analysis"]
    PUBLICATION_HOSTS = ["theguardian.com", "guardian.co.uk"]

    def __init__(
        self,
        seed: int | None = None,
        from_date: date | datetime | None = None,
        to_date: date | datetime | None = None,
    ):
        """Initialize the generator with optional seed for reproducibility and date range.
        
        Args:
            seed: Random seed for deterministic generation
            from_date: Start of date range for generated articles (inclusive)
            to_date: End of date range for generated articles (inclusive)
        """
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
        
        self.from_date = from_date
        self.to_date = to_date
        
        # Convert dates to datetime if needed
        if isinstance(self.from_date, date) and not isinstance(self.from_date, datetime):
            self.from_date = datetime.combine(self.from_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        elif isinstance(self.from_date, datetime) and self.from_date.tzinfo is None:
            self.from_date = self.from_date.replace(tzinfo=timezone.utc)
        
        if isinstance(self.to_date, date) and not isinstance(self.to_date, datetime):
            self.to_date = datetime.combine(self.to_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        elif isinstance(self.to_date, datetime) and self.to_date.tzinfo is None:
            self.to_date = self.to_date.replace(tzinfo=timezone.utc)
    
    def _get_random_date_in_range(self) -> datetime:
        """Get a random date within the configured date range, or last 30 days if no range."""
        if self.from_date and self.to_date:
            # Random date between from_date and to_date
            time_delta = self.to_date - self.from_date
            random_seconds = random.randint(0, int(time_delta.total_seconds()))
            return self.from_date + timedelta(seconds=random_seconds)
        else:
            # Default: last 30 days from now
            base_date = datetime.now(timezone.utc)
            days_ago = random.randint(0, 30)
            return base_date - timedelta(days=days_ago)

    def generate_article_id(self) -> str:
        """Generate a unique article ID similar to Guardian's format."""
        return f"{datetime.now().strftime('%Y/%m/%d')}/{uuid.uuid4().hex[:8]}"

    def generate_metadata(
        self,
        article_id: str | None = None,
        base_date: datetime | None = None,
    ) -> dict:
        """Generate a single mock metadata record (search result).
        
        This represents what comes back from Guardian search API.
        Used by ingestion_metadata.py stage.
        
        Args:
            article_id: Custom article ID (if None, generates one)
            base_date: Ignored if date range is set; uses date range instead
        """
        # Use date range if configured, otherwise use base_date or now
        if self.from_date and self.to_date:
            pub_date = self._get_random_date_in_range()
        else:
            if base_date is None:
                base_date = datetime.now(timezone.utc)
            # Vary publication date within last 30 days
            days_ago = random.randint(0, 30)
            pub_date = base_date - timedelta(days=days_ago)

        if article_id is None:
            article_id = self.generate_article_id()

        return {
            "id": article_id,
            "type": random.choice(self.TYPES),
            "sectionId": random.choice(self.SECTIONS),
            "sectionName": random.choice(self.SECTIONS).title(),
            "webPublicationDate": pub_date.isoformat(),
            "webTitle": fake.sentence(nb_words=8),
            "webUrl": f"https://{random.choice(self.PUBLICATION_HOSTS)}/{'/'.join(article_id.split('/'))}",
            "apiUrl": f"https://content.guardianapis.com/{article_id}",
            "isHosted": random.choice([True, False]),
            "pillarId": f"pillar/{random.choice(['news', 'opinion', 'sport', 'arts', 'lifestyle'])}",
            "pillarName": random.choice(["News", "Opinion", "Sport", "Arts", "Lifestyle"]),
        }

    def generate_article(
        self,
        article_id: str | None = None,
        base_date: datetime | None = None,
    ) -> dict:
        """Generate a full article (with fields/content).
        
        This represents what comes back from Guardian query API.
        Used by ingestion_news.py stage after having metadata IDs.
        
        Args:
            article_id: Custom article ID (if None, generates one)
            base_date: Ignored if date range is set; uses date range instead
        
        NOTE: In production, this would be queried from Guardian API
        for each metadata ID. Here we generate it synthetically.
        """
        # Use date range if configured, otherwise use base_date or now
        if self.from_date and self.to_date:
            pub_date = self._get_random_date_in_range()
        else:
            if base_date is None:
                base_date = datetime.now(timezone.utc)
            # Vary publication date within last 30 days
            days_ago = random.randint(0, 30)
            pub_date = base_date - timedelta(days=days_ago)

        if article_id is None:
            article_id = self.generate_article_id()

        return {
            "id": article_id,
            "type": random.choice(self.TYPES),
            "sectionId": random.choice(self.SECTIONS),
            "sectionName": random.choice(self.SECTIONS).title(),
            "webPublicationDate": pub_date.isoformat(),
            "webTitle": fake.sentence(nb_words=8),
            "webUrl": f"https://{random.choice(self.PUBLICATION_HOSTS)}/{'/'.join(article_id.split('/'))}",
            "apiUrl": f"https://content.guardianapis.com/{article_id}",
            "isHosted": random.choice([True, False]),
            "pillarId": f"pillar/{random.choice(['news', 'opinion', 'sport', 'arts', 'lifestyle'])}",
            "pillarName": random.choice(["News", "Opinion", "Sport", "Arts", "Lifestyle"]),
            "fields": {
                "bodyText": fake.paragraphs(nb=random.randint(3, 8)),
                "byline": fake.name() if random.random() > 0.2 else None,
                "wordcount": str(random.randint(300, 2000)),
                "firstPublicationDate": pub_date.isoformat(),
                "thumbnail": fake.image_url() if random.random() > 0.3 else None,
                "trailText": fake.sentence(nb_words=15),
                "lastModified": (pub_date + timedelta(hours=random.randint(0, 72))).isoformat(),
            },
            "tags": [
                {"id": fake.slug(), "type": "keyword", "webTitle": fake.words(nb=1)[0].title()}
                for _ in range(random.randint(1, 5))
            ],
        }

    def generate_metadata_batch(
        self,
        count: int,
        base_date: datetime | None = None,
    ) -> list[dict]:
        """Generate a batch of metadata records (search results)."""
        return [
            self.generate_metadata(base_date=base_date)
            for _ in range(count)
        ]

    def generate_article_batch(
        self,
        count: int,
        base_date: datetime | None = None,
    ) -> list[dict]:
        """Generate a batch of full articles."""
        return [
            self.generate_article(base_date=base_date)
            for _ in range(count)
        ]

    def generate_batch(
        self,
        count: int,
        base_date: datetime | None = None,
    ) -> list[dict]:
        """Generate a batch of mock articles (backward compatibility)."""
        return self.generate_article_batch(count, base_date)

    def stream_metadata(
        self,
        total_count: int,
        batch_size: int = 1000,
        base_date: datetime | None = None,
    ) -> Generator[dict, None, None]:
        """Stream metadata records one at a time for memory-efficient large-scale generation.
        
        Args:
            total_count: Total number of records to generate
            batch_size: Batch size (for logging)
            base_date: Ignored if date range is set during initialization
        """
        for i in range(total_count):
            yield self.generate_metadata(base_date=base_date)
            if (i + 1) % (batch_size * 10) == 0:
                logger.info(f"Streamed {i + 1} / {total_count} metadata records")

    def stream_articles(
        self,
        total_count: int,
        batch_size: int = 1000,
        base_date: datetime | None = None,
    ) -> Generator[dict, None, None]:
        """Stream articles one at a time for memory-efficient large-scale generation.
        
        Args:
            total_count: Total number of records to generate
            batch_size: Batch size (for logging)
            base_date: Ignored if date range is set during initialization
        """
        for i in range(total_count):
            yield self.generate_article(base_date=base_date)
            if (i + 1) % (batch_size * 10) == 0:
                logger.info(f"Streamed {i + 1} / {total_count} articles")

    def generate_articles_for_metadata(
        self,
        metadata_list: list[dict],
    ) -> list[dict]:
        """Generate full articles for given metadata IDs.
        
        This simulates the ingestion_news stage where we:
        1. Take metadata IDs from ingestion_metadata stage
        2. Generate corresponding full articles
        
        In production, Guardian would be queried for each ID.
        """
        articles = []
        for metadata in metadata_list:
            # Use metadata ID to generate corresponding article
            article = self.generate_article(article_id=metadata["id"])
            articles.append(article)
        return articles


if __name__ == "__main__":
    # Test the generator
    gen = MockArticleGenerator(seed=42)

    # Test metadata generation
    print("=" * 70)
    print("STAGE 1: Metadata Generation")
    print("=" * 70)
    metadata = gen.generate_metadata()
    print("Sample metadata (from search API):")
    print(f"  ID: {metadata['id']}")
    print(f"  Title: {metadata['webTitle']}")
    print(f"  Section: {metadata['sectionId']}\n")

    # Test metadata batch
    metadata_batch = gen.generate_metadata_batch(5)
    print(f"Generated batch of {len(metadata_batch)} metadata records")
    print(f"IDs: {[m['id'] for m in metadata_batch]}\n")

    # Test article generation from metadata
    print("=" * 70)
    print("STAGE 2: Article Generation from Metadata")
    print("=" * 70)
    articles = gen.generate_articles_for_metadata(metadata_batch)
    print(f"Generated {len(articles)} articles from metadata IDs")
    for i, article in enumerate(articles, 1):
        print(f"  {i}. ID: {article['id']}")
        print(f"     Title: {article['webTitle']}")
        print(f"     Body: {len(article['fields']['bodyText'])} paragraphs\n")

    # Test full article generation
    print("=" * 70)
    print("FULL WORKFLOW: Metadata → Articles")
    print("=" * 70)
    workflow_metadata = gen.generate_metadata_batch(3)
    workflow_articles = gen.generate_articles_for_metadata(workflow_metadata)
    print(f"Metadata records: {len(workflow_metadata)}")
    print(f"Articles generated: {len(workflow_articles)}")
    print(f"All IDs match: {all(m['id'] == a['id'] for m, a in zip(workflow_metadata, workflow_articles))}")
    
    # Test idempotency - running same workflow again should produce same articles
    print("\n" + "=" * 70)
    print("IDEMPOTENCY TEST")
    print("=" * 70)
    gen_seed = MockArticleGenerator(seed=42)
    
    # First run
    metadata_run1 = gen_seed.generate_metadata_batch(2)
    articles_run1 = gen_seed.generate_articles_for_metadata(metadata_run1)
    
    # Second run with fresh generator but same seed
    gen_seed2 = MockArticleGenerator(seed=42)
    metadata_run2 = gen_seed2.generate_metadata_batch(2)
    articles_run2 = gen_seed2.generate_articles_for_metadata(metadata_run2)
    
    # Compare
    print(f"Metadata Run 1 ID: {metadata_run1[0]['id']}")
    print(f"Metadata Run 2 ID: {metadata_run2[0]['id']}")
    print(f"Metadata match: {metadata_run1[0]['id'] == metadata_run2[0]['id']}")
    
    print(f"\nArticle Run 1 ID: {articles_run1[0]['id']}")
    print(f"Article Run 2 ID: {articles_run2[0]['id']}")
    print(f"Article match: {articles_run1[0]['id'] == articles_run2[0]['id']}")
    print(f"Article title match: {articles_run1[0]['webTitle'] == articles_run2[0]['webTitle']}")
