"""
Example script: Using flexible data sources for testing ELT pipeline at scale.

This demonstrates how to:
1. Generate mock metadata for 1M articles
2. Stream and load sample articles
3. Measure performance
"""

import asyncio
from loguru import logger
from articles_suzaki.ingestion.data_mock.mock_data_generator import MockArticleGenerator
from articles_suzaki.ingestion.data_mock.load_test import (
    generate_mock_metadata_batch,
    stream_mock_articles,
    load_test_ingestion,
)
from articles_suzaki.warehouse.client import get_async_mongodb


async def example_1_generate_articles():
    """Example 1: Generate and inspect mock articles."""
    print("\n" + "=" * 70)
    print("Example 1: Generate and Inspect Mock Articles")
    print("=" * 70)

    generator = MockArticleGenerator(seed=42)

    # Generate a single article
    print("\n1. Single Article Generation:")
    article = generator.generate_article(article_id="example/0001")
    print(f"   ID: {article['id']}")
    print(f"   Title: {article['webTitle']}")
    print(f"   Section: {article['sectionId']}")
    print(f"   Published: {article['webPublicationDate']}")
    print(f"   Body Preview: {article['fields']['bodyText'][0][:100]}...")

    # Generate a batch
    print("\n2. Batch Generation (5 articles):")
    batch = generator.generate_batch(count=5)
    for i, article in enumerate(batch, 1):
        print(f"   {i}. {article['webTitle'][:50]}...")

    # Stream articles (memory efficient)
    print("\n3. Stream Articles (first 3 out of 100):")
    count = 0
    async for article in stream_mock_articles(batch_size=1000, total_count=100):
        if count < 3:
            print(f"   {count + 1}. {article['webTitle'][:50]}...")
        count += 1
        if count >= 3:
            break
    print(f"   ... (and 97 more)")


async def example_2_load_testing():
    """Example 2: Run a load test with mock data."""
    print("\n" + "=" * 70)
    print("Example 2: Load Testing with 1M Mock Articles")
    print("=" * 70)

    # Connect to MongoDB
    db = get_async_mongodb("articles_test")
    metadata_coll = db["test_metadata"]
    news_coll = db["test_news"]

    # Clear collections
    await metadata_coll.delete_many({})
    await news_coll.delete_many({})

    # Run load test
    print("\nRunning load test...")
    metrics = await load_test_ingestion(
        metadata_collection=metadata_coll,
        news_collection=news_coll,
        total_articles=1_000_000,  # Generate metadata for 1M articles
        batch_size=5_000,          # Insert in 5k batches
        sample_extraction=100,     # Extract and load 100 sample articles
    )

    # Print summary
    print("\n" + "-" * 70)
    print("LOAD TEST RESULTS")
    print("-" * 70)
    print(f"Metadata records generated: {metrics['metadata_records']:,}")
    print(f"Metadata generation time: {metrics['metadata_duration_seconds']:.2f}s")
    if metrics["metadata_duration_seconds"] > 0:
        rate = metrics["metadata_records"] / metrics["metadata_duration_seconds"]
        print(f"  → Throughput: {rate:,.0f} records/second")

    print(f"\nArticles extracted: {metrics['articles_extracted']:,}")
    print(f"Articles loaded: {metrics['articles_loaded']:,}")
    print(f"Extraction time: {metrics['extraction_duration_seconds']:.2f}s")
    if metrics["extraction_duration_seconds"] > 0:
        rate = metrics["articles_extracted"] / metrics["extraction_duration_seconds"]
        print(f"  → Throughput: {rate:,.0f} articles/second")

    print(f"\nTotal pipeline time: {metrics['total_duration_seconds']:.2f}s")

    # Cleanup
    await metadata_coll.delete_many({})
    await news_coll.delete_many({})


async def example_3_deterministic_generation():
    """Example 3: Reproduce exact data with seeds."""
    print("\n" + "=" * 70)
    print("Example 3: Deterministic Generation with Seeds")
    print("=" * 70)

    # Generate articles with seed=42
    print("\nGeneration 1 (seed=42):")
    gen1 = MockArticleGenerator(seed=42)
    articles1 = gen1.generate_batch(count=3)
    for article in articles1:
        print(f"  Title: {article['webTitle'][:50]}...")

    # Generate articles with same seed
    print("\nGeneration 2 (seed=42):")
    gen2 = MockArticleGenerator(seed=42)
    articles2 = gen2.generate_batch(count=3)
    for article in articles2:
        print(f"  Title: {article['webTitle'][:50]}...")

    # Verify they're identical
    print("\nVerification:")
    all_match = all(
        a1["webTitle"] == a2["webTitle"]
        for a1, a2 in zip(articles1, articles2)
    )
    print(f"  Articles match: {all_match} ✓" if all_match else f"  Articles match: False ✗")
    print("  → Use seeds for reproducible test data")


async def example_4_data_source_abstraction():
    """Example 4: Using the data source abstraction."""
    print("\n" + "=" * 70)
    print("Example 4: Flexible Data Source Abstraction")
    print("=" * 70)

    from articles_suzaki.ingestion.data_mock.source import create_source

    # Create mock source
    print("\n1. Create Mock Data Source:")
    source = create_source(
        source_type="mock",
        total_articles=100,
        batch_size=10,
        seed=42,
    )
    print(f"   Source type: {type(source).__name__}")
    print(f"   Total articles: {source.total_articles}")

    # Search with mock source
    print("\n2. Search for articles:")
    result = await source.search(page=1, page_size=10)
    total = result["response"]["total"]
    results = len(result["response"]["results"])
    print(f"   Found {total:,} total articles (page 1 has {results} results)")

    # Query specific article
    print("\n3. Query specific article:")
    result = await source.query("mock/00000000")
    article = result["response"]["content"]
    print(f"   ID: {article['id']}")
    print(f"   Title: {article['webTitle']}")
    print(f"   Section: {article['sectionId']}")

    await source.close()
    print("\n4. Source closed successfully")


async def main():
    """Run all examples."""
    logger.info("Starting flexible data source examples...")

    # Suppress detailed logging for cleaner output
    logger.remove()
    logger.add(lambda msg: None, level="DEBUG")  # Suppress debug
    logger.add(lambda msg: print(msg, end=""), level="INFO", format="{message}")

    try:
        await example_1_generate_articles()
        await example_2_load_testing()
        await example_3_deterministic_generation()
        await example_4_data_source_abstraction()

        print("\n" + "=" * 70)
        print("✓ All examples completed successfully!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. See docs/FLEXIBLE_DATA_SOURCES.md for detailed documentation")
        print("2. Set DATA_SOURCE=mock in your .env to use mock data")
        print("3. Run: uv run -m articles_suzaki.ingestion.ingestion_news")

    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
