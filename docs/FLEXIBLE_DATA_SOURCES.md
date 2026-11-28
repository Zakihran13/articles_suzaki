# Flexible Data Source Architecture for ELT Testing

## Overview

The ELT pipeline now supports **flexible data sources** to enable testing at scale without relying on external APIs. This allows you to:

- **Test with 1M+ rows of mock data** without consuming API quotas
- **Keep the production API source intact** for real deployments
- **Switch between sources via configuration** without code changes
- **Reproduce tests deterministically** with optional seed values
- **Measure performance and resource usage** at scale

## Architecture

```
┌─────────────────────────────────────────────────┐
│          ingestion_news.py (main)               │
│          (source-agnostic)                      │
└──────────────────┬──────────────────────────────┘
                   │
         ┌─────────▼────────────┐
         │  DataSource ABC      │ (Abstract base)
         └─────────┬────────────┘
                   │
       ┌───────────┴───────────┐
       │                       │
  ┌────▼────────┐    ┌────────▼──────┐
  │ APISource   │    │  MockSource   │
  │ (Guardian)  │    │ (1M test data)│
  └─────────────┘    └───────────────┘
       │                   │
       └───────────────────┘
              │
         ┌────▼─────────────┐
         │   warehouse      │
         │   (MongoDB/PSQL) │
         └──────────────────┘
```

## Components

### 1. **DataSource Abstraction** (`source.py`)

The base `DataSource` interface defines:

```python
class DataSource(ABC):
    async def search(...) -> dict
    async def query(news_id: str) -> dict
    async def close()
```

#### `GuardianAPISource`
- Wraps the existing `GuardianClient`
- Used in production
- Respects API rate limits and quotas

#### `MockDataSource`
- Generates synthetic article data
- Configurable: total articles, batch size, seed
- In-memory caching for query efficiency
- No external dependencies or rate limits

### 2. **Mock Data Generator** (`mock_data_generator.py`)

Generates realistic article data with:

- **Deterministic IDs** for reproducibility
- **Realistic fields**: title, URL, section, publication date, article text
- **Varied content**: random authors, sections, body lengths
- **Faker integration** for natural-looking text
- **Stream-friendly**: generate articles one at a time or in batches

```python
# Single article
generator = MockArticleGenerator(seed=42)
article = generator.generate_article()

# Batch
batch = generator.generate_batch(count=1000)

# Stream for memory efficiency
for article in generator.stream_articles(total_count=1_000_000):
    process(article)
```

### 3. **Configuration** (`config.py`)

New environment variables control the data source:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATA_SOURCE` | `api` | Choose `api` or `mock` |
| `MOCK_TOTAL_ARTICLES` | `1000000` | Total articles to simulate |
| `MOCK_BATCH_SIZE` | `10` | Articles per search result page |
| `MOCK_SEED` | `None` | Random seed (optional) |

### 4. **Load Testing Utilities** (`load_test.py`)

Helpers for performance testing:

```python
# Generate metadata for 1M articles
await generate_mock_metadata_batch(
    collection,
    total_records=1_000_000,
    batch_size=10_000
)

# Stream articles
async for article in stream_mock_articles(
    batch_size=1000,
    total_count=1_000_000,
    seed=42
):
    process(article)

# Full load test
metrics = await load_test_ingestion(
    metadata_collection,
    news_collection,
    total_articles=1_000_000,
    sample_extraction=100,
)
```

## Usage Patterns

### Pattern 1: Use Mock Data (Testing/Load Testing)

```bash
# Set environment variables
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=1000000
export MOCK_SEED=42

# Run the ingestion
python -m articles_suzaki.ingestion.ingestion_news
```

**Benefits:**
- No API quota consumption
- Reproducible (same seed = same data)
- Fast (no network latency)
- Deterministic IDs for tracking

### Pattern 2: Use Real API (Production)

```bash
# Set environment variables
export DATA_SOURCE=api
export GUARDIAN_OPEN_PLATFORM_API=your_api_key

# Run the ingestion
python -m articles_suzaki.ingestion.ingestion_news
```

**Benefits:**
- Real article data
- Actual Guardian API behavior
- Production-ready

### Pattern 3: Programmatic Control

```python
from articles_suzaki.ingestion.ingestion_news import main

# Test with mock data
await main(source_type="mock")

# Production with API
await main(source_type="api", api_key="your_key")
```

### Pattern 4: Load Test Script

```python
from articles_suzaki.ingestion.load_test import load_test_ingestion
from articles_suzaki.warehouse.client import get_async_mongodb

db = get_async_mongodb("test_db")
metadata_coll = db["metadata"]
news_coll = db["news"]

metrics = await load_test_ingestion(
    metadata_collection=metadata_coll,
    news_collection=news_coll,
    total_articles=1_000_000,
    batch_size=5_000,
    sample_extraction=100,
)

print(f"Generated {metrics['metadata_records']:,} metadata records")
print(f"Throughput: {metrics['metadata_records'] / metrics['metadata_duration_seconds']:,.0f} records/sec")
```

## Configuration Examples

### `.env` for Testing

```bash
# Use mock source for testing
DATA_SOURCE=mock
MOCK_TOTAL_ARTICLES=1000000
MOCK_BATCH_SIZE=10
MOCK_SEED=42

# Keep other configs as normal
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=articles_test
# ...
```

### `.env` for Production

```bash
# Use real API
DATA_SOURCE=api
GUARDIAN_OPEN_PLATFORM_API=your-api-key-here

# Production database config
MONGO_URI=mongodb://prod-host:27017
MONGO_DB_NAME=articles_production
# ...
```

## Performance Characteristics

### Mock Data Generation

On a modern machine (3GHz+ CPU):

```
Metadata Generation:
  - 1M records: ~10-30 seconds (33k-100k records/sec)
  - Batch size: 10k records for optimal MongoDB performance

Article Streaming:
  - 1M articles: ~2-5 seconds (200k-500k articles/sec)
  - Memory efficient: processes one article at a time
```

### Real Guardian API

```
API Rate Limit: 1 request/second, 500/day
Throughput: 1 request/sec = 3,600/hour max
For 1M articles: 1,000,000 / 3,600 ≈ 278 hours minimum
```

**Use mock data for performance testing. Use API for real data.**

## Testing Strategy

### Unit Tests

```python
# Test mock generator
def test_article_generation():
    gen = MockArticleGenerator(seed=42)
    article = gen.generate_article()
    assert article["id"]
    assert article["webTitle"]
    assert article["fields"]["bodyText"]
```

### Integration Tests

```python
# Test with mock source
async def test_ingestion_with_mock():
    source = create_source("mock", total_articles=100)
    # Test ingestion flow without API
    await main(source_type="mock")
```

### Load Tests

```python
# Test system capacity
async def test_system_at_scale():
    metrics = await load_test_ingestion(
        metadata_collection,
        news_collection,
        total_articles=1_000_000,
    )
    # Verify performance SLAs
    assert metrics["metadata_duration_seconds"] < 60  # <1 min
```

## Migration Path

### For Existing Code

No breaking changes! The old `GuardianClient` interface still works:

```python
# This still works
guardian = GuardianClient(api_key="...")
await guardian.search()

# But now you can also use the abstraction
source = create_source("api", api_key="...")
await source.search()
```

### Gradual Adoption

1. Update `ingestion_news.py` to use `DataSource` (✅ done)
2. Add environment variables to `.env` and `config.py` (✅ done)
3. Update orchestration (Dagster) to pass `source_type` parameter
4. Create load test scripts for CI/CD validation
5. Document in runbooks

## Troubleshooting

### "Unknown source type" error

**Problem:** `ValueError: Unknown source type: xyz`

**Solution:** Set `DATA_SOURCE=api` or `DATA_SOURCE=mock`

```bash
export DATA_SOURCE=mock
```

### Mock data looks unrealistic

**Problem:** Articles have generic/dummy text

**Solution:** Faker generates realistic but synthetic content. For specific requirements, customize `MockArticleGenerator`:

```python
class CustomArticleGenerator(MockArticleGenerator):
    def generate_article(self, ...):
        article = super().generate_article(...)
        # Customize fields
        article["webTitle"] = "Real Guardian Article"
        return article
```

### Seed not reproducing data

**Problem:** Different data with same seed

**Solution:** Seed must be set before generating articles:

```python
# ✅ Correct
gen = MockArticleGenerator(seed=42)
articles = gen.generate_batch(10)

# ❌ Wrong
gen = MockArticleGenerator()
Faker.seed(42)  # Too late!
```

### Out of memory with 1M records

**Problem:** `MemoryError` when generating all articles

**Solution:** Use streaming instead:

```python
# ❌ Avoid
articles = gen.generate_batch(1_000_000)  # Loads all in memory

# ✅ Use streaming
async for article in stream_mock_articles(total_count=1_000_000):
    await process_article(article)
```

## Future Enhancements

1. **Multiple Mock Strategies**
   - Add `--strategy=realistic` for different data distributions
   - Support specialized datasets (e.g., tech-only, varied languages)

2. **Performance Profiling**
   - Measure memory usage per batch
   - Track database insert throughput
   - Profile transformation bottlenecks

3. **Source Switching in Dagster**
   - Add Dagster sensor to toggle between mock/real based on schedule
   - Resource overrides for test vs. prod jobs

4. **Data Validation**
   - Pydantic models for mock articles
   - Validate mock data matches Guardian API schema

5. **Incremental Mode**
   - Cache mock articles to disk
   - Load from cache instead of regenerating

## References

- **Mock Generator:** [`mock_data_generator.py`](mock_data_generator.py)
- **Source Abstraction:** [`source.py`](source.py)
- **Ingestion Logic:** [`ingestion_news.py`](ingestion_news.py)
- **Load Testing:** [`load_test.py`](load_test.py)
- **Configuration:** [`config.py`](../configuration/config.py)

