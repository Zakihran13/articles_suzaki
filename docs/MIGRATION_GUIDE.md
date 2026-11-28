# Migration Guide: Flexible Data Sources

## Overview

This guide helps you migrate from using only the Guardian API to the flexible data source architecture that supports both real API and mock data sources.

**Important:** This is a **backward-compatible** change. Your existing pipeline continues to work without modifications.

## What Changed?

### For Most Users: Nothing Required!

If you're currently using the Guardian API, your pipeline will continue to work exactly as before. No code changes required.

### What's New?

You can now:
- Test with 1M+ mock articles without API quotas
- Switch sources via `.env` configuration
- Run performance benchmarks
- Use reproducible synthetic data in CI/CD

## Step-by-Step Migration

### Step 1: Update Your `.env` File

Add data source configuration (optional, defaults to `api`):

```bash
# Option A: Keep using Guardian API (default)
DATA_SOURCE=api
GUARDIAN_OPEN_PLATFORM_API=your_api_key

# Option B: Use mock data for testing
DATA_SOURCE=mock
MOCK_TOTAL_ARTICLES=1000000
MOCK_BATCH_SIZE=10
MOCK_SEED=42  # Optional: for reproducible data
```

### Step 2: Update Dependencies

If you haven't already run `uv sync`:

```bash
cd /path/to/articles_suzaki
uv sync
```

This installs the new dependency: `faker>=30.0.0`

### Step 3: Choose Your Source

#### Option A: Production (Use API - Recommended for Production)

```bash
export DATA_SOURCE=api
export GUARDIAN_OPEN_PLATFORM_API=your_api_key
uv run -m articles_suzaki.ingestion.ingestion_news
```

**Benefits:**
- Real Guardian data
- Production-ready
- Same behavior as before

#### Option B: Testing (Use Mock Data - Recommended for Testing)

```bash
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=1000000
export MOCK_SEED=42
uv run -m articles_suzaki.ingestion.ingestion_news
```

**Benefits:**
- No API quota consumption
- Reproducible data (same seed = same data)
- Fast generation (no network latency)
- Deterministic IDs

## Configuration Examples

### Configuration 1: Development Environment

```bash
# .env for local development
DATA_SOURCE=mock
MOCK_TOTAL_ARTICLES=10000
MOCK_SEED=42

MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=articles_dev
```

**Usage:**
```bash
uv run -m articles_suzaki.ingestion.ingestion_news
```

### Configuration 2: CI/CD Pipeline

```bash
# In your GitHub Actions / CI configuration
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=1000
export MOCK_SEED=$GITHUB_RUN_ID  # Different for each run
uv run pytest tests/test_ingestion.py
```

### Configuration 3: Production

```bash
# .env for production
DATA_SOURCE=api
GUARDIAN_OPEN_PLATFORM_API=$PROD_API_KEY

MONGO_URI=$PROD_MONGO_URI
MONGO_DB_NAME=articles_production
```

## Code Changes (Optional)

### Your Existing Code Still Works

```python
# This still works - no changes needed
from articles_suzaki.ingestion.ingestion_news import main
await main()  # Uses DATA_SOURCE from config
```

### New: Explicit Source Control

You can now control the source explicitly:

```python
# Option A: Use configuration
from articles_suzaki.ingestion.ingestion_news import main
await main()  # Uses DATA_SOURCE from .env

# Option B: Override with explicit parameter
from articles_suzaki.ingestion.ingestion_news import main
await main(source_type="mock")  # Always use mock
await main(source_type="api")   # Always use API
```

### New: Mock Data in Tests

```python
# Before: Had to mock API calls manually
@patch('articles_suzaki.ingestion.client.GuardianClient.query')
async def test_ingestion(mock_query):
    mock_query.return_value = {...}
    # ... complex mocking

# After: Use mock data source
from articles_suzaki.ingestion.source import create_source

async def test_ingestion():
    source = create_source("mock", total_articles=100)
    # ... simple, no mocking needed
```

## Verification Steps

### Verify Installation

```bash
# Check that dependencies are installed
uv run python -c "from articles_suzaki.ingestion.mock_data_generator import MockArticleGenerator; print('✓ Mock generator available')"
uv run python -c "from articles_suzaki.ingestion.source import create_source; print('✓ Source factory available')"
```

### Verify Configuration

```bash
# Check that config loads
uv run python -c "from articles_suzaki.configuration.config import config; print(f'Data source: {config.data_source}')"
```

### Run Tests

```bash
# Run all tests (including new flexible source tests)
uv run pytest tests/test_flexible_sources.py -v
```

### Run Examples

```bash
# Try the examples
uv run examples/flexible_sources_demo.py
```

## Troubleshooting

### "Module not found" error for mock_data_generator

**Problem:** `ModuleNotFoundError: No module named 'articles_suzaki.ingestion.mock_data_generator'`

**Solution:** Run `uv sync` to install dependencies:
```bash
uv sync
```

### "Unknown source type" error

**Problem:** `ValueError: Unknown source type: xyz`

**Solution:** Set `DATA_SOURCE` to either `api` or `mock`:
```bash
export DATA_SOURCE=mock
```

### Mock data looks too generic

**Problem:** Fake titles/bodies seem unrealistic

**Solution:** This is normal - Faker generates synthetic but realistic data. For specific requirements, customize `MockArticleGenerator` or use API data.

### Out of memory with 1M articles

**Problem:** `MemoryError` when generating all articles

**Solution:** Use streaming instead of batch generation:
```python
# ❌ Wrong
articles = generator.generate_batch(1_000_000)

# ✅ Correct
async for article in stream_mock_articles(total_count=1_000_000):
    await process(article)
```

## Performance Considerations

### Mock Data Performance

On modern hardware (3GHz+ CPU):

```
Metadata generation: 30k-100k records/second
Article streaming:   200k-500k articles/second
1M articles:         ~5-15 minutes total
```

### API Performance

Guardian API limits:
- **Rate limit:** 1 request/second
- **Daily limit:** 500 requests/day
- **For 1M articles:** ~278 hours minimum (not practical)

**Recommendation:** Use mock data for performance testing, API for production.

## Rollback Plan

If you need to rollback to only using the Guardian API:

### Step 1: Revert Configuration
```bash
# In .env or environment
DATA_SOURCE=api
GUARDIAN_OPEN_PLATFORM_API=your_api_key
```

### Step 2: Revert Code (if modified)
```bash
git checkout src/articles_suzaki/ingestion/ingestion_news.py
```

### Step 3: Existing Features Still Work
- All existing code continues to work
- No database schema changes
- No breaking changes

## Next Steps

1. **For Development:** Set `DATA_SOURCE=mock` and test locally
2. **For CI/CD:** Add load testing with mock data
3. **For Production:** Keep `DATA_SOURCE=api` with your production key
4. **For Documentation:** Read [FLEXIBLE_DATA_SOURCES.md](../docs/FLEXIBLE_DATA_SOURCES.md)

## FAQ

**Q: Will my existing pipeline break?**
A: No. It's backward-compatible. Set `DATA_SOURCE=api` and everything works as before.

**Q: Do I have to use mock data?**
A: No. It's optional. Keep using the API if you prefer.

**Q: Can I mix mock and real data?**
A: Not in a single run, but you can test with mock and deploy with API.

**Q: Is mock data realistic?**
A: Yes. Faker generates synthetic but realistic article metadata and content.

**Q: Why is mock data so fast?**
A: No network calls. Data is generated in-memory or from cache.

**Q: Can I export mock data to Parquet?**
A: Yes. Use `load_test.stream_mock_articles()` and export as needed.

**Q: Does mock data consume disk space?**
A: Only when stored (in MongoDB). Generation is in-memory.

## Support

For issues or questions:

1. Check [FLEXIBLE_DATA_SOURCES.md](../docs/FLEXIBLE_DATA_SOURCES.md) for detailed documentation
2. Review [examples/README.md](../examples/README.md) for practical examples
3. Run tests: `uv run pytest tests/test_flexible_sources.py -v`
4. Check error messages - they're descriptive

