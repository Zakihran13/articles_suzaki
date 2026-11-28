# Flexible Data Sources Examples

This directory contains practical examples of using the flexible data source architecture for testing your ELT pipeline at scale.

## Quick Start

### Example 1: Mock Data Generation
See how to generate realistic mock articles:

```bash
uv run examples/flexible_sources_demo.py
```

This demonstrates:
- Single article generation
- Batch generation
- Streaming articles efficiently
- Deterministic generation with seeds

### Example 2: Switching Data Sources

#### Use Mock Data (for testing/load testing):

```bash
# Set environment variables
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=1000000
export MOCK_SEED=42

# Run the ingestion
uv run -m articles_suzaki.ingestion.ingestion_news
```

#### Use Real Guardian API (for production):

```bash
# Set environment variables
export DATA_SOURCE=api
export GUARDIAN_OPEN_PLATFORM_API=your_api_key

# Run the ingestion
uv run -m articles_suzaki.ingestion.ingestion_news
```

### Example 3: Load Testing

Generate metadata for 1M articles and measure throughput:

```bash
# Using the load test utilities
uv run examples/flexible_sources_demo.py
```

Expected performance on modern hardware:
- Metadata generation: 30k-100k records/second
- Article streaming: 200k-500k articles/second

## Files

- **flexible_sources_demo.py** - Interactive examples showing all capabilities
- **FLEXIBLE_DATA_SOURCES.md** - Detailed documentation (see `docs/`)

## Configuration Options

| Environment Variable | Default | Purpose |
|---|---|---|
| `DATA_SOURCE` | `api` | Choose `api` or `mock` |
| `MOCK_TOTAL_ARTICLES` | `1000000` | Total articles to simulate |
| `MOCK_BATCH_SIZE` | `10` | Articles per search result page |
| `MOCK_SEED` | `None` | Random seed for reproducibility |

## Real-World Scenarios

### Scenario 1: CI/CD Testing
Use mock data to test your pipeline without consuming API quotas:

```bash
# In your CI/CD pipeline
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=10000
export MOCK_SEED=42  # Always reproducible
uv run pytest tests/test_ingestion.py
```

### Scenario 2: Local Development
Test full pipeline locally with realistic data:

```bash
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=100000
uv run -m articles_suzaki.ingestion.ingestion_news
```

### Scenario 3: Performance Testing
Measure system capacity with 1M articles:

```bash
export DATA_SOURCE=mock
export MOCK_TOTAL_ARTICLES=1000000
export MOCK_BATCH_SIZE=10000
time uv run -m articles_suzaki.ingestion.ingestion_news
```

### Scenario 4: Production Deployment
Use real API data:

```bash
export DATA_SOURCE=api
export GUARDIAN_OPEN_PLATFORM_API=your_production_key
uv run -m articles_suzaki.ingestion.ingestion_news
```

## Benchmarking

Run the demo script and monitor these metrics:

1. **Metadata Generation Rate** (records/second)
   - Target: >50k records/second
   - Varies by: batch size, MongoDB performance

2. **Article Extraction Rate** (articles/second)
   - Target: >100k articles/second
   - Memory efficient (single article at a time)

3. **End-to-End Throughput**
   - For 1M articles: typically 5-15 minutes total
   - Depends on batch sizes and MongoDB index performance

## Troubleshooting

### "Unknown source type" error
```bash
export DATA_SOURCE=mock  # or 'api'
```

### Out of memory with large datasets
Use the streaming approach:
```python
async for article in stream_mock_articles(total_count=1_000_000):
    # Process one article at a time
    await process(article)
```

### Need reproducible data
Always use a seed:
```bash
export MOCK_SEED=42
```

## Next Steps

1. Read [FLEXIBLE_DATA_SOURCES.md](../docs/FLEXIBLE_DATA_SOURCES.md) for comprehensive documentation
2. Update your `.env` file with `DATA_SOURCE=mock` for testing
3. Add load testing to your CI/CD pipeline
4. Monitor performance metrics for your specific hardware

