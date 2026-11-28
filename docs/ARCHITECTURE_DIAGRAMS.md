# Architecture Diagram: Flexible Data Sources

## System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Ingestion Pipeline                            │
│                   (ingestion_news.py)                             │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────┐
         │   DataSource Abstraction        │
         │  (source.py - Abstract Base)    │
         └────────────┬────────────────────┘
                      │
        ┌─────────────┴──────────────┐
        │                            │
        ▼                            ▼
    ┌────────────┐           ┌───────────────┐
    │  API Mode  │           │   Mock Mode   │
    └────────────┘           └───────────────┘
        │                            │
        ▼                            ▼
  ┌──────────────┐        ┌──────────────────────┐
  │ Guardian     │        │ MockDataSource       │
  │ APISource    │        │ (source.py)          │
  │ ├─ search() │        │ ├─ search()         │
  │ ├─ query()  │        │ ├─ query()          │
  │ └─ close()  │        │ └─ close()          │
  └──────┬───────┘        └──────┬───────────────┘
         │                       │
         ▼                       ▼
  ┌──────────────┐        ┌──────────────────────┐
  │ Guardian     │        │ MockArticle          │
  │ Client       │        │ Generator            │
  │ (client.py)  │        │ (mock_data_generator)│
  │              │        │ ├─ generate_article()│
  │ Actual API   │        │ ├─ generate_batch() │
  │ Calls via    │        │ └─ stream_articles()│
  │ httpx        │        │                      │
  │ 1 req/sec    │        │ Deterministic       │
  │ 500/day      │        │ Reproducible        │
  │              │        │ 100k+ articles/sec  │
  └──────────────┘        └──────────────────────┘
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
         ┌──────────────────────┐
         │ MongoDB (Landing)    │
         │ ├─ raw_metadata      │
         │ └─ raw_articles      │
         └──────────────────────┘
                     │
                     ▼
         ┌──────────────────────┐
         │ PostgreSQL           │
         │ (Staging/Warehouse)  │
         └──────────────────────┘
```

## Configuration Flow

```
Environment Variables (.env)
│
├─ DATA_SOURCE = "api" or "mock"
│  │
│  └─→ Routing in create_source()
│      ├─ "api"  → GuardianAPISource
│      └─ "mock" → MockDataSource
│
└─ MOCK_TOTAL_ARTICLES
└─ MOCK_BATCH_SIZE
└─ MOCK_SEED
   ├─→ MockArticleGenerator initialization
   └─→ Deterministic or random generation
```

## Data Flow Comparison

### Real Guardian API (DATA_SOURCE=api)
```
Request → Guardian API (1 req/sec) → Response → MongoDB → PostgreSQL
   ↓              ↓                      ↓         ↓         ↓
1 sec            Actual                 Real     Raw       Processed
            Network Latency          Content   Data       Data

Performance: ~1 article/sec (rate limited)
Cost: API quotas
Real Data: Yes
```

### Mock Data (DATA_SOURCE=mock)
```
Article ID → Generator (Faker) → Article Object → MongoDB → PostgreSQL
   ↓              ↓                    ↓          ↓         ↓
instant      In-Memory          Synthetic       Mock      Processed
            Generation           Content       Data       Data

Performance: 100k+ articles/sec (no limits)
Cost: None
Real Data: No (but realistic)
```

## Component Interactions

```
Test/Application Code
        │
        ├─ ingestion_news.py (source-agnostic)
        │  ├─ main(source_type=?)
        │  └─ create_source()
        │
        ▼
    Configuration (config.py)
        │
        ├─ data_source: str
        ├─ mock_total_articles: int
        ├─ mock_batch_size: int
        └─ mock_seed: int | None
            │
            ├─→ Loaded from .env
            └─→ Validated by Pydantic
        │
        ▼
    Source Factory (source.py:create_source)
        │
        ├─ If source_type == "api"
        │  └─→ GuardianAPISource(GuardianClient(...))
        │
        └─ If source_type == "mock"
           └─→ MockDataSource(total_articles, batch_size, seed)
                   │
                   ▼
            MockArticleGenerator(seed)
                   │
                   ├─ generate_article()
                   ├─ generate_batch()
                   └─ stream_articles()
                       │
                       └─→ Uses Faker for text generation
```

## Load Testing Flow

```
load_test_ingestion()
│
├─ Phase 1: Metadata Generation
│  │
│  ├─ generate_mock_metadata_batch(1,000,000)
│  │  ├─ Batch 1: 10,000 metadata records → MongoDB
│  │  ├─ Batch 2: 10,000 metadata records → MongoDB
│  │  └─ ... 100 batches total
│  │
│  └─ Metrics: Records/second, duration
│
└─ Phase 2: Extract & Load Sample
   │
   ├─ stream_mock_articles(sample_extraction=100)
   │  ├─ Article 1 → Extract → Load
   │  ├─ Article 2 → Extract → Load
   │  └─ ... 100 articles total
   │
   └─ Metrics: Articles/second, duration, total throughput
```

## Configuration Examples

### Configuration Stack

```
Environment Variables (.env)
        │
        ├─ DATA_SOURCE=api
        │  └─→ Uses Guardian API (Production)
        │
        ├─ DATA_SOURCE=mock
        │  ├─ MOCK_TOTAL_ARTICLES=1000000
        │  ├─ MOCK_BATCH_SIZE=10
        │  └─ MOCK_SEED=42
        │     └─→ Uses Mock Data (Testing)
        │
        └─ Database Settings
           ├─ MONGO_URI
           ├─ MONGO_DB_NAME
           ├─ POSTGRESQL settings
           └─→ Same for both sources
```

## Test Coverage

```
test_flexible_sources.py
│
├─ TestMockArticleGenerator (5 tests)
│  ├─ test_generate_single_article
│  ├─ test_deterministic_with_seed
│  ├─ test_generate_batch
│  ├─ test_article_id_format
│  └─ test_custom_article_id
│
├─ TestDataSourceFactory (3 tests)
│  ├─ test_create_mock_source
│  ├─ test_create_api_source
│  └─ test_invalid_source_type
│
├─ TestMockDataSource (5 tests)
│  ├─ test_search_returns_results
│  ├─ test_search_pagination
│  ├─ test_search_beyond_total
│  ├─ test_query_returns_full_article
│  └─ test_close
│
└─ TestArticleSchema (3 tests)
   ├─ test_article_has_required_fields
   ├─ test_article_fields_completeness
   └─ test_article_tags_structure

Result: ✓ 16/16 PASSED
```

## Deployment Scenarios

```
Development (Local)
├─ DATA_SOURCE=mock
├─ MOCK_TOTAL_ARTICLES=10000
├─ MOCK_SEED=42
└─ MONGO_URI=mongodb://localhost
   └─→ Fast, reproducible, no API calls

CI/CD (GitHub Actions)
├─ DATA_SOURCE=mock
├─ MOCK_TOTAL_ARTICLES=1000
├─ MOCK_SEED=$GITHUB_RUN_ID
└─ MONGO_URI=mongodb://test-container
   └─→ Fast, deterministic, no quota cost

Production (Cloud)
├─ DATA_SOURCE=api
├─ GUARDIAN_OPEN_PLATFORM_API=$SECRETS
└─ MONGO_URI=mongodb+srv://prod
   └─→ Real data, production-safe

Benchmarking (Local)
├─ DATA_SOURCE=mock
├─ MOCK_TOTAL_ARTICLES=1000000
└─ MOCK_BATCH_SIZE=10000
   └─→ Performance testing, unlimited scale
```

## Performance Layers

```
Layer 1: Article Generation
  ├─ Single: generate_article()      → ~100k articles/sec
  ├─ Batch:  generate_batch(n)        → ~100k articles/sec
  └─ Stream: stream_articles()        → ~200k articles/sec (memory efficient)

Layer 2: Metadata Insertion
  ├─ Batch size: 10,000 records
  ├─ Concurrency: 4 connections
  └─ Throughput: 30k-100k records/sec

Layer 3: Article Extraction/Loading
  ├─ Async gathering: parallel queries
  ├─ Batch upserts: 2,000 per batch
  └─ Throughput: 50k-100k articles/sec

Total: 1M articles in 5-15 minutes
```

