# Articles ETL/ELT implementation plan

## 1. MVP definition

Build one hourly pipeline that ingests **The Guardian's technology articles**, lands raw API data in MongoDB, and maintains a queryable `articles` table in PostgreSQL.

**Delivery target:** a reproducible, locally hosted system with retries, recovery, validation, tests, operational logs, and a working scheduled run—not a claim of cloud-scale production deployment.

### Scope

| Priority | Deliverables |
|---|---|
| **Must have: within 36 hours** | One source; hourly Dagster job; MongoDB raw landing; PostgreSQL staging and warehouse; incremental loading; idempotent upserts; retry and restart; validation; basic tests; logs and failure notification; Docker Compose; CI; concise documentation |
| **Nice to have: after MVP** | Dashboard, additional source, automated deployment, Parquet export, richer monitoring |
| **Future improvements** | Cloud object storage and warehouse, comprehensive change-data capture, deletion reconciliation, historical versions, distributed orchestration, advanced observability |

**Important limits:** The MVP is production-*oriented*, but a single local host, local disk, and free source API are not highly available. It cannot guarantee complete detection of source deletions or changes that the API no longer exposes.

---

## 2. Source recommendation

Limits and terms can change; verify them before implementation. The figures below reflect the linked documentation checked on November 26, 2026.

| Source | Why consider it | Authentication and limits | Incremental loading | Difficulty |
|---|---|---|---|---|
| **[The Guardian Open Platform](https://open-platform.theguardian.com/access/)** — **recommended** | Actual news articles, article text, stable source IDs, publication and modification metadata; easy to narrow to a section | Free developer key; **1 request/second, 500/day**; non-commercial use only | Date filtering and pagination; last-modified filtering is documented. Re-read an overlap to capture late changes | Medium |
| [Hacker News Search API](https://hn.algolia.com/api) | No key, simple JSON, rich technical-community metadata | No authentication; documented **10,000 requests/IP/hour** | Search by creation timestamp; split busy windows and paginate. Not a complete update/deletion feed | Easy |
| [DEV / Forem API](https://developers.forem.com/api/v1) | Real developer blog posts; public read endpoints and individual article bodies | Some endpoints are public; key required for others. Confirm current DEV-specific read limits before relying on them; Forem deployments may differ | Published-date ordering and pagination; no guaranteed complete change feed assumed | Medium |
| [NewsAPI](https://newsapi.org/pricing) | Broad selection of publishers and straightforward search | Free API key; **100 requests/day**, 24-hour delay, development/testing only | Date filters and pagination; free plan limited to one month of history | Easy, but unsuitable for a deployed MVP |

**Why The Guardian:** It best matches the requested article schema, including article text, without scraping publisher sites. Start with the **technology section** to keep hourly pagination and quota use manageable. If key registration or terms block the deadline, use Hacker News as a fallback, explicitly documenting that many stories are links without article bodies and that `content` will often be null.

**Content rights:** Review the [developer access terms](https://open-platform.theguardian.com/access/) before storing or displaying content. Keep raw and warehouse data private, do not commit article bodies to the repository, and do not republish full articles in the portfolio. Store and display only what the applicable terms permit; use synthetic fixtures for public tests.

---

## 3. Technology decisions

| Decision | MVP recommendation | Trade-off and rationale |
|---|---|---|
| Raw storage: SQL vs NoSQL | **MongoDB**, using raw BSON documents on a persistent volume | Flexible, queryable storage for source records and page metadata without accumulating JSON files in the project. Adds a service and operational complexity compared with files. |
| Processed storage | **SQL: PostgreSQL** | Constraints, transactions, indexed queries, and reliable upserts make recovery straightforward; the final analytical model is relational. |
| Warehouse | **PostgreSQL**, used as a small analytical warehouse | Free, local, familiar, and sufficient for a portfolio-sized dataset. It is not a distributed analytical warehouse. Migrate to BigQuery or another cloud warehouse when scale justifies cost and infrastructure. |
| Orchestration | **Dagster OSS** | Python-first jobs, schedules, retries, and a useful UI fit a single-pipeline project. Airflow is strong for larger established workflows, but has more setup overhead for this MVP. |
| Raw format | **BSON documents**, preserving API JSON values | Store each article separately along with page metadata. MongoDB does not preserve the exact original response bytes; byte-for-byte archival and Parquet analytical exports are future options. |
| Development | Linux/macOS, Docker Compose, Python, **uv** | One reproducible environment; host Python tooling remains convenient. |
| Containers | One application image reused for Dagster webserver and daemon; PostgreSQL and MongoDB services | Reuse the application image and give each database its own persistent volume. |
| MongoDB driver | **PyMongo**, managed by uv | Official Python driver; sufficient for inserts, indexes, and replay without an additional abstraction. |
| Tests | **pytest** | Fast unit tests and PostgreSQL integration tests with familiar tooling. |
| Logging | Python standard-library `logging`, structured JSON output | No extra logging service or framework needed. |
| Validation | **Pydantic** for source records; SQL constraints and quality queries for warehouse data | Explicit source contracts and database enforcement without introducing a large data-quality platform. |
| Configuration | Environment variables validated at startup with Pydantic Settings; local `.env` excluded from Git | Simple, typed, and compatible with containers. |
| CI/CD | GitHub Actions: lint, tests, and integration tests against PostgreSQL and MongoDB | CI is essential; deployment automation is not required for the local MVP. |
| Lint/format | Ruff | One fast tool for both linting and formatting checks. |

**Cloud alternative:** BigQuery is a good future destination, but its no-billing sandbox does **not** support DML statements, including the merge strategy needed here; it also expires tables after 60 days. A billed project changes the cost and setup assumptions. See [BigQuery sandbox limitations](https://docs.cloud.google.com/bigquery/docs/sandbox).

---

## 4. Architecture

```text
The Guardian Content API
        |
        v
Landing / raw: MongoDB article documents and page manifests
        |
        v
Staging: PostgreSQL batch-scoped validated records
        |
        v
Transformation: Python normalization + SQL deduplication
        |
        v
Warehouse: PostgreSQL warehouse.articles
        |
        v
Analytics: documented SQL queries / optional client
```

Dagster coordinates the flow, while PostgreSQL stores pipeline state and run metadata.

| Layer | Purpose |
|---|---|
| **Source** | Provides article records and source metadata. Scope the MVP to the technology section and article content type. |
| **Landing/raw** | Retain source article JSON values as MongoDB documents plus page envelopes and batch metadata for audit and replay. No source content is committed to Git. |
| **Staging** | Hold validated records for one attempt, isolating incomplete work from the final table. |
| **Transformation** | Normalize timestamps to UTC, map fields, calculate a business-content hash, and select one record per source ID. |
| **Warehouse** | Maintain the latest known state of each article with constraints and idempotent upserts. |
| **Analytics** | Demonstrate useful queries, such as articles per publication day and articles by author. Counts reflect the scoped source, not all news. |

**Storage durability and capacity:** MongoDB still consumes disk and does not by itself prevent data growth or provide distributed big-data scalability. Its Docker volume and the PostgreSQL volume survive container recreation, not host loss. Apply the retention policy below, monitor available disk, and document backup and restore; cloud storage, automated backups, and MongoDB replication or sharding are future work.

### MongoDB landing design

- Use a dedicated database with `raw_articles` and `raw_pages` collections. Store one article per document, preserving all source fields in a nested payload; store the response envelope without its article array in the page manifest. Together these retain the source JSON values and page structure, but not exact JSON bytes or formatting.
- Give each article a unique key based on batch, attempt, page, and record position. A page manifest records the same identity, source, requested window, page number, retrieval time, expected record count, and a complete status. Index the unique identities and batch/attempt lookup fields.
- Persist all article documents first; publish the complete page manifest only after every write succeeds. Verify the count when reading, and consume only complete, consistent pages. Incomplete writes are ignored and visible for diagnosis. An empty page has a complete manifest with count zero.
- MongoDB has a **16 MiB BSON document limit**. Keeping articles separate avoids a page-sized document, but a single oversized article or envelope must fail visibly; never truncate it. GridFS or object storage can be evaluated later if oversized records arise.
- For development, keep raw data for **seven days after an attempt completes or is declared abandoned**. During a scheduled run, while holding the pipeline lock, explicitly purge documents for eligible attempts using their batch/attempt metadata and deleting article documents before page manifests. Never purge the active attempt. A later cleanup retries any interrupted deletion; readers verify page counts and reject incomplete pages. Log counts and surface cleanup failures; cleanup must not alter a pipeline checkpoint. Do not rely on independent TTL deletion across collections, which could leave partial pages.
- Raw replay is available only within the retention window. After expiration, recovery re-fetches from the source if still available; old source state cannot necessarily be reconstructed. MongoDB capacity can still grow during a prolonged outage or sustained high ingestion, so set a disk-space alert or manual check and narrow the source scope if needed.

---

## 5. Data model

### `warehouse.articles`

| Column | Type / nullability | Meaning |
|---|---|---|
| `id` | text, primary key | Source-qualified ID, e.g. `guardian:<source-id>` |
| `title` | text, not null | Guardian headline |
| `content` | text, nullable | Source-provided article body, retained in its supplied format, typically HTML; do not scrape missing bodies |
| `published_at` | timestamp with time zone, not null | Source publication time, normalized to UTC |
| `author_id` | text, nullable | First available stable contributor tag ID; null if unavailable. Multiple contributors are not modeled in MVP |
| `created_at` | timestamp with time zone, not null | First insertion into this warehouse—not an invented source creation time |
| `updated_at` | timestamp with time zone, not null | Last time the warehouse's business fields changed |
| `deleted_at` | timestamp with time zone, nullable | Time of an explicitly confirmed deletion; normally null in MVP |

### Additional metadata

| Column | Priority | Purpose |
|---|---|---|
| `source` | **MVP** | Identifies the provider. |
| `source_modified_at` | **MVP** | Source last-modified timestamp when supplied; supports ordering changes. |
| `ingestion_timestamp` | **MVP** | Observation time associated with the latest accepted business change. |
| `batch_id` | **MVP** | Identifies the batch responsible for the latest accepted business change. |
| `checksum` | **MVP** | Hash of canonical normalized business fields; detects meaningful changes. |
| `load_timestamp` | Future | Separate load timestamp if operational needs justify it; `created_at`, `updated_at`, and run metadata are enough initially. |
| `is_deleted` | Future, if needed | Redundant with `deleted_at`; avoid maintaining two deletion indicators in MVP. |
| `version` | Future | Add with a historical/versioned model, not to simulate history in a current-state table. |

### Modeling rules

- **Primary key:** source-qualified `id`. The source identifier itself is preserved within that value.
- **Foreign keys:** none in MVP. `author_id` is an optional source identifier, not a foreign key to an authors table.
- **Indexes:** primary-key index plus an index on `published_at`. Add others only when observed queries need them.
- **Deduplication:** choose one staged record per `id` using source modification time; use a deterministic tie-breaker for equal timestamps. Count duplicates.
- **Updates:** use a conditional upsert. A newer source record with changed business fields updates the row, hash, and `updated_at`; identical replays do not alter business fields or timestamps. A newer source modification timestamp with unchanged business fields may update `source_modified_at` alone.
- **Older records:** do not let an older known `source_modified_at` overwrite a newer one. Document ambiguity when timestamps are missing or equal.
- **Soft deletes:** never infer deletion from absence in a partial API response. Only set `deleted_at` on a reliable explicit deletion signal. The MVP does not promise deletion detection.
- **History:** this is a current-state table, not a complete audit trail. MongoDB raw documents provide limited, retention-bound replay evidence; historical modeling is future work.

---

## 6. Incremental ingestion and recovery

### Window strategy

1. Run hourly in **UTC**, using a persisted **last successfully committed window end**.
2. On the first run, start with a small configurable lookback, such as the preceding 24 hours.
3. For each run, capture a fixed end time and query from the previous successful end **minus a 24-hour overlap** through that end. Use the source's last-modified date filter and request its modification timestamp.
4. If the API only accepts calendar-day boundaries for the selected filter, request all intersecting UTC dates and filter records locally to the intended window. Verify date-boundary semantics during the first source spike.
5. Paginate every matching date/window fully. Respect the 1-request/second limit and track daily request consumption. If pagination, quota, or source responses prevent completion, **fail the run without advancing the checkpoint**.
6. On success, commit the warehouse changes and checkpoint together.

The overlap catches many delayed indexing and recent edits, but **does not prove completeness**: API pagination may shift while records change, and late changes outside the overlap or disappeared records can be missed. Document this limitation and use a longer reconciliation process as future work.

**Quota planning:** 500 requests/day averages about 20 per hourly run, including overlap, pagination, retries, and any backfill. Measure actual section volume early; reserve headroom for retries. If the scope cannot be fetched within budget, narrow it and state the scope explicitly rather than silently truncating results.

### Idempotency, concurrency, and restart

- Assign each logical run a `batch_id` and each retry a separate attempt identifier. Persist raw article documents before marking their page manifest complete; incomplete attempts remain identifiable for diagnosis and retention cleanup.
- Use the stable article key and conditional upsert to make replays safe.
- Serialize pipeline execution with a PostgreSQL advisory lock held for the run on the same live database session used to coordinate commits. If that session is lost, abort the attempt; do not continue without reacquiring the lock.
- MongoDB and PostgreSQL do not share a transaction. Complete and verify MongoDB landing first, then write final PostgreSQL upserts, success metadata, and checkpoint advancement in **one PostgreSQL transaction**. If the load fails after landing, the checkpoint stays unchanged and retry or replay safely processes the retained raw data or re-fetches it.
- Use bounded retries with exponential backoff and jitter for transient network errors, `429`, and appropriate `5xx` responses; honor `Retry-After` when supplied. Do not retry invalid credentials or malformed data indefinitely.
- After downtime, the next successful run starts from the persisted checkpoint and processes bounded windows sequentially until caught up. If the backlog exceeds quota, fail visibly and resume later; never silently skip the gap.
- Provide a documented manual replay for a chosen window. Replays use the same lock and upsert rules and must not move the normal checkpoint backward.

---

## 7. Pipeline flow

1. **Start:** Dagster triggers the hourly job; validate configuration and acquire the single-run lock.
2. **Determine window:** Read the successful checkpoint, apply overlap, and calculate fixed UTC bounds.
3. **Fetch:** Request the scoped articles, required fields and contributor tags; paginate completely while respecting quota.
4. **Land:** Store each successful page's article payloads and response envelope in MongoDB; publish its manifest only once every document is persisted.
5. **Validate:** Read complete MongoDB pages, check response status, page counts and pagination completeness, then validate required article identifiers, titles, and timestamps. Missing optional content or author is acceptable.
6. **Stage:** Load valid records into batch-scoped PostgreSQL staging. On invalid required data, retain raw documents, report the failure, and do not advance the checkpoint.
7. **Transform:** Normalize UTC timestamps, map fields, calculate hashes, and deterministically deduplicate.
8. **Load:** In one transaction, conditionally upsert the final table.
9. **Quality checks:** Before commit, verify required fields, unique IDs, expected batch counts, and timestamp validity. An empty successful source window is valid; distinguish it from a failed fetch.
10. **Commit:** Record metrics and advance the checkpoint in the same transaction as the load.
11. **Finish:** Emit a structured success or failure log, release the lock, and notify on failure.

Do not log API keys, full article bodies, or request URLs containing secrets.

---

## 8. Project structure

```text
articles-etl/
├── pyproject.toml               # uv dependencies and tool configuration
├── uv.lock
├── compose.yaml
├── .env.example                 # placeholders only
├── .gitignore
├── README.md
├── Dockerfile
├── docker/
│   ├── dagster.yaml             # persistent Dagster instance configuration
│   └── workspace.yaml
├── src/articles_etl/
│   ├── orchestration/           # Dagster definitions, schedule, job
│   ├── ingestion/               # Guardian client, pagination, MongoDB landing
│   ├── transformation/          # normalization and deduplication
│   ├── warehouse/               # database access, checkpoint and load
│   ├── models/                  # source and normalized contracts
│   ├── configuration/           # validated settings
│   └── logging/                 # structured logging setup
├── sql/
│   ├── schema.sql
│   └── analytics.sql
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/                # small synthetic payloads
├── docs/
│   └── operations.md
└── .github/workflows/
    └── ci.yaml
```

**Container setup:** Run MongoDB, PostgreSQL, Dagster webserver, and Dagster daemon with Compose. Give the Dagster services the same application image and configuration; use PostgreSQL-backed Dagster metadata. Use separate named volumes for MongoDB and PostgreSQL, health checks and authenticated database connections with secrets from the environment, and avoid exposing database ports publicly. Enable the hourly schedule and verify the daemon is running. Do not commit credentials or database data.

---

## 9. Testing plan

| Priority | Test | Expected result |
|---|---|---|
| **MVP unit** | Mapping, UTC conversion, nullable author/content, deterministic hash | Correct normalized output; malformed required fields fail visibly |
| **MVP unit** | Pagination and retry behavior with mocked HTTP | All pages requested; retryable failures retry within bounds; permanent failures stop |
| **MVP integration** | MongoDB page landing, incomplete writes, and retention | Complete pages reconstruct correctly; incomplete or oversized pages fail visibly; cleanup excludes active and recent attempts |
| **MVP integration** | Load the same batch twice into PostgreSQL | One row per ID; unchanged data does not alter business timestamps |
| **MVP integration** | Changed record, stale record, and failure after MongoDB landing or during PostgreSQL load | Newer changes apply; older data does not overwrite; failure leaves checkpoint unchanged and replay recovers |
| **MVP data quality** | Required fields, uniqueness, valid timestamps, staged/load counts | Violations fail the run; an honestly empty batch succeeds |
| **MVP end-to-end** | Dagster job against mocked source and real MongoDB and PostgreSQL | Raw documents, final row, success metadata, and checkpoint exist; failure and replay recover |
| Future | Load tests, property-based tests, contract tests against live API, disaster recovery exercises | Add when source count or operational requirements grow |

CI should use synthetic fixtures, not a live API key. Separately perform one manual live-source smoke test and observe at least one actual scheduled hourly run.

---

## 10. Monitoring and operations

### Must have

- **Structured logs:** timestamp, level, run/attempt ID, window, stage, duration, record counts, retry count, and error category.
- **Run metrics:** fetched, validated, rejected, deduplicated, inserted, updated, unchanged, request count, duration, checkpoint, latest successful completion, and MongoDB cleanup counts. Store summary in PostgreSQL and expose it in Dagster logs/UI.
- **Failure notification:** send a concise message to a configured Slack-compatible webhook after a final job failure. Keep the webhook secret out of logs and Git. Test with a mock webhook; document setup and verify a real notification if credentials are available. Notification failure must itself be logged and must not mark the pipeline successful.
- **Operational checks:** Dagster schedule enabled, daemon alive, MongoDB and PostgreSQL healthy, disk space monitored manually, and last successful run visible.
- **Runbook:** restart services, inspect failed runs and MongoDB raw documents, replay a window within retention, deal with quota exhaustion, and back up/restore both database volumes.

### Future

Prometheus/Grafana, centralized logs, freshness alerts, dead-man monitoring for a stopped host or daemon, automated backups, and service-level objectives.

---

## 11. Portfolio documentation

Keep documentation focused:

1. **README:** objective, architecture diagram, technology choices, prerequisites, setup with uv and Compose, environment variables, how to start the schedule, run tests, inspect MongoDB landing, and query the warehouse.
2. **Operations guide:** restart/replay, seven-day raw retention, checkpoint behavior, failure handling, backup/restore for both databases, and known limitations.
3. **Evidence:** screenshots of a successful scheduled run and sample aggregate query results. Avoid screenshots containing secrets or restricted article text.
4. **Source and licensing notes:** scope, quota, attribution and permitted usage, plus the fact that missing deletes and very late changes are not guaranteed to be captured.

---

## 12. 36-hour schedule

This is **36 hours elapsed**, including six hours of rest and two hours of contingency. The 28 working hours assume one experienced engineer and no major API registration delays. MongoDB setup, retention, and integration testing fit within the relevant phases but add risk; use the contingency if needed.

| Elapsed time | Phase | Work | Definition of done |
|---|---|---|---|
| Hours 0–2 | Source spike | Obtain key; verify terms, article/body fields, contributor IDs, date filters, pagination, and actual section volume | A small live sample confirms the chosen scope and quota budget; otherwise switch to documented fallback |
| Hours 2–5 | Foundation | Initialize uv project, Compose, MongoDB, PostgreSQL, Dagster services, configuration and logging | Services start; authenticated databases, persistent volumes and Dagster UI work |
| Hours 5–10 | Ingestion | Implement client, bounded retries, windowing, pagination, quota tracking, MongoDB raw landing and retention | A complete scoped window lands raw documents; incomplete fetch or landing fails visibly |
| Hours 10–14 | Warehouse | Create schema, staging, run metadata, checkpoint, lock, and transactional upsert | Replay is idempotent; rollback leaves checkpoint unchanged |
| Hours 14–18 | Transformation and quality | Mapping, validation, deduplication, hash, SQL quality checks | Synthetic valid, duplicate, empty, and invalid cases behave as specified |
| Hours 18–24 | Rest | Six-hour break | — |
| Hours 24–28 | Orchestration and recovery | Connect Dagster stages, enable hourly schedule, retries, recovery, and failure webhook | Manual success/failure/replay works; schedule is enabled |
| Hours 28–31 | Testing and CI | Highest-value unit, integration, and end-to-end tests; GitHub Actions | Targeted suite and CI pass without live credentials |
| Hours 31–33 | Documentation | README, operations guide, analytics queries, licensing and limitation notes | A reviewer can reproduce the project from the documented steps |
| Hours 33–34 | Final verification | Live smoke test; check an actual scheduled run and notification setup | Warehouse is queryable; run metadata and checkpoint agree; scheduled execution is observed |
| Hours 34–36 | Contingency | Resolve source, infrastructure, or test problems | MVP acceptance criteria below met, or remaining limitation explicitly reported |

If contingency is exhausted, cut optional analytics polish—not recovery, correctness, or tests.

## 13. MVP acceptance criteria

- The stack starts from documented instructions using **uv and Docker Compose**.
- A configured hourly Dagster schedule executes automatically while the host is running.
- A complete source window produces persistent MongoDB raw documents and queryable warehouse rows.
- Raw data is retained for seven days and cleanup does not remove active attempts; the MongoDB and PostgreSQL volumes persist across container recreation.
- Replaying the same data does not create duplicates or change unchanged business timestamps.
- A failed fetch, validation, or load does not advance the checkpoint.
- After restart or downtime, the pipeline resumes from the successful checkpoint without silently skipping a gap.
- Required quality checks and focused tests pass; CI runs without source credentials.
- Logs, run metrics, and failure-notification configuration make failures visible.
- Documentation states source permissions, quota, operating steps, and the limitations of local durability and source completeness.