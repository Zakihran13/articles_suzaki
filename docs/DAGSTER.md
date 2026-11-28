# Dagster orchestration

The code location is `articles_suzaki.orchestration.definitions`. Its small,
reusable ops call the existing ingestion and transformation modules through a
`PipelineResource`; the jobs supply the ordering and run configuration.
Ops are used rather than assets because these are imperative, manually
parameterized operations over a shared database, not independently
materialized, partitioned datasets. No sensor is needed for these flows.

```
orchestration/
  config.py       # launch schemas and inclusive UTC date windows
  resources.py    # pipeline adapters and ingestion tuning
  ops.py          # window, metadata, articles, validation, transformation
  jobs.py         # four jobs and config mappings
  schedules.py    # optional, stopped daily schedule
  definitions.py  # code location
```

## Run locally

1. Copy `.env.example` to `.env` if needed and configure MongoDB, PostgreSQL,
   collection names, and a Guardian API key for `api`. The database credentials
   are read by the existing application settings; don't put secrets in run config.
2. Install with `uv sync`, then run `uv run dagster dev`. The code location is
   selected automatically by `pyproject.toml`.
3. Open `http://127.0.0.1:3000` and launch a job from its Launchpad. All jobs
   are manual initially. Set `DAGSTER_HOME` to a persistent directory if local
   run history must survive restarts.

Dagster CLI commands load the current working directory's `.env` and may
override same-named shell variables. Check the effective database and mock
settings before launching; do not rely on shell overrides to redirect a run
while a conflicting `.env` is present.

For `api` runs, set `GUARDIAN_OPEN_PLATFORM_API` to a valid Guardian API key.
An empty value fails before ingestion starts; a nonempty invalid or expired key
can still result in HTTP 401. Restart Dagster after changing the setting. For
Compose, recreate the webserver and daemon containers
so they receive the updated environment. API error logs can contain the key in
the request URL, so redact them before sharing. Mock runs do not require a key.

Alternatively, `docker compose up --build` runs the webserver, daemon, MongoDB,
and PostgreSQL; the UI is at `http://localhost:3010`. Compose reads `.env`,
overrides the database hosts to its service names, and persists Dagster history
and database data in named volumes. It uses fixed container names and published
ports, so check for conflicts before starting it. Its unauthenticated MongoDB,
published ports, local SQLite Dagster storage, and single-host setup are for
development/single-node deployment, not an internet-facing production service.
For production, secure the UI and databases, manage secrets externally, use
durable supported Dagster instance storage and backups, and configure a suitable
run launcher, monitoring, and infrastructure for the target deployment.

## Launch configurations

Enter these YAML values at the root of the Launchpad run configuration. Dates
are required ISO `YYYY-MM-DD` strings; both ends are inclusive and UTC.
`source` is `api` or `mock`, defaulting to `api`.

`daily_full_pipeline` (today's UTC publication date, captured when the run
starts):

```yaml
source: mock
```

`historical_full_pipeline` and `ingestion_only_pipeline`:

```yaml
from_date: "2024-01-01"
to_date: "2024-01-31"
source: api
```

`transformation_only_pipeline`:

```yaml
from_date: "2024-01-01"
to_date: "2024-01-31"
```

The same YAML can be passed to `dagster job execute -m
articles_suzaki.orchestration.definitions -j JOB_NAME -c path/to/config.yaml`.
The daily job also accepts an empty configuration. Transformation converts
the inclusive end date to the following midnight, because the existing
transformer uses a half-open interval. It reads all matching raw articles,
regardless of their ingestion source; transformation-only does not ingest.

## Schedule and operations

`daily_full_pipeline_schedule` is stopped by default. It runs at 00:00 UTC
using `api` if explicitly enabled in the UI. It processes the UTC date when
the run starts, **not the previous completed day**; at midnight this is a
partial day, and a queued run starting after midnight uses the later date.
Choose an appropriate cron and window policy before enabling it. The daemon
must be running. Other jobs have no schedule.

The metadata search requests the entire inclusive range and all API pages;
article ingestion then fetches the pending records scoped to that range and
source. Validation fails if any of those records remain pending. A source
error, missing content, or invalid required article fields fails the step and
blocks downstream transformation. A range with no results is successful.
Existing transformation validation still logs and skips invalid pre-existing
raw records. The API client has its own request-rate and per-client request
limit; account-level quotas, concurrent clients, and historical backfills
must be managed operationally.

Article ingestion, validation, and transformation retry twice with exponential
backoff. Metadata ingestion deliberately does not retry automatically:
mock generation uses random IDs, so a retry could create additional records.
API metadata and article writes are upserts, and metadata refreshes preserve
completed article checkpoints. A manually repeated mock run creates new
synthetic articles. Failed transformation attempts can leave staging rows
under their Dagster run ID; a step retry reuses that ID, and successful
transformation cleans those rows. Monitor and clean abandoned staging batches
according to the deployment's retention policy.

Compose serializes Dagster runs (`max_concurrent_runs: 1`) because the existing
pipeline uses shared Mongo collections and staging/warehouse tables. Maintain
equivalent serialization for any other deployment and avoid simultaneous
out-of-band pipeline writes. Per-run ingestion batching and request rate can
be adjusted in the `PipelineResource` configured in `definitions.py`.
Transformation batch settings and mock volume remain in `.env`; the example
mock volume is one million records, so lower it for initial testing.

Full historical transformations accumulate normalized records in memory before
deduplication, as in the existing transformer; split very large ranges into
manageable runs. The orchestration does not implement a historical partition
backfill service or change the transformer's data model.
