"""Runtime tuning and adapters to the existing pipeline."""

import asyncio
from datetime import date
from typing import Any

from dagster import ConfigurableResource
from pydantic import Field

from articles_suzaki.configuration.config import config
from articles_suzaki.ingestion import ingestion_metadata, ingestion_news
from articles_suzaki.transformation.staging import initialize_schema, verify_tables_exist
from articles_suzaki.transformation.transform import transform_and_stage
from articles_suzaki.warehouse.client import (
    AsyncMongoDBConnection,
    get_async_mongodb,
    init_async_db,
)
from articles_suzaki.warehouse.statements import get_news_ids
from articles_suzaki.orchestration.config import transformation_bounds


def _validate_api_key(source: str) -> None:
    """Reject API runs with no credentials before performing any I/O."""
    if source == "api" and not config.guardian_api_key.strip():
        raise ValueError(
            "GUARDIAN_OPEN_PLATFORM_API is empty. Set it to a valid Guardian "
            "API key in .env or the Dagster process environment, then restart "
            "the Dagster services."
        )


class PipelineResource(ConfigurableResource):
    """Control bounded ingestion concurrency and transform batch sizes."""

    requests_per_second: int = Field(default=1, gt=0)
    ingestion_batch_size: int = Field(default=100, gt=0)
    max_concurrent_batches: int = Field(default=3, gt=0)

    def ingest_metadata(self, start: date, end: date, source: str) -> None:
        """Fetch all metadata pages for the inclusive range."""
        _validate_api_key(source)

        async def run() -> None:
            try:
                await ingestion_metadata.main(
                    from_date=start, to_date=end, source_type=source,
                    api_key=config.guardian_api_key,
                    requests_per_second=self.requests_per_second,
                    batch_size=self.ingestion_batch_size,
                    max_concurrent_batches=self.max_concurrent_batches,
                    max_roll=None, tag_source=True,
                )
            finally:
                AsyncMongoDBConnection.close()

        asyncio.run(run())

    def ingest_news(self, start: date, end: date, source: str) -> None:
        """Fetch pending articles for the same source and range, failing on errors."""
        _validate_api_key(source)

        async def run() -> None:
            try:
                await ingestion_news.main(
                    from_date=start, to_date=end, source_type=source, strict=True,
                    api_key=config.guardian_api_key,
                    requests_per_second=self.requests_per_second,
                    batch_size=self.ingestion_batch_size,
                    max_concurrent_batches=self.max_concurrent_batches,
                )
            finally:
                AsyncMongoDBConnection.close()

        asyncio.run(run())

    def validate_ingestion(self, start: date, end: date, source: str) -> None:
        """Fail if any scoped metadata remains unprocessed."""
        async def run() -> None:
            try:
                db = get_async_mongodb(config.mongo_db_name)
                pending = await get_news_ids(
                    db[config.mongo_coll_metadata], start, end, source
                )
                if pending:
                    raise RuntimeError(f"{len(pending)} articles remain unprocessed")
            finally:
                AsyncMongoDBConnection.close()

        asyncio.run(run())

    def transform(self, start: date, end: date, batch_id: str) -> dict[str, Any]:
        """Run the existing transformer over an inclusive date range."""
        async def run() -> dict[str, Any]:
            engine = init_async_db()
            try:
                db = get_async_mongodb(config.mongo_db_name)
                await initialize_schema(engine)
                if not await verify_tables_exist(engine):
                    raise RuntimeError("Schema initialization failed: required tables missing")
                start_at, end_at = transformation_bounds(start, end)
                return await transform_and_stage(
                    db, engine, batch_id, from_date=start_at, to_date=end_at,
                    extract_batch_size=config.transform_extract_batch_size,
                    minibatch_size=config.transform_minibatch_size,
                )
            finally:
                await engine.dispose()
                AsyncMongoDBConnection.close()

        return asyncio.run(run())
