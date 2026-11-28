"""Reusable Dagster operations for the pipeline."""

from datetime import datetime, timezone
from dataclasses import dataclass

from dagster import Backoff, OpExecutionContext, RetryPolicy, op

from articles_suzaki.orchestration.config import (
    DailyConfig, DateRangeConfig, IngestionConfig, parse_window,
)
from articles_suzaki.orchestration.resources import PipelineResource


@dataclass(frozen=True)
class Window:
    """Serializable inclusive date window shared between operations."""

    from_date: str
    to_date: str
    source: str


@op
def daily_window(context: OpExecutionContext, config: DailyConfig) -> Window:
    """Capture the UTC day once for every stage of a daily run."""
    today = datetime.now(timezone.utc).date().isoformat()
    context.log.info("Daily UTC window: %s", today)
    return Window(from_date=today, to_date=today, source=config.source)


@op
def ingestion_window(context: OpExecutionContext, config: IngestionConfig) -> Window:
    """Validate the requested ingestion window before any writes."""
    start, end = parse_window(config.from_date, config.to_date)
    context.log.info("Ingestion window: %s through %s (%s)", start, end, config.source)
    return Window(from_date=str(start), to_date=str(end), source=config.source)


@op
def transformation_window(context: OpExecutionContext, config: DateRangeConfig) -> Window:
    """Validate the requested transformation window before any writes."""
    start, end = parse_window(config.from_date, config.to_date)
    context.log.info("Transformation window: %s through %s", start, end)
    return Window(from_date=str(start), to_date=str(end), source="")


@op
def ingest_metadata(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Ingest every metadata page; do not retry non-idempotent mock generation."""
    start, end = parse_window(window.from_date, window.to_date)
    context.log.info("Starting metadata ingestion")
    pipeline.ingest_metadata(start, end, window.source)
    return window


RETRY_POLICY = RetryPolicy(max_retries=2, delay=10, backoff=Backoff.EXPONENTIAL)


@op(retry_policy=RETRY_POLICY)
def ingest_news(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Fetch article contents; successfully stored articles remain checkpointed."""
    start, end = parse_window(window.from_date, window.to_date)
    context.log.info("Starting article ingestion")
    pipeline.ingest_news(start, end, window.source)
    return window


@op(retry_policy=RETRY_POLICY)
def validate_ingestion(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Verify there are no failed or pending articles before transformation."""
    start, end = parse_window(window.from_date, window.to_date)
    pipeline.validate_ingestion(start, end, window.source)
    context.log.info("Ingestion validation passed")
    return window


@op(retry_policy=RETRY_POLICY)
def transform(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> None:
    """Run transformation only after all upstream operations succeed."""
    start, end = parse_window(window.from_date, window.to_date)
    metrics = pipeline.transform(start, end, f"dagster_{context.run_id}")
    context.log.info("Transformation completed: %s", metrics)
