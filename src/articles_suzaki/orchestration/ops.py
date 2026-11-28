"""Reusable Dagster operations for the pipeline."""

from datetime import datetime, timezone
from dataclasses import dataclass

from dagster import Backoff, OpExecutionContext, RetryPolicy, op

from articles_suzaki.logging.flows import flow_logging
from articles_suzaki.logging.runs import start_run, tracked_stage
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
    with flow_logging("orchestration", run_id=context.run_id, job=context.job_name):
        today = datetime.now(timezone.utc).date()
        start_run(context, today, today, config.source)
        context.log.info("Daily UTC window: %s", today)
        return Window(from_date=str(today), to_date=str(today), source=config.source)


@op
def ingestion_window(context: OpExecutionContext, config: IngestionConfig) -> Window:
    """Validate the requested ingestion window before any writes."""
    with flow_logging("orchestration", run_id=context.run_id, job=context.job_name):
        start, end = parse_window(config.from_date, config.to_date)
        start_run(context, start, end, config.source)
        context.log.info("Ingestion window: %s through %s (%s)", start, end, config.source)
        return Window(from_date=str(start), to_date=str(end), source=config.source)


@op
def transformation_window(context: OpExecutionContext, config: DateRangeConfig) -> Window:
    """Validate the requested transformation window before any writes."""
    with flow_logging("orchestration", run_id=context.run_id, job=context.job_name):
        start, end = parse_window(config.from_date, config.to_date)
        start_run(context, start, end, "")
        context.log.info("Transformation window: %s through %s", start, end)
        return Window(from_date=str(start), to_date=str(end), source="")


@op
def ingest_metadata(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Ingest every metadata page; do not retry non-idempotent mock generation."""
    start, end = parse_window(window.from_date, window.to_date)
    with tracked_stage(context, start, end, window.source, "ingestion_metadata"):
        context.log.info("Starting metadata ingestion")
        pipeline.ingest_metadata(start, end, window.source)
    return window


RETRY_POLICY = RetryPolicy(max_retries=2, delay=10, backoff=Backoff.EXPONENTIAL)


@op(retry_policy=RETRY_POLICY)
def ingest_news(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Fetch article contents; successfully stored articles remain checkpointed."""
    start, end = parse_window(window.from_date, window.to_date)
    with tracked_stage(
        context, start, end, window.source, "ingestion_news",
        final=context.job_name == "news_ingestion_only_pipeline",
    ):
        context.log.info("Starting article ingestion")
        pipeline.ingest_news(start, end, window.source)
    return window


@op(retry_policy=RETRY_POLICY)
def validate_ingestion(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> Window:
    """Verify there are no failed or pending articles before transformation."""
    start, end = parse_window(window.from_date, window.to_date)
    with tracked_stage(
        context, start, end, window.source, "orchestration",
        final=context.job_name == "ingestion_only_pipeline",
    ):
        pipeline.validate_ingestion(start, end, window.source)
        context.log.info("Ingestion validation passed")
    return window


@op(retry_policy=RETRY_POLICY)
def transform(context: OpExecutionContext, pipeline: PipelineResource, window: Window) -> None:
    """Run transformation only after all upstream operations succeed."""
    start, end = parse_window(window.from_date, window.to_date)
    with tracked_stage(context, start, end, window.source, "transformation", final=True) as result:
        metrics = pipeline.transform(start, end, f"dagster_{context.run_id}")
        result.update(metrics)
        context.log.info("Transformation completed: %s", metrics)
