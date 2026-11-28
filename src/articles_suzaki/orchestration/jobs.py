"""Jobs and their user-facing Dagster configuration mappings."""

from dagster import config_mapping, job

from articles_suzaki.orchestration.config import DailyConfig, DateRangeConfig, IngestionConfig
from articles_suzaki.orchestration.ops import (
    daily_window, ingest_metadata, ingest_news, ingestion_window,
    transform, transformation_window, validate_ingestion,
)


@config_mapping
def daily_config(config: DailyConfig) -> dict:
    """Map daily launch parameters to the window operation."""
    return {"ops": {"daily_window": {"config": config.model_dump()}}}


@config_mapping
def ingestion_config(config: IngestionConfig) -> dict:
    """Map ingestion parameters to the shared window operation."""
    return {"ops": {"ingestion_window": {"config": config.model_dump()}}}


@config_mapping
def transform_config(config: DateRangeConfig) -> dict:
    """Map transformation parameters to the window operation."""
    return {"ops": {"transformation_window": {"config": config.model_dump()}}}


@job(config=daily_config)
def daily_full_pipeline() -> None:
    """Ingest and transform today's UTC publication window."""
    transform(validate_ingestion(ingest_news(ingest_metadata(daily_window()))))


@job(config=ingestion_config)
def historical_full_pipeline() -> None:
    """Ingest and transform all articles in an inclusive date range."""
    transform(validate_ingestion(ingest_news(ingest_metadata(ingestion_window()))))


@job(config=ingestion_config)
def ingestion_only_pipeline() -> None:
    """Ingest and validate without transforming."""
    validate_ingestion(ingest_news(ingest_metadata(ingestion_window())))


@job(config=transform_config)
def transformation_only_pipeline() -> None:
    """Transform previously ingested raw articles in an inclusive date range."""
    transform(transformation_window())
