"""Dagster code location for the articles pipeline."""

from dagster import Definitions

from articles_suzaki.orchestration.jobs import (
    daily_full_pipeline, historical_full_pipeline, ingestion_only_pipeline,
    transformation_only_pipeline,
)
from articles_suzaki.orchestration.resources import PipelineResource
from articles_suzaki.orchestration.schedules import daily_schedule

defs = Definitions(
    jobs=[
        daily_full_pipeline, historical_full_pipeline, ingestion_only_pipeline,
        transformation_only_pipeline,
    ],
    schedules=[daily_schedule],
    resources={"pipeline": PipelineResource()},
)
