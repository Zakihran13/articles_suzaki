"""Persist Dagster pipeline execution state in PostgreSQL."""

import asyncio
from contextlib import contextmanager
from datetime import date
from typing import Any

from dagster import OpExecutionContext
from loguru import logger
from sqlalchemy import text

from articles_suzaki.logging.flows import flow_logging
from articles_suzaki.orchestration.config import transformation_bounds
from articles_suzaki.transformation.staging import initialize_schema
from articles_suzaki.warehouse.client import init_async_db


async def _write_run(
    context: OpExecutionContext,
    start: date,
    end: date,
    source: str,
    status: str,
    error: str | None = None,
    metrics: dict[str, Any] | None = None,
    initialize: bool = False,
) -> None:
    engine = init_async_db()
    try:
        if initialize:
            await initialize_schema(engine)
        window_start, window_end = transformation_bounds(start, end)
        values = {
            "batch_id": f"dagster_{context.run_id}",
            "flow_name": context.job_name,
            "source": source or "all",
            "window_start": window_start,
            "window_end": window_end,
            "status": status,
            "error_message": error,
            "retry_count": context.retry_number,
            "inserted_count": (metrics or {}).get("inserted"),
            "updated_count": (metrics or {}).get("updated"),
            "unchanged_count": (metrics or {}).get("unchanged"),
        }
        async with engine.begin() as conn:
            await conn.execute(text("""
                INSERT INTO pipeline.runs
                    (batch_id, flow_name, source, window_start, window_end, status,
                     retry_count, error_message, inserted_count, updated_count,
                     unchanged_count, completed_at, duration_ms)
                VALUES
                    (:batch_id, :flow_name, :source, :window_start, :window_end, :status,
                     :retry_count, :error_message, :inserted_count, :updated_count,
                     :unchanged_count,
                     CASE WHEN :status = 'started' THEN NULL ELSE NOW() END,
                     CASE WHEN :status = 'started' THEN NULL ELSE 0 END)
                ON CONFLICT (batch_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    retry_count = GREATEST(pipeline.runs.retry_count, EXCLUDED.retry_count),
                    error_message = EXCLUDED.error_message,
                    inserted_count = COALESCE(EXCLUDED.inserted_count, pipeline.runs.inserted_count),
                    updated_count = COALESCE(EXCLUDED.updated_count, pipeline.runs.updated_count),
                    unchanged_count = COALESCE(EXCLUDED.unchanged_count, pipeline.runs.unchanged_count),
                    completed_at = EXCLUDED.completed_at,
                    duration_ms = CASE WHEN EXCLUDED.status = 'started' THEN NULL
                        ELSE (EXTRACT(EPOCH FROM (NOW() - pipeline.runs.started_at)) * 1000)::int
                    END
            """), values)
    finally:
        await engine.dispose()


def start_run(context: OpExecutionContext, start: date, end: date, source: str) -> None:
    logger.info("Starting pipeline window {} through {} (source={})", start, end, source or "all")
    asyncio.run(_write_run(context, start, end, source, "started", initialize=True))


@contextmanager
def tracked_stage(
    context: OpExecutionContext, start: date, end: date, source: str, flow: str,
    final: bool = False,
):
    with flow_logging(flow, run_id=context.run_id, job=context.job_name, attempt=context.retry_number):
        asyncio.run(_write_run(context, start, end, source, "started"))
        metrics: dict[str, Any] = {}
        try:
            yield metrics
        except BaseException as exc:
            asyncio.run(_write_run(context, start, end, source, "failed", error=str(exc)))
            raise
        else:
            if final:
                logger.info("Pipeline completed with metrics: {}", metrics)
                asyncio.run(_write_run(context, start, end, source, "completed", metrics=metrics))
