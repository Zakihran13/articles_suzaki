"""Tests for structured flow logs and PostgreSQL run records."""

import asyncio
from datetime import date, datetime, timezone
import json
import os
from types import SimpleNamespace

import pytest
from loguru import logger
from sqlalchemy import text

from articles_suzaki.logging import runs
from articles_suzaki.logging.flows import flow_logging


@pytest.mark.asyncio
async def test_flow_logs_are_json_and_keep_concurrent_contexts(tmp_path, monkeypatch):
    monkeypatch.setenv("ARTICLES_LOG_DIR", str(tmp_path))

    async def emit(flow):
        with flow_logging(flow, run_id=flow):
            await asyncio.sleep(0)
            logger.info("message {}", flow)

    await asyncio.gather(emit("ingestion_metadata"), emit("ingestion_news"))
    day = datetime.now(timezone.utc).date().isoformat()
    for flow in ("ingestion_metadata", "ingestion_news"):
        records = [
            json.loads(line)["record"]
            for line in (tmp_path / f"{flow}_{day}.log").read_text().splitlines()
        ]
        assert [record["message"] for record in records] == [
            "Flow started", f"message {flow}", "Flow completed",
        ]
        assert all(record["extra"]["run_id"] == flow for record in records)


def test_failed_flow_is_logged_and_reraised(tmp_path, monkeypatch):
    monkeypatch.setenv("ARTICLES_LOG_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="bad input"):
        with flow_logging("transformation"):
            raise ValueError("bad input")
    path = tmp_path / f"transformation_{datetime.now(timezone.utc).date()}.log"
    records = [json.loads(line)["record"] for line in path.read_text().splitlines()]
    assert records[-1]["message"] == "Flow failed"
    assert records[-1]["exception"]["type"] == "ValueError"


@pytest.mark.asyncio
async def test_run_write_records_window_and_metrics(monkeypatch):
    executed = []

    class Engine:
        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, statement, params):
            executed.append((str(statement), params))

        async def dispose(self):
            pass

    monkeypatch.setattr(runs, "init_async_db", Engine)
    context = SimpleNamespace(run_id="123", job_name="historical_full_pipeline", retry_number=1)
    await runs._write_run(
        context, date(2024, 1, 1), date(2024, 1, 2), "mock", "completed",
        metrics={"inserted": 2, "updated": 1, "unchanged": 3},
    )
    statement, values = executed[0]
    assert "INSERT INTO pipeline.runs" in statement
    assert values["batch_id"] == "dagster_123"
    assert values["flow_name"] == "historical_full_pipeline"
    assert values["window_start"] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert values["window_end"] == datetime(2024, 1, 3, tzinfo=timezone.utc)
    assert (values["inserted_count"], values["updated_count"], values["unchanged_count"]) == (2, 1, 3)


@pytest.mark.asyncio
@pytest.mark.skipif(not os.environ.get("ARTICLES_TEST_POSTGRES"), reason="requires PostgreSQL")
async def test_run_transitions_against_postgres(monkeypatch):
    real_engine = runs.init_async_db()
    context = SimpleNamespace(run_id="test", job_name="historical_full_pipeline", retry_number=0)

    async with real_engine.connect() as connection:
        await connection.execute(text("""
            CREATE TEMP TABLE runs (
                batch_id TEXT PRIMARY KEY, flow_name TEXT, source TEXT,
                window_start TIMESTAMPTZ, window_end TIMESTAMPTZ, status TEXT,
                retry_count INT, error_message TEXT, inserted_count INT,
                updated_count INT, unchanged_count INT, completed_at TIMESTAMPTZ,
                duration_ms INT, started_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        class TempEngine:
            def begin(self):
                return self

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def execute(self, statement, params):
                await connection.execute(text(str(statement).replace("pipeline.runs", "pg_temp.runs")), params)

            async def dispose(self):
                pass

        monkeypatch.setattr(runs, "init_async_db", TempEngine)
        start, end = date(2024, 1, 1), date(2024, 1, 2)
        await runs._write_run(context, start, end, "mock", "started")
        await runs._write_run(context, start, end, "mock", "failed", error="failed")
        failed = (await connection.execute(text("SELECT status, error_message, completed_at FROM pg_temp.runs"))).one()
        assert failed.status == "failed" and failed.error_message == "failed" and failed.completed_at
        context.retry_number = 1
        await runs._write_run(context, start, end, "mock", "started")
        await runs._write_run(context, start, end, "mock", "completed", metrics={"inserted": 4})
        completed = (await connection.execute(text(
            "SELECT status, retry_count, error_message, inserted_count FROM pg_temp.runs"
        ))).one()
        assert tuple(completed) == ("completed", 1, None, 4)
    await real_engine.dispose()
