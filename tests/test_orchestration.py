"""Focused tests for Dagster orchestration and ingestion boundaries."""

from datetime import date, datetime, timezone

import pytest
from dagster import DefaultScheduleStatus, Definitions, build_schedule_context

from articles_suzaki.ingestion import ingestion_metadata, ingestion_news
from articles_suzaki.orchestration.config import transformation_bounds
from articles_suzaki.orchestration.definitions import defs
from articles_suzaki.orchestration.jobs import (
    daily_full_pipeline, historical_full_pipeline, ingestion_only_pipeline,
    transformation_only_pipeline,
)
from articles_suzaki.orchestration.resources import PipelineResource
from articles_suzaki.orchestration import resources
from articles_suzaki.orchestration.schedules import daily_schedule
from articles_suzaki.warehouse.statements import get_news_ids


@pytest.fixture
def pipeline(monkeypatch):
    """Replace external services while exercising real Dagster execution."""
    calls = []

    def metadata(self, start, end, source):
        calls.append(("metadata", start, end, source))

    def news(self, start, end, source):
        calls.append(("news", start, end, source))

    def validate(self, start, end, source):
        calls.append(("validate", start, end, source))

    def transform(self, start, end, batch_id):
        calls.append(("transform", start, end, batch_id))
        return {"status": "success"}

    monkeypatch.setattr(PipelineResource, "ingest_metadata", metadata)
    monkeypatch.setattr(PipelineResource, "ingest_news", news)
    monkeypatch.setattr(PipelineResource, "validate_ingestion", validate)
    monkeypatch.setattr(PipelineResource, "transform", transform)
    return calls


def test_definitions_and_disabled_schedule():
    Definitions.validate_loadable(defs)
    assert daily_schedule.default_status == DefaultScheduleStatus.STOPPED
    result = daily_schedule.evaluate_tick(build_schedule_context())
    assert result.run_requests[0].run_config == {"source": "api"}


def test_historical_pipeline_runs_in_order_and_includes_end_date(pipeline):
    result = historical_full_pipeline.execute_in_process(
        run_config={"from_date": "2024-01-30", "to_date": "2024-02-02", "source": "mock"},
        resources={"pipeline": PipelineResource()},
    )
    assert result.success
    assert [call[0] for call in pipeline] == ["metadata", "news", "validate", "transform"]
    assert all(call[1:3] == (date(2024, 1, 30), date(2024, 2, 2)) for call in pipeline)
    assert all(call[3] == "mock" for call in pipeline[:3])


def test_ingestion_and_transformation_only_are_independent(pipeline):
    config = {"from_date": "2024-06-01", "to_date": "2024-06-01"}
    assert ingestion_only_pipeline.execute_in_process(
        run_config={**config, "source": "api"}, resources={"pipeline": PipelineResource()}
    ).success
    assert [call[0] for call in pipeline] == ["metadata", "news", "validate"]
    pipeline.clear()
    assert transformation_only_pipeline.execute_in_process(
        run_config=config, resources={"pipeline": PipelineResource()}
    ).success
    assert [call[0] for call in pipeline] == ["transform"]
    assert transformation_bounds(date(2024, 6, 1), date(2024, 6, 1)) == (
        datetime(2024, 6, 1, tzinfo=timezone.utc),
        datetime(2024, 6, 2, tzinfo=timezone.utc),
    )


def test_daily_captures_one_utc_day(pipeline):
    before = datetime.now(timezone.utc).date()
    assert daily_full_pipeline.execute_in_process(
        run_config={"source": "mock"}, resources={"pipeline": PipelineResource()}
    ).success
    after = datetime.now(timezone.utc).date()
    assert all(call[1] == call[2] and before <= call[1] <= after for call in pipeline)
    assert len({call[1] for call in pipeline}) == 1


@pytest.mark.parametrize("start,end", [
    ("2024-02-02", "2024-02-01"),
    ("not-a-date", "2024-02-01"),
    ("2024-01-01", "9999-12-31"),
])
def test_invalid_dates_fail_before_ingestion(pipeline, start, end):
    result = historical_full_pipeline.execute_in_process(
        run_config={"from_date": start, "to_date": end, "source": "api"},
        resources={"pipeline": PipelineResource()}, raise_on_error=False,
    )
    assert not result.success
    assert pipeline == []


def test_ingestion_failure_stops_downstream(monkeypatch, pipeline):
    def fail(self, start, end, source):
        raise RuntimeError("metadata failed")
    monkeypatch.setattr(PipelineResource, "ingest_metadata", fail)
    result = historical_full_pipeline.execute_in_process(
        run_config={"from_date": "2024-01-01", "to_date": "2024-01-02", "source": "api"},
        resources={"pipeline": PipelineResource()}, raise_on_error=False,
    )
    assert not result.success
    assert pipeline == []


@pytest.mark.parametrize("method", ["ingest_metadata", "ingest_news"])
@pytest.mark.parametrize("api_key", ["", "  "])
def test_missing_api_key_fails_before_ingestion(monkeypatch, method, api_key):
    calls = []

    async def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(resources.config, "guardian_api_key", api_key)
    monkeypatch.setattr(ingestion_metadata, "main", fake_main)
    monkeypatch.setattr(ingestion_news, "main", fake_main)
    monkeypatch.setattr(resources.AsyncMongoDBConnection, "close", lambda: None)
    resource = PipelineResource()
    with pytest.raises(ValueError, match="GUARDIAN_OPEN_PLATFORM_API is empty"):
        getattr(resource, method)(date(2024, 1, 1), date(2024, 1, 1), "api")
    assert calls == []


@pytest.mark.parametrize("source,key", [("api", "configured-key"), ("mock", "")])
def test_ingestion_passes_current_api_key_without_requiring_it_for_mock(
    monkeypatch, source, key
):
    calls = []

    async def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(resources.config, "guardian_api_key", key)
    monkeypatch.setattr(ingestion_metadata, "main", fake_main)
    monkeypatch.setattr(resources.AsyncMongoDBConnection, "close", lambda: None)
    PipelineResource().ingest_metadata(date(2024, 1, 1), date(2024, 1, 1), source)
    assert calls[0]["api_key"] == key
    assert calls[0]["source_type"] == source


@pytest.mark.asyncio
async def test_strict_article_failures_propagate():
    class Source:
        async def query(self, news_id):
            raise RuntimeError("fetch failed")

    with pytest.raises(RuntimeError, match="Failed to pull"):
        async for _ in ingestion_news.extract_news_stream(
            Source(), [{"id": "article"}], strict=True
        ):
            pass


@pytest.mark.asyncio
async def test_strict_article_validation_rejects_mismatched_id():
    class Source:
        async def query(self, news_id):
            return {"response": {"content": {
                "id": "different", "webTitle": "title",
                "webPublicationDate": "2024-01-01T00:00:00Z",
            }}}

    with pytest.raises(RuntimeError, match="Invalid content"):
        async for _ in ingestion_news.extract_news_stream(
            Source(), [{"id": "article"}], strict=True
        ):
            pass


@pytest.mark.asyncio
async def test_mock_article_preserves_metadata_publication_date():
    class Source:
        async def query(self, news_id):
            return {"response": {"content": {
                "id": news_id, "webPublicationDate": "2026-01-01T00:00:00Z",
                "webTitle": "A test article",
                "fields": {"firstPublicationDate": "2026-01-01T00:00:00Z"},
            }}}

    metadata_date = "2024-06-30T12:00:00+00:00"
    batches = [
        batch async for batch in ingestion_news.extract_news_stream(
            Source(), [{"id": "article", "webPublicationDate": metadata_date}],
            strict=True, preserve_metadata_date=True,
        )
    ]
    article = batches[0][0][0]
    assert article["webPublicationDate"] == metadata_date
    assert article["fields"]["firstPublicationDate"] == metadata_date


@pytest.mark.asyncio
async def test_metadata_api_stream_does_not_limit_pages():
    class Guardian:
        def __init__(self):
            self.pages = []

        async def search(self, **kwargs):
            self.pages.append(kwargs["page"])
            return {"response": {"results": [{"id": str(kwargs["page"])}], "pages": 3}}

    guardian = Guardian()
    batches = [
        batch async for batch in ingestion_metadata.extract_metadata_from_api_stream(
            guardian, from_date=date(2024, 1, 1), to_date=date(2024, 1, 3),
            max_roll=None, batch_size=1,
        )
    ]
    assert guardian.pages == [1, 2, 3]
    assert len(batches) == 3


@pytest.mark.asyncio
async def test_news_query_is_scoped_by_source_and_inclusive_dates():
    class Collection:
        def find(self, query, projection):
            self.query = query
            return self

        def sort(self, *args):
            return self

        async def to_list(self, length):
            return []

    collection = Collection()
    await get_news_ids(collection, date(2024, 1, 30), date(2024, 2, 2), "mock")
    assert collection.query == {
        "is_pull": False, "ingestion_source": "mock",
        "webPublicationDate": {
            "$gte": "2024-01-30T00:00:00",
            "$lt": "2024-02-03T00:00:00",
        },
    }


def test_metadata_refresh_does_not_reset_completed_article():
    record = {
        "id": "id", "webTitle": "title", "webUrl": "url", "sectionId": "section",
        "webPublicationDate": "2024-01-01T12:00:00Z", "is_pull": False,
    }
    update = ingestion_metadata.build_metadata_updates([record], datetime.now(timezone.utc))[0]["update"]
    assert "is_pull" not in update["$set"]
    assert update["$setOnInsert"]["is_pull"] is False
