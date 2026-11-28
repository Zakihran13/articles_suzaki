import asyncio
from types import SimpleNamespace

import pytest

from articles_suzaki.ingestion import ingestion_metadata, ingestion_news


class FakeCollection:
    def __init__(self, name, writes, fail=False):
        self.name = name
        self.writes = writes
        self.fail = fail

    async def create_index(self, *args, **kwargs):
        pass

    async def bulk_write(self, operations, ordered=False):
        await asyncio.sleep(0.01)
        if self.fail:
            raise RuntimeError(f"{self.name} write failed")
        self.writes.append((self.name, [op._filter["id"] for op in operations]))
        return SimpleNamespace(matched_count=0, modified_count=0, upserted_count=len(operations))


@pytest.mark.asyncio
async def test_mock_metadata_processes_more_batches_than_concurrency_limit(monkeypatch):
    writes = []
    collection = FakeCollection("metadata", writes)
    monkeypatch.setattr(ingestion_metadata.config, "mock_total_articles", 5)
    monkeypatch.setattr(ingestion_metadata, "get_async_mongodb", lambda _: {
        ingestion_metadata.config.mongo_coll_metadata: collection,
    })

    await ingestion_metadata.main(source_type="mock", batch_size=1, max_concurrent_batches=2)

    assert len(writes) == 5


@pytest.mark.asyncio
async def test_metadata_write_failure_is_propagated(monkeypatch):
    collection = FakeCollection("metadata", [], fail=True)
    monkeypatch.setattr(ingestion_metadata.config, "mock_total_articles", 3)
    monkeypatch.setattr(ingestion_metadata, "get_async_mongodb", lambda _: {
        ingestion_metadata.config.mongo_coll_metadata: collection,
    })

    with pytest.raises(RuntimeError, match="metadata write failed"):
        await ingestion_metadata.main(source_type="mock", batch_size=1, max_concurrent_batches=2)


@pytest.mark.asyncio
async def test_mock_news_stores_articles_before_marking_metadata(monkeypatch):
    writes = []
    meta = FakeCollection("metadata", writes)
    news = FakeCollection("news", writes)
    monkeypatch.setattr(ingestion_news, "get_async_mongodb", lambda _: {
        ingestion_news.config.mongo_coll_metadata: meta,
        ingestion_news.config.mongo_coll_news: news,
    })

    async def get_ids(_):
        return [{"id": f"mock/{i:08d}"} for i in range(5)]

    monkeypatch.setattr(ingestion_news, "get_news_ids", get_ids)

    await ingestion_news.main(source_type="mock", batch_size=1, max_concurrent_batches=2)

    assert len(writes) == 10
    for i, (name, ids) in enumerate(writes):
        if name == "metadata":
            assert ("news", ids) in writes[:i]


@pytest.mark.asyncio
async def test_news_write_failure_does_not_mark_metadata(monkeypatch):
    writes = []
    meta = FakeCollection("metadata", writes)
    news = FakeCollection("news", writes, fail=True)
    monkeypatch.setattr(ingestion_news, "get_async_mongodb", lambda _: {
        ingestion_news.config.mongo_coll_metadata: meta,
        ingestion_news.config.mongo_coll_news: news,
    })

    async def get_ids(_):
        return [{"id": "mock/00000000"}]

    monkeypatch.setattr(ingestion_news, "get_news_ids", get_ids)

    with pytest.raises(RuntimeError, match="news write failed"):
        await ingestion_news.main(source_type="mock", batch_size=1, max_concurrent_batches=2)

    assert writes == []
