


async def extract_each_news(
    guardian: GuardianClient,
    metadata_list: list[dict],
) -> list[dict]:
    params = {
        "api-key": guardian.api_key,
        "order-by": "newest",
        "show-fields": "all",
        "show-tags": "keyword,contributor",
        "type": "article",
    }

    pending = [
        news_metadata
        for news_metadata in metadata_list
        if not news_metadata.get("is_pull", False)
    ]

    logger.info(f"found {len(pending)} metadata entries to pull")

    results = await asyncio.gather(
        *(guardian.query(meta["id"], params) for meta in pending),
        return_exceptions=True,
    )

    news_list = []

    for meta, result in zip(pending, results):
        if isinstance(result, Exception):
            logger.error(f"failed to pull {meta['id']}: {result}")
            continue

        content = result.get("response", {}).get("content")

        if not content:
            logger.error(f"no content returned for {meta['id']}")
            continue

        news_list.append(content)

    return news_list