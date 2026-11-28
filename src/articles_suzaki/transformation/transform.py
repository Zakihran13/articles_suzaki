"""Main transformation flow: consume MongoDB raw data and load into PostgreSQL warehouse."""
import asyncio
from datetime import datetime, timezone, timedelta
from loguru import logger
from typing import AsyncGenerator, Optional
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from sqlalchemy.ext.asyncio import AsyncEngine

from articles_suzaki.configuration.config import config
from articles_suzaki.logging.flows import logged_async_flow
from articles_suzaki.warehouse.client import init_async_db, get_async_mongodb
from articles_suzaki.transformation.normalize import normalize_batch
from articles_suzaki.transformation.deduplication import deduplicate_articles
from articles_suzaki.transformation.staging import (
    initialize_schema,
    verify_tables_exist,
    insert_staging_articles,
    upsert_warehouse_articles,
    clean_staging,
)


async def extract_raw_articles_stream(
    coll_raw: AsyncIOMotorCollection,
    batch_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    batch_size: int = 100,
) -> AsyncGenerator[list[dict], None]:
    """Stream raw articles from MongoDB in batches with optional date filtering.
    
    Supports:
    - Single batch_id query (all articles in that batch)
    - Date range filtering (for historical data or specific day ranges)
    
    Args:
        coll_raw: MongoDB raw_articles collection
        batch_id: Batch identifier to filter by
        from_date: Optional start date (inclusive) for date range queries
        to_date: Optional end date (exclusive) for date range queries
        batch_size: Records per batch yield
        
    Yields:
        Batches of raw article documents
    """
    logger.info(
        f"Fetching raw articles for batch {batch_id} "
        f"(from={from_date}, to={to_date}, batch_size={batch_size})"
    )
    
    # batch_id only labels this run; raw documents are selected by date range.
    # webPublicationDate is stored as an ISO string, so compare against ISO strings.
    filter_dict = {}
    
    if from_date or to_date:
        date_filter = {}
        if from_date:
            date_filter['$gte'] = from_date.strftime('%Y-%m-%dT%H:%M:%S')
        if to_date:
            date_filter['$lt'] = to_date.strftime('%Y-%m-%dT%H:%M:%S')
        
        filter_dict['webPublicationDate'] = date_filter
        logger.info(f"Date range filter: {date_filter}")
    
    # Find all matching articles, ordered by insertion order
    cursor = coll_raw.find(filter_dict).sort('_id', 1)
    
    batch = []
    async for doc in cursor:
        batch.append(doc)
        
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Yield remaining articles
    if batch:
        yield batch
    
    logger.info(f"Finished streaming raw articles for batch {batch_id}")


async def normalize_and_deduplicate_stream(
    coll_raw: AsyncIOMotorCollection,
    batch_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    extract_batch_size: int = 100,
) -> AsyncGenerator[pd.DataFrame, None]:
    """Stream normalized and deduplicated articles from MongoDB.
    
    This async generator:
    1. Extracts raw articles in batches
    2. Normalizes each batch
    3. Deduplicates globally across the entire dataset
    4. Yields deduplicated minibatches ready for database insertion
    
    Args:
        coll_raw: MongoDB raw_articles collection
        batch_id: Batch identifier
        from_date: Optional start date (inclusive)
        to_date: Optional end date (exclusive)
        extract_batch_size: Initial extraction batch size from MongoDB
        
    Yields:
        DataFrames of deduplicated articles ready for staging
    """
    logger.info(f"Starting normalization and deduplication stream for batch {batch_id}")
    now = datetime.now(timezone.utc)
    
    all_normalized_df = None
    extraction_batch_num = 0
    total_extracted = 0
    
    # First pass: Extract and normalize all articles
    async for raw_batch in extract_raw_articles_stream(
        coll_raw, batch_id, from_date, to_date, extract_batch_size
    ):
        extraction_batch_num += 1
        total_extracted += len(raw_batch)
        
        logger.info(f"Extraction batch {extraction_batch_num}: {len(raw_batch)} raw articles")
        
        # Normalize
        df_normalized = normalize_batch(raw_batch, batch_id, now)
        
        # Accumulate all normalized records
        if all_normalized_df is None:
            all_normalized_df = df_normalized
        else:
            all_normalized_df = pd.concat(
                [all_normalized_df, df_normalized], ignore_index=True
            )
        
        logger.info(f"Accumulated {len(all_normalized_df)} normalized articles so far")
    
    if all_normalized_df is None or all_normalized_df.empty:
        logger.warning(f"No articles to process for batch {batch_id}")
        return
    
    logger.info(f"Total extracted: {total_extracted}, total normalized: {len(all_normalized_df)}")
    
    # Deduplicate globally
    df_dedup, duplicates = deduplicate_articles(all_normalized_df)
    logger.info(
        f"Deduplication complete: {len(df_dedup)} unique articles, {duplicates} duplicates removed"
    )
    
    # Yield deduplicated articles (will be used for staging)
    yield df_dedup


async def stage_minibatch_stream(
    engine_pg: AsyncEngine,
    batch_id: str,
    df_all: pd.DataFrame,
    minibatch_size: int = 500,
) -> AsyncGenerator[int, None]:
    """Stream minibatch insertions to staging table.
    
    Takes a deduplicated DataFrame and inserts it in minibatches for reliability
    and memory efficiency.
    
    Args:
        engine_pg: PostgreSQL async engine
        batch_id: Batch identifier
        df_all: Complete deduplicated DataFrame
        minibatch_size: Rows per minibatch insertion
        
    Yields:
        Count of rows inserted in each minibatch
    """
    logger.info(
        f"Starting minibatch staging for batch {batch_id} "
        f"(total={len(df_all)}, minibatch_size={minibatch_size})"
    )
    
    total_staged = 0
    minibatch_num = 0
    
    # Split into minibatches
    for i in range(0, len(df_all), minibatch_size):
        minibatch_num += 1
        df_minibatch = df_all.iloc[i : i + minibatch_size]
        
        logger.info(
            f"Staging minibatch {minibatch_num}: {len(df_minibatch)} rows "
            f"({i + 1}-{min(i + minibatch_size, len(df_all))} of {len(df_all)})"
        )
        
        # Insert minibatch
        rows_inserted = await insert_staging_articles(engine_pg, batch_id, df_minibatch)
        total_staged += rows_inserted
        
        yield rows_inserted
    
    logger.info(f"Minibatch staging complete: {total_staged} total rows inserted")


@logged_async_flow("transformation")
async def transform_and_stage(
    db_mongo: AsyncIOMotorDatabase,
    engine_pg: AsyncEngine,
    batch_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    extract_batch_size: int = 100,
    minibatch_size: int = 500,
) -> dict:
    """Transform raw MongoDB articles to warehouse schema and load into PostgreSQL.
    
    Robust flow with async generators and minibatch processing:
    1. Extract raw articles from MongoDB (with date filtering)
    2. Normalize each extraction batch
    3. Deduplicate globally
    4. Stage in minibatches (async generator)
    5. Upsert to warehouse
    6. Clean staging
    
    Args:
        db_mongo: MongoDB database connection
        engine_pg: PostgreSQL async engine
        batch_id: Batch identifier
        from_date: Optional start date (inclusive)
        to_date: Optional end date (exclusive)
        extract_batch_size: MongoDB extraction batch size
        minibatch_size: PostgreSQL staging minibatch size (default 500)
        
    Returns:
        Dict with transformation metrics
    """
    logger.info(
        f"Starting transformation for batch {batch_id} "
        f"(from={from_date}, to={to_date})"
    )
    
    coll_raw = db_mongo[config.mongo_coll_news]
    
    total_normalized = 0
    total_deduplicated = 0
    total_staged = 0
    
    try:
        # Normalize and deduplicate stream
        async for df_dedup in normalize_and_deduplicate_stream(
            coll_raw, batch_id, from_date, to_date, extract_batch_size
        ):
            total_normalized = len(df_dedup)
            total_deduplicated = len(df_dedup)
            
            # Stage in minibatches
            async for rows_in_minibatch in stage_minibatch_stream(
                engine_pg, batch_id, df_dedup, minibatch_size
            ):
                total_staged += rows_in_minibatch
        
        if total_staged == 0:
            logger.warning(f"No articles staged for batch {batch_id}")
            return {
                'batch_id': batch_id,
                'from_date': from_date,
                'to_date': to_date,
                'total_normalized': total_normalized,
                'total_deduplicated': total_deduplicated,
                'total_staged': 0,
                'status': 'empty',
            }
        
        # Upsert to warehouse
        logger.info("Starting warehouse upsert...")
        inserted, updated, unchanged = await upsert_warehouse_articles(engine_pg, batch_id)
        logger.info(f"Warehouse upsert complete (inserted={inserted}, updated={updated})")
        
        # Clean staging
        logger.info("Cleaning staging table...")
        await clean_staging(engine_pg, batch_id)
        
        logger.info(
            f"Transformation complete for batch {batch_id}: "
            f"normalized={total_normalized}, "
            f"deduplicated={total_deduplicated}, "
            f"staged={total_staged}"
        )
        
        return {
            'batch_id': batch_id,
            'from_date': from_date,
            'to_date': to_date,
            'total_normalized': total_normalized,
            'total_deduplicated': total_deduplicated,
            'total_staged': total_staged,
            'inserted': inserted,
            'updated': updated,
            'unchanged': unchanged,
            'status': 'success',
        }
    
    except Exception as e:
        logger.error(
            f"Transformation failed for batch {batch_id}: {e}", exc_info=True
        )
        raise


@logged_async_flow("transformation")
async def main():
    """Main transformation entry point, configured through .env (see config.py).

    - Daily mode: leave TRANSFORM_FROM_DATE and TRANSFORM_TO_DATE empty
      (processes yesterday + today, UTC).
    - Historical mode: set both dates (TO is exclusive).
    """
    parsed_from_date, parsed_to_date = config.transform_range
    extract_batch_size = config.transform_extract_batch_size
    minibatch_size = config.transform_minibatch_size
    batch_id = f"transform_{config.current_time:%Y%m%dT%H%M%S}"

    logger.info(
        f"Starting transformation pipeline for batch {batch_id} "
        f"(from={parsed_from_date}, to={parsed_to_date}, "
        f"extract_batch={extract_batch_size}, minibatch={minibatch_size})"
    )
    
    # Initialize connections
    logger.info("Connecting to MongoDB and PostgreSQL...")
    db_mongo = get_async_mongodb(config.mongo_db_name)
    engine_pg = init_async_db()
    
    try:
        # Initialize schema and tables
        logger.info("Ensuring PostgreSQL schema and tables exist...")
        await initialize_schema(engine_pg)
        
        # Verify tables were created successfully
        tables_ok = await verify_tables_exist(engine_pg)
        if not tables_ok:
            logger.error("Required tables do not exist after schema initialization")
            raise RuntimeError("Schema initialization failed: required tables missing")
        
        logger.info("Schema verification passed")
        
        # Run transformation
        metrics = await transform_and_stage(
            db_mongo,
            engine_pg,
            batch_id,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            extract_batch_size=extract_batch_size,
            minibatch_size=minibatch_size,
        )
        
        logger.info(f"Transformation metrics: {metrics}")
        
        if metrics['status'] == 'success':
            logger.info(f"Transformation completed successfully for batch {batch_id}")
        else:
            logger.warning(f"Transformation resulted in empty batch: {batch_id}")
    
    except Exception as e:
        logger.error(f"Transformation failed for batch {batch_id}: {e}", exc_info=True)
        raise
    
    finally:
        await engine_pg.dispose()


if __name__ == "__main__":
    asyncio.run(main())
