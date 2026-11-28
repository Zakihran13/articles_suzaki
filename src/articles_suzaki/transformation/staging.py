"""Database operations for staging and loading articles into PostgreSQL warehouse."""
from pathlib import Path
from sqlalchemy import text, inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from loguru import logger
from typing import List
import pandas as pd


async def initialize_schema(engine: AsyncEngine) -> None:
    """Create PostgreSQL schema and tables if they don't exist.
    
    Reads schema.sql from the sql/ directory and executes it.
    
    Args:
        engine: SQLAlchemy async engine
        
    Raises:
        FileNotFoundError: If schema.sql is not found
        Exception: If schema creation fails
    """
    logger.info("Checking and initializing PostgreSQL schema...")
    
    # Locate schema.sql file
    schema_file = Path(__file__).resolve().parents[3] / 'sql' / 'schema.sql'
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    logger.info(f"Reading schema from {schema_file}")
    schema_sql = schema_file.read_text()
    
    # schema.sql is idempotent (IF NOT EXISTS); run it in one transaction
    async with engine.begin() as conn:
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        for statement in statements:
            await conn.execute(text(statement))
    
    logger.info("Schema initialization complete")


async def verify_tables_exist(engine: AsyncEngine) -> bool:
    """Verify that required tables exist in PostgreSQL.
    
    Args:
        engine: SQLAlchemy async engine
        
    Returns:
        True if all required tables exist, False otherwise
    """
    required_tables = [
        ('warehouse', 'articles'),
        ('staging', 'articles'),
        ('pipeline', 'checkpoint'),
        ('pipeline', 'runs'),
    ]
    
    logger.info("Verifying required tables exist...")
    
    async with engine.begin() as conn:
        for schema, table in required_tables:
            result = await conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                )
            """))
            exists = result.scalar()
            if exists:
                logger.info(f"✓ Table {schema}.{table} exists")
            else:
                logger.error(f"✗ Table {schema}.{table} does NOT exist")
                return False
    
    return True


async def insert_staging_articles(
    engine: AsyncEngine,
    batch_id: str,
    df: pd.DataFrame,
) -> int:
    """Insert normalized articles into staging table.
    
    Args:
        engine: SQLAlchemy async engine
        batch_id: Batch identifier
        df: DataFrame of normalized articles
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.info(f"Empty DataFrame for batch {batch_id}, skipping staging insert")
        return 0
    
    logger.info(f"Inserting {len(df)} articles into staging.articles for batch {batch_id}")
    
    # Prepare data for insert
    records = df.to_dict('records')
    
    insert_sql = text("""
        INSERT INTO staging.articles 
        (id, batch_id, title, content, published_at, author_id, 
         source, source_modified_at, ingestion_timestamp, checksum)
        VALUES 
        (:id, :batch_id, :title, :content, :published_at, :author_id,
         :source, :source_modified_at, :ingestion_timestamp, :checksum)
        ON CONFLICT (batch_id, id) DO NOTHING
    """)
    
    async with engine.begin() as conn:
        await conn.execute(insert_sql, records)
        rows_inserted = len(records)
    
    logger.info(f"Inserted {rows_inserted} articles into staging.articles")
    return rows_inserted


async def upsert_warehouse_articles(
    engine: AsyncEngine,
    batch_id: str,
) -> tuple[int, int, int]:
    """Conditionally upsert articles from staging to warehouse.
    
    Updates if source_modified_at is newer or if business fields changed.
    Does not let older source_modified_at overwrite newer ones.
    
    Args:
        engine: SQLAlchemy async engine
        batch_id: Batch identifier
        
    Returns:
        Tuple of (inserted_count, updated_count, unchanged_count)
    """
    logger.info(f"Upserting articles from staging to warehouse for batch {batch_id}")
    
    # Upsert logic: for each article in staging
    # 1. If not in warehouse -> INSERT
    # 2. If in warehouse:
    #    - If staging.source_modified_at > warehouse.source_modified_at -> UPDATE
    #    - If staging.source_modified_at = warehouse.source_modified_at and checksum differs -> UPDATE
    #    - Otherwise -> no change
    
    upsert_sql = text("""
        INSERT INTO warehouse.articles 
        (id, title, content, published_at, author_id, 
         source, source_modified_at, ingestion_timestamp, batch_id, checksum,
         created_at, updated_at, deleted_at)
        SELECT 
            id, title, content, published_at, author_id,
            source, source_modified_at, ingestion_timestamp, batch_id, checksum,
            ingestion_timestamp, ingestion_timestamp, NULL
        FROM staging.articles
        WHERE batch_id = :batch_id
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            content = EXCLUDED.content,
            published_at = EXCLUDED.published_at,
            author_id = EXCLUDED.author_id,
            source = EXCLUDED.source,
            checksum = EXCLUDED.checksum,
            updated_at = EXCLUDED.updated_at,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp,
            batch_id = EXCLUDED.batch_id,
            deleted_at = NULL
        WHERE 
            warehouse.articles.source_modified_at IS NULL
            OR EXCLUDED.source_modified_at IS NULL
            OR EXCLUDED.source_modified_at > warehouse.articles.source_modified_at
            OR (EXCLUDED.source_modified_at = warehouse.articles.source_modified_at 
                AND EXCLUDED.checksum != warehouse.articles.checksum)
        RETURNING (xmax = 0) AS inserted
    """)
    
    async with engine.begin() as conn:
        result = await conn.execute(upsert_sql, {'batch_id': batch_id})
        flags = [row.inserted for row in result]
        total = (await conn.execute(
            text("SELECT count(*) FROM staging.articles WHERE batch_id = :batch_id"),
            {'batch_id': batch_id},
        )).scalar_one()
    
    inserted = sum(flags)
    updated = len(flags) - inserted
    unchanged = total - len(flags)
    logger.info(f"Upsert completed for batch {batch_id}")
    return inserted, updated, unchanged


async def clean_staging(
    engine: AsyncEngine,
    batch_id: str,
) -> int:
    """Remove articles from staging after successful warehouse load.
    
    Args:
        engine: SQLAlchemy async engine
        batch_id: Batch identifier
        
    Returns:
        Number of rows deleted
    """
    logger.info(f"Cleaning staging articles for batch {batch_id}")
    
    delete_sql = text("""
        DELETE FROM staging.articles
        WHERE batch_id = :batch_id
    """)
    
    async with engine.begin() as conn:
        result = await conn.execute(delete_sql, {'batch_id': batch_id})
        rows_deleted = result.rowcount
    
    logger.info(f"Deleted {rows_deleted} rows from staging.articles")
    return rows_deleted
