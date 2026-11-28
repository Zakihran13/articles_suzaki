-- PostgreSQL schema for articles ETL/ELT pipeline
-- Articles warehouse and pipeline state tables (idempotent: safe to run repeatedly)

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE SCHEMA IF NOT EXISTS staging;

-- Main warehouse table for articles
CREATE TABLE IF NOT EXISTS warehouse.articles (
    -- Core business fields
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    author_id TEXT,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,

    -- Metadata
    source TEXT NOT NULL,
    source_modified_at TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    batch_id TEXT NOT NULL,
    checksum TEXT NOT NULL
);

-- Index for published_at queries (required by MVP)
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON warehouse.articles(published_at);

-- Index for batch_id lookups (useful for operations)
CREATE INDEX IF NOT EXISTS idx_articles_batch_id ON warehouse.articles(batch_id);

-- Index for source and source_modified_at (useful for incremental queries)
CREATE INDEX IF NOT EXISTS idx_articles_source_modified ON warehouse.articles(source, source_modified_at);


-- Pipeline state and metadata tables

-- Checkpoint table: tracks the last successfully committed window
CREATE TABLE IF NOT EXISTS pipeline.checkpoint (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL UNIQUE,
    last_successful_end TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Run metadata: summary of each pipeline execution
CREATE TABLE IF NOT EXISTS pipeline.runs (
    id SERIAL PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    flow_name TEXT,
    source TEXT NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL,  -- 'started', 'completed', 'failed'
    
    -- Counts
    fetched_count INT,
    validated_count INT,
    rejected_count INT,
    deduplicated_count INT,
    inserted_count INT,
    updated_count INT,
    unchanged_count INT,
    request_count INT,
    
    -- Timing and diagnostics
    duration_ms INT,
    retry_count INT,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Existing installations may predate flow-level run tracking.
ALTER TABLE pipeline.runs ADD COLUMN IF NOT EXISTS flow_name TEXT;

-- Index for batch lookups
CREATE INDEX IF NOT EXISTS idx_runs_batch_id ON pipeline.runs(batch_id);

-- Index for source and time-based queries
CREATE INDEX IF NOT EXISTS idx_runs_source_completed ON pipeline.runs(source, completed_at);


-- Staging table: holds validated records for one attempt before final load
CREATE TABLE IF NOT EXISTS staging.articles (
    id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    author_id TEXT,
    source TEXT NOT NULL,
    source_modified_at TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    checksum TEXT NOT NULL,
    
    PRIMARY KEY (batch_id, id)
);

-- Index for batch operations
CREATE INDEX IF NOT EXISTS idx_staging_articles_batch ON staging.articles(batch_id);
