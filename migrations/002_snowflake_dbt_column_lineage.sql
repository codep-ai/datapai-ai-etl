-- 002_snowflake_dbt_column_lineage.sql
-- Column-level lineage store for AI-generated dbt pipelines.
--
-- Target: Snowflake, database DATAPAI (engine over the Iceberg floor; table is a Snowflake-managed
--         ICEBERG table on DATAPAI_S3_VOL, base location s3://codepais3/stock/bronze/ai_etl/...).
-- Applied by: pipeline_planner/column_lineage.py:write_lineage_table() with CREATE ... IF NOT EXISTS
--             (`python -m pipeline_planner.cli lineage --to-warehouse`). This file is the source-of-truth
--             copy of that DDL for review/history; running it by hand is equivalent.
-- First applied: 2026-09-10 (run_id 20260910T041420Z, 166 edges).
-- Schema is per pipeline: AI_ETL_LINEAGE_SCHEMA (default STOCK_MARTS).

CREATE SCHEMA IF NOT EXISTS DATAPAI.STOCK_MARTS;

CREATE ICEBERG TABLE IF NOT EXISTS DATAPAI.STOCK_MARTS."DBT_COLUMN_LINEAGE" (
  "PIPELINE_ID"    VARCHAR,            -- e.g. stock_ai_medallion (sys_ai_pipeline.pipeline_id)
  "RUN_ID"         VARCHAR,            -- UTC timestamp id of the lineage run
  "RUN_AT"         TIMESTAMP_NTZ(6),
  "DIALECT"        VARCHAR,            -- sqlglot dialect used (snowflake | trino | databricks | ...)
  "FROM_NODE"      VARCHAR,            -- upstream dbt node name or 'source:<table>'
  "FROM_COLUMN"    VARCHAR,
  "TO_NODE"        VARCHAR,            -- downstream dbt model
  "TO_COLUMN"      VARCHAR,
  "KIND"           VARCHAR,            -- passthrough | derived
  "TO_SENSITIVITY" VARCHAR,            -- PUBLIC | PII_LOW | PII_MEDIUM | FINANCIAL | PII_HIGH (propagated)
  "DEGRADED"       BOOLEAN             -- true when resolved without schema (upstream column undeclared)
)
 EXTERNAL_VOLUME = 'DATAPAI_S3_VOL'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'ai_etl/STOCK_MARTS/dbt_column_lineage/';

-- Verification
-- SELECT pipeline_id, run_id, COUNT(*) FROM DATAPAI.STOCK_MARTS.DBT_COLUMN_LINEAGE GROUP BY 1,2 ORDER BY 2 DESC;
-- Recursive trace example: docs/demo/2026-09-10-column-level-lineage-demo.md §5

-- Rollback
-- DROP ICEBERG TABLE IF EXISTS DATAPAI.STOCK_MARTS."DBT_COLUMN_LINEAGE";
