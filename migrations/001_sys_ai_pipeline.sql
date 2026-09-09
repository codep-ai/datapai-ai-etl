-- 001_sys_ai_pipeline.sql
-- Registry for AI-generated dbt pipelines (decision #9, 2026-09-09).
--
-- Target database: datapai_framework_db (docker, EC2 :5433), schema datapai
--   ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai \
--     "docker exec -i datapai_framework_db psql -U postgres -d postgres" < migrations/001_sys_ai_pipeline.sql
--
-- Status: FILE ONLY as of Week 1 — NOT applied. Apply in Week 3 together with the
-- register node (pipeline_registry). Applying is a prod DDL change → needs confirmation.
--
-- Why a table and not YAML: CLAUDE.md DB-driven-defaults rule; lets the
-- datapai_eod_dynamic.py-style DAG factory read pipelines from the DB later.

CREATE SCHEMA IF NOT EXISTS datapai;

CREATE TABLE IF NOT EXISTS datapai.sys_ai_pipeline (
    pipeline_id        text PRIMARY KEY,                 -- e.g. stock_ai_medallion
    domain             text        NOT NULL,             -- stock | fundamentals | ...
    source_system      text        NOT NULL DEFAULT 'postgres',
    source_schema      text        NOT NULL,             -- datapai
    source_tables      text[]      NOT NULL,             -- raw tables introspected
    target_contracts   text[]      NOT NULL DEFAULT '{}',-- existing tables a mart must match
    target             text        NOT NULL DEFAULT 'snowflake_iceberg',
    dbt_project_dir    text        NOT NULL,             -- /home/ec2-user/git/dbt-demo
    dbt_models_subdir  text        NOT NULL,             -- models/stock
    tag                text        NOT NULL,             -- stock_ai (dbt select tag)
    goal               text        NOT NULL,             -- business goal fed to the planner
    required_models    text[]      NOT NULL,
    catalog_json       jsonb,                            -- SourceCatalog snapshot
    plan_json          jsonb,                            -- MedallionPlan (validated)
    manifest_hash      text,                             -- sha256 of dbt target/manifest.json after build
    status             text        NOT NULL DEFAULT 'planned'
                       CHECK (status IN ('planned','approved','generated','built','registered','failed','retired')),
    hitl_required      boolean     NOT NULL DEFAULT true, -- demo mode flips to false via sys_common_config
    created_by         text        NOT NULL DEFAULT current_user,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    last_run_at        timestamptz,
    notes              text
);

COMMENT ON TABLE datapai.sys_ai_pipeline IS
  'AI-generated medallion pipelines (datapai-ai-etl/pipeline_planner). One row per pipeline; plan_json is the validated MedallionPlan.';

CREATE INDEX IF NOT EXISTS ix_sys_ai_pipeline_domain_status
    ON datapai.sys_ai_pipeline (domain, status);

-- Seed: stock MVP request (mirrors pipeline_planner/domains/stock.json; the JSON file
-- is the interim source until this row exists).
INSERT INTO datapai.sys_ai_pipeline
    (pipeline_id, domain, source_schema, source_tables, target_contracts, dbt_project_dir, dbt_models_subdir,
     tag, goal, required_models, status, hitl_required, notes)
VALUES
    ('stock_ai_medallion', 'stock', 'datapai',
     ARRAY['prices','ticker_universe','exchange_ref','fx_rates_daily','stock_market_closed_dates'],
     ARRAY['screener_metrics'],
     '/home/ec2-user/git/dbt-demo', 'models/stock',
     'stock_ai',
     'Build the stock-price medallion mart for stock.datap.ai screeners (see pipeline_planner/domains/stock.json for the full goal text).',
     ARRAY['dim_ticker','dim_exchange','dim_date','fct_daily_price','int_price_with_indicators','fct_screener_metrics'],
     'planned', false,
     'MVP demo pipeline; hitl_required=false = demo mode')
ON CONFLICT (pipeline_id) DO NOTHING;

-- Verification
-- SELECT pipeline_id, domain, tag, status, array_length(required_models,1) FROM datapai.sys_ai_pipeline;

-- Rollback
-- DROP TABLE IF EXISTS datapai.sys_ai_pipeline;
