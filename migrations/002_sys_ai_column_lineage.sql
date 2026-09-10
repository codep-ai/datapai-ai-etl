-- 002_sys_ai_column_lineage.sql
-- Column-level lineage store for AI-generated dbt pipelines — SYSTEM table (control plane).
--
-- Target database: docker datapai_framework_db (EC2 :5433), DATABASE datapai_auth_db (FRAMEWORK_DB_NAME in .env.dev —
--   this is where every other datapai.sys_* table lives; NOT the default "postgres" db), schema datapai
--   ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai \
--     "docker exec -i datapai_framework_db psql -U postgres -d datapai_auth_db" < migrations/002_sys_ai_column_lineage.sql
--
-- Decision 2026-09-10 (Donny): lineage is framework metadata, not customer data → Postgres next to
-- sys_ai_pipeline, not the customer's warehouse. Engine-independent (works for Athena/Databricks/ClickHouse
-- pipelines), joinable to registry/guardrail/audit tables, cheap per-run upserts, recursive graph queries.
-- A read-only projection into the warehouse for BI is optional (see column_lineage.write_lineage_table).
-- Written by: pipeline_planner/column_lineage.py:write_lineage_pg()  (`cli lineage --to-db`)

CREATE SCHEMA IF NOT EXISTS datapai;

CREATE TABLE IF NOT EXISTS datapai.sys_ai_column_lineage (
    id              bigserial   PRIMARY KEY,
    pipeline_id     text        NOT NULL,                 -- sys_ai_pipeline.pipeline_id (soft FK: registry may be seeded later)
    run_id          text        NOT NULL,                 -- lineage run id (UTC timestamp id)
    run_at          timestamptz NOT NULL DEFAULT now(),
    dialect         text        NOT NULL,                 -- sqlglot dialect: snowflake | trino | databricks | clickhouse | bigquery
    from_node       text        NOT NULL,                 -- upstream dbt node name or 'source:<table>'
    from_column     text        NOT NULL,
    to_node         text        NOT NULL,                 -- downstream dbt model
    to_column       text        NOT NULL,
    kind            text        NOT NULL CHECK (kind IN ('passthrough','derived')),
    to_sensitivity  text        NOT NULL DEFAULT 'PUBLIC'
                    CHECK (to_sensitivity IN ('PUBLIC','PII_LOW','PII_MEDIUM','FINANCIAL','PII_HIGH')),
    degraded        boolean     NOT NULL DEFAULT false,   -- resolved without schema (upstream column undeclared)
    created_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE datapai.sys_ai_column_lineage IS
  'Column-level lineage edges per pipeline run, computed by datapai-ai-etl (sqlglot over dbt manifest). One row per (from col -> to col).';

CREATE INDEX IF NOT EXISTS ix_sys_ai_column_lineage_run    ON datapai.sys_ai_column_lineage (pipeline_id, run_id);
CREATE INDEX IF NOT EXISTS ix_sys_ai_column_lineage_to     ON datapai.sys_ai_column_lineage (to_node, to_column);
CREATE INDEX IF NOT EXISTS ix_sys_ai_column_lineage_from   ON datapai.sys_ai_column_lineage (from_node, from_column);

-- Latest run per pipeline (what UIs should read by default)
CREATE OR REPLACE VIEW datapai.v_sys_ai_column_lineage_latest AS
SELECT l.*
FROM datapai.sys_ai_column_lineage l
JOIN (SELECT pipeline_id, max(run_at) AS run_at FROM datapai.sys_ai_column_lineage GROUP BY pipeline_id) m
  ON m.pipeline_id = l.pipeline_id AND m.run_at = l.run_at;

-- Verification
-- SELECT pipeline_id, run_id, count(*) FROM datapai.sys_ai_column_lineage GROUP BY 1,2 ORDER BY 2 DESC;
-- WITH RECURSIVE up AS (
--   SELECT from_node, from_column, to_node, to_column, 1 AS hop FROM datapai.v_sys_ai_column_lineage_latest
--    WHERE to_node='fct_daily_price' AND to_column='close_usd'
--   UNION ALL
--   SELECT l.from_node, l.from_column, l.to_node, l.to_column, up.hop+1 FROM datapai.v_sys_ai_column_lineage_latest l
--     JOIN up ON l.to_node=up.from_node AND l.to_column=up.from_column WHERE up.hop < 6)
-- SELECT hop, from_node||'.'||from_column FROM up ORDER BY 1,2;

-- Rollback
-- DROP VIEW IF EXISTS datapai.v_sys_ai_column_lineage_latest;
-- DROP TABLE IF EXISTS datapai.sys_ai_column_lineage;
