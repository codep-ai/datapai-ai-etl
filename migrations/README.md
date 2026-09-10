# migrations

Numbered files here target the framework Postgres (`datapai_auth_db`) or Snowflake and belong to the AI ETL generator:

- `001_sys_ai_pipeline.sql` — pipeline registry (applied 2026-09-10)
- `003_snowflake_bronze_reference_tables.sql` — bronze Iceberg reference tables (applied by raw_loader)

**Moved to `datapai-platform-be/migrations/` on 2026-09-10** (column lineage + PII governance are platform features):
`2026-09-10_sys_ai_column_lineage.sql`, `2026-09-10_v_sys_ai_pii_columns.sql`, `2026-09-10_sys_common_config_pii_policy.sql`,
`2026-09-10_sys_common_config_guardrail_default_access.sql`, `2026-09-10_pii_defaults_permissive.sql` (formerly 002, 004–007 here; all applied).
