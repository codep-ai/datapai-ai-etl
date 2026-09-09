# Week 1 — schema_introspector + medallion_planner

**Date:** 2026-09-09 · **Repo:** datapai-ai-etl · **Runtime:** EC2 platform.datap.ai, venv `/home/ec2-user/venv_ai_etl`

## Shipped
- `pipeline_planner/` package: `models.py` (SourceCatalog, MedallionPlan + validator + topo sort), `schema_introspector.py` (Postgres → catalog), `medallion_planner.py` (RouterChatClient → plan JSON, validate, re-prompt ≤2), `graph.py` (LangGraph introspect→plan→validate, SQLite/Postgres checkpointer), `cli.py`, `domains/stock.json`, `few_shot/chinook_medallion_plan.json`, `_vendored/` (PII heuristics + profiling ported from ag2_pipeline, autogen/duckdb removed).
- `platform_init.py` (DATAPAI_PLATFORM_DIR → `/home/ec2-user/git/datapai-platform-be` on EC2; `vanna-streamlit` does NOT have the agents), `requirements.txt` (no ag2), `migrations/001_sys_ai_pipeline.sql` (file only, not applied), README rewrite, `.gitignore`.
- `docs/architecture/column-level-lineage.md` — dbt-core cannot do column lineage; use sqlglot over compiled dbt SQL (Week 3), not SQLMesh.

## Verification (EC2)
- Introspect: 5 tables in 3.5 s. prices 29.99M rows via partition stats (15 LIST partitions), keys verified (`ticker+trade_date+exchange`), inferred FKs prices→ticker_universe, prices/ticker_universe→exchange_ref. `screener_metrics` captured as 55-column target contract.
- Plan: Gemini 2.5 flash, accepted on attempt 3 (validator rejected layer violations on attempts 1–2), 12 models = 5 staging + int_daily_price_enriched + the 6 required, topologically ordered. Saved: `docs/samples/2026-09-09-stock_medallion_plan.json`, `…stock_source_catalog.json`.

## Gotchas
- **EC2 glibc 2.26**: dbt-core ≥1.10 needs manylinux_2_28 wheels → pinned `dbt-core==1.9.*`, `dbt-snowflake==1.9.*`. No python3.11; venv is 3.12.
- **Gemini output truncated at ~8.8 KB**: platform-be `llm_policy` caps ad-hoc calls at 8192 tokens incl. thinking. Fix: `llm.py` sets `DATAPAI_ADHOC_MAX_TOKENS=32768`, `DATAPAI_ADHOC_THINKING=2048` (env-overridable) + minified JSON prompt.
- **PII false positive** on numeric columns (`adj_close`): value-regex skipped for numeric/temporal types.
- `sys_common_config` does not exist in the stock DB (5434); llm_policy falls back to env.
- `pkill -f` from an ssh one-liner kills the ssh shell itself when the pattern is in the command line — use a script file.

## Pending
- Week 2: `model_generator.py` + `build_loop.py` → `dbt build --select tag:stock_ai` green on Snowflake Iceberg, ≥1 auto-repair.
- Apply migration 001 (Week 3, needs confirmation). `git push` not done (needs confirmation).
