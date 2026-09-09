# Handover — AI ETL + Iceberg MVP (session brief)

**Date:** 2026-09-09
**From:** product-strategy session (datapai-stock-be)
**To:** dedicated delivery session — **this repo (`datapai-ai-etl`) is the code home**
**Owner:** Donny Zhao

---

## 1. Why this exists (30 seconds)

DATAP.AI's pitch: *"neutral Iceberg data floor + AI-powered ETL/dbt + APAC governance for regulated fintechs; cuts license AND engineering cost."* Today we can AI-generate dbt **sources + staging** (single/multi-table, independent). We cannot generate a **full raw → staging → intermediate → dim/fact pipeline** that is lineage-aware, `dbt build`-green, self-repairing, Iceberg-materialized, and governance-gated. That is the product gap this session closes. Orchestra (UK, USD 3M raised) automates T + orchestration on top of walled-garden lakehouses; we go one layer deeper on a neutral Iceberg floor.

Full pitch context: `~/.claude/projects/-Users-linlin-git-datapai-stock-be/memory/project_datapai_pitch_positioning.md`.

## 2. The authoritative plan — read it first

`/Users/linlin/git/datapai-platform-be/docs/phase-journals/2026-09-09-e2e-ai-medallion-pipeline-inventory-and-mvp-plan.md` (committed `fff6809`).
It holds: 6-repo inventory with exact paths to reuse, the precise gap table, LangGraph graph shape, 8 new components, reuse map, 4-week sequencing with definitions of done, decisions/alternatives, EC2 probe results. **Do not re-inventory.**

## 3. Approved decisions (locked, 2026-09-09)

| # | Decision |
|---|---|
| 1 | **Code home = `datapai-ai-etl`** (this repo). New package: `pipeline_planner/`. Import platform primitives from `datapai-platform-be` via `DATAPAI_PLATFORM_DIR` (copy the `platform_init.py` pattern from `datapai-stock-be/platform_init.py`). Do NOT build inside platform-be. |
| 2 | **dbt landing = `/Users/linlin/git/dbt-demo`** (EC2 `/home/ec2-user/git/dbt-demo`), `models/stock/{staging,intermediate,marts}`, tag `stock_ai`. Reusable across domains. |
| 3 | **Framework = LangGraph + LangChain. NOT AG2.** `ag2_pipeline/` in this repo is an archived PoC — vendor only `compliance_tools.py` (PII-annotated schema.yml) and `tools.py:profile_table/check_primary_key`; leave the rest. |
| 4 | **Iceberg is the system of record.** Every generated model: `+table_format: iceberg`, `+external_volume: DATAPAI_S3_VOL` (bronze `s3://codepais3/stock/bronze/`). Sources already in Iceberg → Snowflake **external Iceberg table** (`dbt-demo/macros/register_iceberg_sources.sql` pattern). Also flip `dbt-demo/models/stock/prices.sql` to Iceberg (config-only). Snowflake = engine over the floor, never the store. |
| 5 | **Engine for MVP = Snowflake** profile `datapai_snowflake_svc/prod` (key-pair `/snowflake-keys/airbyte_user.p8`). Athena/Glue = week 5+ (same Iceberg files, different engine). |
| 6 | **LLM transport = `datapai-platform-be/agents/llm_client.py:RouterChatClient`** (9 vendors; model-agnostic claim is real). Wrap as LangChain `BaseChatModel` or call from graph nodes. |
| 7 | **Seed to extend = `datapai-platform-be/agents/dbt_agent.py`** (LLM = planner JSON, Python = executor). Extend `DbtPlan` → `MedallionPlan`; reuse `run_dbt_commands`, `dbt_read_manifest_summary`. |
| 8 | **Governance in the loop:** `guardrail/governed_action.GovernedAction`, `agents/dbt_guardrail_agent.guardrail_validate_sql`, `traceability/ledger.TraceLedger`, HITL interrupt before first `dbt build` (skippable via `sys_common_config` demo flag). |
| 9 | **Registry = `sys_ai_pipeline` table** (DB-driven per CLAUDE.md), migration in this repo's `migrations/`. |
| 10 | **Orchestration = Airflow Cosmos DAG** in `datapai-airflow/dbt_airflow/dags/`, copy `governance_metrics.py` shape, `RenderConfig(select=["tag:stock_ai"])`, Dataset-triggered. No crontab. |

## 4. MVP scope (ruthless)

- **Domain:** stock prices. Raw = Postgres `datapai.prices` (LIST-partitioned, ~21M rows) + `ticker_universe`, `exchange_ref`, `fx_rates_daily`, `stock_market_closed_dates`. Already synced to S3/Snowflake by `datapai-stock-be/scripts/sync_*`.
- **Output:** `dim_ticker`, `dim_exchange`, `dim_date`, `fct_daily_price`, `int_price_with_indicators`, `fct_screener_metrics` — `ref()`-linked, `dbt build --select tag:stock_ai` green, Iceberg-materialized.
- **Credibility moment:** diff `fct_screener_metrics` row-for-row vs today's Python-computed `datapai.screener_metrics` / `datapai.ta_indicators`. Target ≥95% on a 30-day sample.
- **Graph:** `introspect → plan → [HITL] → generate(node) → build → parse run_results.json → repair(≤3) → register → emit_dag → diff`, checkpointed (SqliteSaver local, Postgres saver EC2).

## 5. Week 1 — start here

**Done when:** `schema_introspector` + `medallion_planner` emit a correct, topologically ordered 6-model `MedallionPlan` JSON from the 5 raw tables, reviewed by hand. Few-shot from `datapai-platform-be/dbt-traceability/` and `dbt-demo/models/chinook/`.

Step 0 (before any code): set up the venv on EC2 — see §6.

## 6. Runtime facts & gotchas (verified 2026-09-09)

- **EC2 is the ONLY runtime.** `ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai`. Local Mac = source only. Stock Postgres = docker `datapai_stock_db` :5434, schema `datapai`; framework DB `datapai_framework_db` :5433.
- **No venv with LangGraph exists.** `dbt_venv_governance` (py3.11, cosmos) referenced in platform-be journals was not found under `/home/ec2-user/{,git/*/}{venv,.venv}`. Run `find / -maxdepth 4 -name "dbt_venv*" 2>/dev/null`; else create `~/venv_ai_etl` (py3.11): `langgraph langchain-core dbt-core dbt-snowflake snowflake-connector-python psycopg2-binary pyyaml`.
- **`/home/ec2-user/git/datapai-platform-be` on EC2 is NOT a git checkout.** `DATAPAI_PLATFORM_DIR` on EC2 defaults to `/home/ec2-user/git/vanna-streamlit`; Airflow uses `/opt/datapai`. Confirm mapping before importing.
- **Reverse-sync EC2 → local before every commit** (`feedback_ec2_is_source_of_truth`). Deploy additive-only; **never `rsync --delete`**. This repo's `sync.sh` is a backup script — check its direction before reusing.
- **Snowflake creds** are in `~/.bash_profile` on EC2, not `/opt/datapai/.env.dev`. `DATAPAI_S3_VOL` + `DATAPAI_S3_INTEGRATION` already exist (`dbt-demo/macros/create_stock_infra.sql`).
- `~/.dbt/profiles.yml` on EC2: `datapai_snowflake_svc/prod`, `datapai_snowflake/dev`, `datapai` (redshift), `jaffle_shop` (postgres).
- Run docker compose as `ec2-user`, never `sudo`. Node = 20.20.0 if any UI is touched.
- Airflow common env: `datapai-airflow/dbt_airflow/dags/datapai_common.py` (`datapai_bash_task`, sources `/opt/datapai/.env.dev`, sets LLM scheduled/adhoc marker read by `agents/llm_policy.py`).

## 7. Working rules for this session (from `~/.claude/CLAUDE.md` + project memory)

- Autonomous execution under this approved plan; stop only for `git push`, prod deploys (systemd/Airflow activation), destructive shared-state ops.
- Phase journal + local commit **before** moving to the next week. Migration files `migrations/NNN_*.sql` with rollback note.
- DB-driven defaults; no hardcoded lists.
- Pilot stance: availability over strictness — generated-pipeline failures should degrade/warn, not block.

## 8. Deliverables by week (from the plan)

| Wk | Deliverable | DoD |
|---|---|---|
| 1 | introspector + planner | correct topo-ordered 6-model plan |
| 2 | generator + build/repair loop | `dbt build` green from cold start on Snowflake-over-Iceberg; ≥1 auto-repair observed |
| 3 | diff validator + DAG emitter + `sys_ai_pipeline` | ≥95% match vs Python truth; DAG parses in Airflow |
| 4 | governance bridge + 3-min demo + Lightdash explore | every SQL has guardrail decision + ledger row; recorded demo |
| 5–12 | Athena target, fundamentals domain, SCD2 dims, customer-VPC packaging, HITL UI | POC-ready for ACY / ex-ASX design partner |

## 9. Repo housekeeping (first commit of the session)

- Add `docs/` (this file), `pipeline_planner/`, `migrations/`, `requirements.txt` (no `ag2`), `platform_init.py`.
- Update `README.md`: this repo is now the **AI ETL product**, not an archived PoC; `ag2_pipeline/` and `workflow_engine/` are legacy references pending removal.
