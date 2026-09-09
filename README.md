# datapai-ai-etl — AI ETL + Iceberg "one-click medallion"

**Status (2026-09-09):** active product code. This repo is the code home for DATAP.AI's
AI-generated dbt medallion pipelines (raw → staging → intermediate → dim/fact) on an
Apache Iceberg floor, with governance in the loop.

Pitch line it implements: *"neutral Iceberg data floor + AI-powered ETL/dbt + APAC
governance for regulated fintechs; cuts license AND engineering cost."*

## Read first
1. `docs/phase-journals/2026-09-09-handover-ai-etl-iceberg-mvp.md` — session brief, 10 locked decisions, week-by-week definition of done, EC2 runtime gotchas.
2. `/Users/linlin/git/datapai-platform-be/docs/phase-journals/2026-09-09-e2e-ai-medallion-pipeline-inventory-and-mvp-plan.md` — 6-repo inventory, gap table, graph shape, reuse map.
3. `docs/phase-journals/` — one journal per delivery week (newest = current state).

## Layout
| Path | What |
|---|---|
| `pipeline_planner/` | **The product.** LangGraph graph: `introspect → plan → [HITL] → generate → build → parse_results → repair* → register → emit_dag → diff` |
| `pipeline_planner/schema_introspector.py` | Postgres raw schema → `SourceCatalog` (keys, inferred FKs, partition-aware counts, PII sensitivity) |
| `pipeline_planner/medallion_planner.py` | LLM (RouterChatClient) → `MedallionPlan` JSON, validated + topologically ordered, self-repairing prompt loop |
| `pipeline_planner/models.py` | `SourceCatalog`, `MedallionPlan` (pydantic) |
| `pipeline_planner/graph.py` | LangGraph `StateGraph` + checkpointer (SQLite local / Postgres EC2) |
| `pipeline_planner/cli.py` | `python -m pipeline_planner.cli introspect|plan|validate|run` |
| `pipeline_planner/domains/*.json` | Per-domain request (tables, goal, required models). Interim until `sys_ai_pipeline` is live |
| `pipeline_planner/few_shot/` | Planner exemplars (chinook shape) |
| `pipeline_planner/_vendored/` | Dependency-free ports of `compliance_tools` PII heuristics and `profile_table/check_primary_key` |
| `platform_init.py` | Adds `DATAPAI_PLATFORM_DIR` (datapai-platform-be) to `sys.path` for `agents.llm_client`, `agents.dbt_agent`, governance, ledger |
| `migrations/` | `001_sys_ai_pipeline.sql` — pipeline registry (framework DB). File only until Week 3 |
| `docs/` | Phase journals, architecture notes, operator runbook |
| `ag2_pipeline/`, `workflow_engine/` | **Legacy PoC (AG2). Reference only, pending removal.** Never ran in prod. Only `compliance_tools.py` and `tools.py` heuristics were vendored |
| `sync.sh` | Local → EC2 backup rsync (additive, `--dry-run` supported). Never add `--delete` |

## Runtime
- **EC2 `platform.datap.ai` is the only runtime.** Local Mac is source only.
  `ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai`
- venv: `/home/ec2-user/venv_ai_etl` (python3.12; `pip install -r requirements.txt`).
  dbt is pinned to 1.9.x because the host glibc (2.26) cannot load dbt-core 1.10+ parser wheels.
- Env (EC2): `export DATAPAI_PLATFORM_DIR=/home/ec2-user/git/datapai-platform-be` (NOT `vanna-streamlit`),
  `source ~/.env.dev` for `DATAPAI_PG_*` (stock DB :5434) and LLM keys; Snowflake vars are in `~/.bash_profile`.
- dbt landing zone: `/home/ec2-user/git/dbt-demo/models/stock/{staging,intermediate,marts}`, tag `stock_ai`, profile `datapai_snowflake_svc/prod`.

## Week-1 quickstart (on EC2)
```bash
cd /home/ec2-user/git/datapai-ai-etl
source ~/.env.dev; export DATAPAI_PLATFORM_DIR=/home/ec2-user/git/datapai-platform-be
/home/ec2-user/venv_ai_etl/bin/python -m pipeline_planner.cli introspect --out out/stock_catalog.json
/home/ec2-user/venv_ai_etl/bin/python -m pipeline_planner.cli plan --catalog out/stock_catalog.json --out out/stock_plan.json
```

## Working rules
- Reverse-sync EC2 → local before every commit; deploy additive-only (`rsync` without `--delete`).
- Phase journal + local commit before moving to the next week. `git push` and Airflow activation need explicit confirmation.
- DB-driven defaults (no hardcoded plan lists); pilot stance = availability over strictness.
