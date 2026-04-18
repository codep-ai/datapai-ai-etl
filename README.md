# datapai-ai-etl

**AI-driven ETL research & PoC code.** Not shipped, not supported, not part of the `datapai-platform-be` product surface.

## Status

Extracted from `datapai-platform-be` on 2026-04-18 as part of the platform-isolation refactor (see `datapai-platform-be/docs/phase-journals/2026-04-18-platform-isolation-plan.md`). The code here was PoC / research work not in active production use when removed.

Kept in a separate repo rather than deleted so the research can be revisited.

## Contents

### `workflow_engine/` — lightweight AI-driven pipeline runner

Source: former `datapai-platform-be/etl/` directory.

- `contracts.py` — pipeline contract definitions
- `plans.py` — plan representation
- `run_context.py` — runtime context
- `workflow_runner.py` — pipeline execution
- `workflow_service.py` — service wrapper
- `copy_data.py`, `insert_redshift.py`, `insert_snowflake.py` — data-movement steps

### `ag2_pipeline/` — AG2 multi-agent ETL with compliance hooks

Source: former `datapai-platform-be/agents/etl/` directory.

- `pipeline.py` — AG2 multi-agent pipeline orchestrator
- `audit.py` — pipeline audit logging
- `compliance_tools.py` — compliance check functions
- `cost_control.py` — cost guard hooks
- `llm_config.py` — LLM configuration
- `prompts.py` — prompt templates
- `tools.py` — tool registry
- `airbyte_tools.py` — Airbyte integration (note: Airbyte stopped on prod EC2 as of 2026-04-06)

## Why moved here

Two reasons:
1. **Never used in production** — dead code in the active platform repo adds security-review surface without value.
2. **Positioning** — the platform-be product is sold as an add-in into BMC/SAP deployments; workflow engines and Airbyte integrations visually compete with Control-M / SAP Data Services, which is the wrong signal for that buyer.

If AI-driven ETL becomes a product direction, revive this code here and ship it as its own product — not as a platform feature.

## Recall path

```
cp ai_etl/<path> ../datapai-platform-be/<destination>
# or
git clone <this repo>
cherry-pick specific files
```
