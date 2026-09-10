# Customer instruction files and re-platform projects — design

**Date:** 2026-09-10 · **Status:** design, debated with Donny; implementation Week 5+ · **Owner:** datapai-ai-etl

## 1. The two questions

1. **Custom prompts / instruction files.** A customer hands us a Word, PDF or text document describing sources, target and transformation rules. How does that drive the generator without us hand-writing `domains/<x>.json`?
2. **Existing estates.** Digital transformation, data migration and re-platform projects (Snowflake → Databricks, Databricks → Snowflake, Informatica/Control-M → dbt) start from *code that already exists*, not from a raw schema. How does the same graph serve those?

## 2. Principle that settles most of it

**Instruction files are data, never instructions to the agent.** They are parsed into a typed `PipelineBrief`; only fields of that schema reach the planner and generator prompts, inside a delimited "customer requirements" block. Free text from a customer document is never spliced into a system prompt. This is the prompt-injection boundary and it is also what makes the brief reviewable, diffable and storable in `sys_ai_pipeline`.

The LLM extracts; Python validates; a human approves the brief (HITL) before any plan is made. Same pattern as plan → validate → generate → build.

## 3. Greenfield: instruction file → PipelineBrief → plan

```
instruction files (.docx/.pdf/.md/.txt/.xlsx mapping sheets)
        │  parse (python-docx / pypdf / openpyxl; platform-be file_ingest_agent for S3 uploads)
        ▼
  sections + tables + page/para refs
        │  LLM extract (RouterChatClient) → JSON, schema-constrained, each field cites its source span
        ▼
  PipelineBrief {
      sources:      [{system, schema, tables|patterns, connection_ref, notes}]
      targets:      [{engine, database, schema, storage: iceberg|delta|native, external_volume}]
      outputs:      [{name, layer, grain, business_definition, must_match: <existing table>}]
      rules:        [{kind: transform|quality|pii|scd|naming|retention, text, applies_to, citation}]
      conventions:  {naming, test_policy, scd_default, timezone, currency}
      glossary:     {term: definition}
      open_questions: [..]                       # things the doc does not settle
  }
        │  validate (pydantic) + HITL approve/edit
        ▼
  existing graph: introspect(sources) → plan(goal = brief.outputs + rules + conventions) → …
```

- `domains/stock.json` becomes just a hand-written `PipelineBrief`. One schema for both paths.
- **Custom prompts** are *conventions and rules*, not prompt text. They live in a DB registry (`sys_ai_prompt`: scope global / tenant / project / node, version, approved_by, effective_from) per the DB-driven rule, and render into a fixed slot of the planner/generator prompts. Customers can edit rules; they cannot rewrite the system prompt.
- Each `rules[]` item maps to something the framework can *enforce*, not only ask for: `quality` → dbt_expectations tests; `pii` → masking in staging + restricted-column list for the guardrail; `scd` → snapshot/SCD2 macro; `naming` → validator regex. A rule the framework cannot enforce is surfaced in `open_questions`, not silently trusted.

## 4. Migration / re-platform: code → catalog → plan

Same graph, different introspector. The *source* is an existing project, and the plan's "logic" is recovered from code rather than invented.

| Source estate | Introspector reads | Deterministic first pass | LLM only for |
|---|---|---|---|
| dbt project (any adapter) | `manifest.json`, model SQL, schema.yml | `sqlglot.transpile(dialect_from → dialect_to)` per model; keep refs/tests/docs | non-transpilable functions, UDFs, macros with adapter-specific SQL |
| Snowflake estate | `information_schema` + `get_ddl` for views, tasks, streams, procedures | views → models via sqlglot; tasks → DAG edges | JavaScript/Python procedures → model + note |
| Databricks estate | Unity Catalog, notebooks (`.py`/`.sql` cells), Delta Live Tables | SQL cells → models; DLT expectations → dbt tests | PySpark dataframe code → SQL (LLM, then equivalence-tested) |
| Informatica / DataStage / Control-M | XML exports, job definitions | `controlm_converter` renderer (platform-be) for schedules; mapping XML → column map | expression language → SQL |

Steps that are the same for every estate:

1. **Catalog the code**: parse every transformation with `sqlglot` into an AST; build the table- and **column-level lineage graph** (the same `column_lineage.py` from Week 3). Output: `SourceCatalog` + `LegacyPlan` (the plan *as it exists*).
2. **Diff the intent**: instruction files (§3) describe what the *new* platform should do; `LegacyPlan` describes what the old one does. The planner receives both and must list every divergence explicitly (`open_questions`).
3. **Translate deterministically first**: `sqlglot.transpile` handles the bulk (types, functions, quoting, `QUALIFY`, `ILIKE`, date functions). Only constructs it flags go to the LLM with the original SQL as context. Cheaper, auditable, and the failure mode is a listed unsupported construct, not a hallucinated rewrite.
4. **Storage decision is explicit**: for a re-platform the target storage is Iceberg on the customer's object store; both Snowflake and Databricks can read it, which is what makes the move reversible and is our differentiator versus either vendor's migration tooling.
5. **Acceptance = equivalence, not green build**: run old and new side by side on a fixed window and compare with `audit_helper.compare_relations` / `data-diff` (repo `~/git/data-diff` already on EC2). Match % per table and per column is the deliverable; the build/repair loop feeds mismatched columns back to the generator, exactly as Week 3's diff validator does for stock.
6. **Orchestration translation** is a separate emitter: Control-M / Airflow / Databricks Workflows from the same plan DAG (`dag_emitter` with a template per scheduler; platform-be's Control-M renderer is the precedent).

## 5. What this changes in the current code (Week 5+ scope)

- `pipeline_planner/brief.py`: `PipelineBrief` pydantic model; `instruction_ingest` node (parse + extract + cite + validate); HITL interrupt before plan.
- `pipeline_planner/introspectors/`: split `schema_introspector.py` into `postgres.py`, `snowflake.py`, `dbt_project.py`, `databricks.py`; all return `SourceCatalog` (+ optional `LegacyPlan`).
- `pipeline_planner/translate.py`: sqlglot transpile pass with an "unsupported constructs" report.
- `sys_ai_prompt` migration (registry for conventions/rules); `sys_ai_pipeline.brief_json` column.
- Acceptance: `diff_validator` generalised to any (old relation, new relation, keys, window) — the Week 3 component already has this shape.

## 6. Decisions and alternatives

| Decision | Alternative rejected | Why |
|---|---|---|
| Typed `PipelineBrief` extracted from documents | paste the document into the prompt | injection risk, not reviewable, not storable, not enforceable |
| Rules must map to an enforcement mechanism | trust the LLM to "follow" rules | the demo proved green ≠ correct; enforcement is Python's job |
| sqlglot transpile first, LLM second | LLM rewrites everything | cost, auditability, deterministic failure list; LLM still covers the long tail |
| Equivalence diff as acceptance | build green | same lesson as Week 2/3 |
| Prompt registry in DB | prompt files in repo | CLAUDE.md DB-driven rule; per-tenant scope; approval trail |

## 7. Related
- `docs/architecture/column-level-lineage.md` (sqlglot lineage, reused for legacy catalogs)
- `datapai-platform-be/agents/controlm_converter/` (scheduler translation precedent)
- `datapai-platform-be/agents/file_ingest_agent.py` (document upload path)
- `~/git/data-diff` (equivalence tooling on EC2)
