# Week 3a — contract enforcement, key conformance, dbt_expectations, column-level lineage

**Date:** 2026-09-10 · **Repo:** datapai-ai-etl (+ dbt-demo landing) · **Runtime:** EC2

## Why this stretch happened
Week 2's green build was hollow: the repair loop let the LLM shrink `int_price_with_indicators` to 14 columns, the screener
filled the rest with nulls, and `fct_daily_price` lost every ASX row (prices `BHP.AX` vs ticker master `BHP`, inner join).
Donny's framing: the framework must *generate the cleansing rule* humans used to write by hand, and green must mean complete.
Framework, not stock, is the product; stock is the practice domain.

## Shipped
| Area | Change | Evidence |
|---|---|---|
| Generator contract | `contract_violations()`: missing contract columns, `cast(null as …)` placeholders, INNER JOIN in facts/intermediates → re-prompt ≤2 before dbt | `fct_daily_price: null placeholders for contract columns: ['volume']` caught and fixed pre-build; int model back to 18 cols in the first run |
| Row-count reconciliation | `dbt_utils.equal_rowcount` auto-added for facts/intermediates at an upstream's grain | in generated `_fct_*.yml` |
| FK coverage | `verify_foreign_keys()`: sampled `match_rate`, per-group breakdown (`TABLESAMPLE` so every partition contributes), alternative parent columns | `prices(ticker,exchange)→ticker_universe`: 0% raw; `yf_symbol` 100% |
| **Conform rules** | tests normalisations of the child key (`split_part`, regex suffix strip, upper/trim, …) against the parent; reports the winner | `split_part(ticker,'.',1)` matches 100% (ASX/HKEX/LSE/… per group) |
| Planner | must apply conform rule in staging (`<col>_raw` + conformed `<col>`), join on conformed key, add relationships test | plan `stg_prices` logic: `ticker as ticker_raw, split_part(ticker_raw,'.',1) as ticker` — generated with no human rule |
| dbt_expectations | `TestSpec` accepts any `expect_*` test + `params` + `severity`; generator emits column- and table-level expectations | package already in dbt-demo; planner emitted none yet (prompt nudge too weak — pending) |
| **Column-level lineage** | `column_lineage.py` (sqlglot over `manifest.json`), CLI `lineage --mermaid --annotate`, YAML `meta.lineage`/`meta.sensitivity` write-back | 11 models, 166 edges, 0 hard unresolved; demo doc `docs/demo/2026-09-10-column-level-lineage-demo.md` |
| Design docs | `docs/architecture/instruction-files-and-replatform.md` (PipelineBrief from docx/pdf/xlsx; re-platform via estate introspectors + sqlglot transpile + parity diff; Companies A/B/C) | |

## Gotchas
- sqlglot needs a **normalising** `MappingSchema` (dialect-aware case) or nothing matches and `select *` never expands.
- CTE names leak as lineage leaves for literal-derived columns (date spine) → filter leaves to known relations.
- Schema-strict lineage failing on a model = the model references an undeclared upstream column → real generator defect, use as pre-build signal.
- `str.format` on regex `{1,3}` → KeyError; escape braces.
- `LIMIT n` on a LIST-partitioned table samples one partition → `TABLESAMPLE SYSTEM`.
- Iceberg on Snowflake: timestamps must be `TIMESTAMP_NTZ(6)`; scale 9 fails at create.

## State of the stock build (honest)
- Last run: 31/34 PASS, `fct_screener_metrics` RED (undeclared upstream column). Per Donny: stock is a sample, temp-fix and move on.
- `fct_daily_price` keeps all rows now (LEFT JOIN + equal_rowcount); conformed ticker key in staging.

## Pending
- Planner rarely emits dbt_expectations → add deterministic baseline expectations from the profile (ranges from min/max, sets from low-cardinality samples).
- Generalise per Donny's goal: `PipelineBrief` (engine as parameter, Iceberg fixed), engine adapters (Snowflake / Athena / Databricks / ClickHouse), remove hardcoded Snowflake/`DATAPAI`/`STOCK`/paths; document ingest node.
- Week 3 originals: diff validator, `sys_ai_pipeline` apply + register, DAG emitter. OpenLineage emit; lineage node in graph.
- `git push` (both repos) needs confirmation.
