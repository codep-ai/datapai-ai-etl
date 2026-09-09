# Column-level lineage for AI-generated dbt pipelines

**Date:** 2026-09-09
**Question (Donny):** can dbt-core give us column-level lineage, or do we need SQLMesh or another project?
**Decision:** keep dbt-core; add a thin `sqlglot`-based lineage step of our own. Do not adopt SQLMesh for the MVP.

## 1. What dbt gives you today

| | Table-level | Column-level |
|---|---|---|
| dbt-core (1.9 / 1.10) | ✅ `manifest.json` `depends_on.nodes`, `ref()` graph, `dbt docs` DAG | ❌ none. dbt-core parses Jinja, not SQL; it never resolves `select` lists |
| dbt Cloud (Explorer) | ✅ | ✅ but proprietary, cloud-only, per-seat — exactly the licence trap our pitch attacks |
| dbt `catalog.json` (`dbt docs generate`) | column names + types per node (from the warehouse) | ❌ no edges |

So the answer is **no: dbt-core alone cannot produce column-level lineage.** It does give us the two inputs we need: compiled SQL per node (`compiled_code`) and per-node column lists (`catalog.json`).

## 2. Options considered

| Option | How | Pros | Cons | Verdict |
|---|---|---|---|---|
| **A. sqlglot on compiled dbt SQL (own module)** | after `dbt compile`, walk `manifest.json`, call `sqlglot.lineage.lineage(column, compiled_sql, schema=catalog, dialect='snowflake')` per output column | pure Python, ~150 lines, multi-dialect (snowflake / trino-athena / duckdb / postgres = our engine-portability story), output is *our* JSON so it lands in `sys_ai_pipeline`, TraceLedger and the governance gate | must feed sqlglot a schema for `select *` resolution (we have `catalog.json` + our `SourceCatalog`); Jinja must be compiled first (it is, by `dbt compile`) | **Chosen** |
| B. `dbt-colibri` / `recce` / `dbt-column-lineage` (OSS, all sqlglot underneath) | pip install, point at `target/` | zero code | UI/report oriented; another moving dependency in the customer VPC; still sqlglot, so no capability we cannot get from A | use as a *reference implementation*, not a dependency |
| C. `sqllineage` (reata) | parse SQL files | mature | weaker dialect coverage than sqlglot for Snowflake/Trino; no schema-aware `select *` | no |
| D. **SQLMesh** (Tobiko → Fivetran) | replace/wrap dbt; built-in column lineage UI, virtual environments, `table_diff` | best-in-class lineage + plan/apply semantics | contradicts locked decisions (dbt-demo landing, Cosmos DAGs, `dbt_agent.py` reuse); every customer artefact would become SQLMesh-specific; Fivetran ownership is its own walled-garden risk; the dbt-project adapter mode still runs *their* engine | **not for MVP**; revisit at Week 12 only if dbt itself becomes the bottleneck |
| E. Catalog platforms (DataHub, OpenMetadata, Marquez/OpenLineage) | ingest dbt artefacts; they compute column lineage with sqlglot/sqllineage | customer may already run one | heavy infra; and for *our* loop we need lineage *inside* the generation graph, before anything is published | emit OpenLineage events later (Week 5+) from the same JSON |

## 3. Design (component 9, lands Week 3 alongside `dag_emitter` / `sys_ai_pipeline`)

```
build_loop (dbt compile → build)  ──►  target/manifest.json + target/catalog.json
                                              │
                                    column_lineage.py
                                              │
        for node in plan.build_order:
            sql    = manifest.nodes[node].compiled_code
            schema = {upstream: {col: type}}  from catalog.json ∪ SourceCatalog
            for col in catalog.nodes[node].columns:
                node_tree = sqlglot.lineage.lineage(col, sql, schema=schema, dialect=target_dialect)
                edges += [(upstream_table, upstream_col, node, col, transform_kind)]
                                              │
                         ┌────────────────────┼─────────────────────┐
              sys_ai_pipeline.lineage_json    TraceLedger rows     governance_bridge:
              (registry, diffable)            (audit)              PII propagation —
                                                                   sensitivity(col) = max(sensitivity(upstream cols))
```

* **Why this is the differentiator, not a nice-to-have:** the vendored `pii_classifier` tags *source* columns. Column lineage lets `governance_bridge` propagate `PII_HIGH/FINANCIAL/...` through every `int_*`/`fct_*` automatically and hand `guardrail_validate_sql` / `dbt_guardrail_agent.guardrail_list_restricted_columns` a complete restricted-column set for the *generated* marts. That is "APAC governance in the loop" made concrete.
* **Engine portability:** `dialect` comes from the pipeline target (`snowflake` now, `trino` for Athena in Week 5+). Same Iceberg files, same lineage code.
* **Repair loop synergy:** a lineage parse failure on a generated model is a cheap pre-`dbt build` signal that the SQL is malformed; feed it to `repair` before spending a Snowflake run.
* **Output contract:** `{edges: [{from_node, from_column, to_node, to_column, kind: passthrough|derived|aggregated}], unresolved: [...]}` stored per pipeline; also rendered into the plan's `lineage_edges()` view for the demo lineage graph.
* **Fallback (availability over strictness):** if sqlglot cannot parse a node, record `unresolved` and continue; table-level lineage from `manifest.json` is always present.

## 4. Consequences

* `sqlglot>=25` added to `requirements.txt` (installed in `venv_ai_etl`, 30.18.0 verified 2026-09-09).
* No change to the locked stack (dbt-core 1.9 on Snowflake-over-Iceberg, Cosmos, LangGraph).
* Column lineage is generated by us, owned by the customer, and readable by any catalog (OpenLineage emit later). dbt Cloud Explorer becomes unnecessary — that is the pitch.

## 5. Pointers
* sqlglot lineage API: `sqlglot.lineage.lineage(column, sql, schema=None, sources=None, dialect=None)`
* dbt artefacts: `target/manifest.json` (`nodes[*].compiled_code`, `depends_on`), `target/catalog.json` (`nodes[*].columns`)
* Governance consumers: `datapai-platform-be/agents/dbt_guardrail_agent.py`, `guardrail/governed_action.py`, `traceability/ledger.py`
