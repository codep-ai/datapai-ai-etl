# TODO — DATAP.AI Data & AI layer (backlog, 2026-09-11)

Order agreed with Donny: **finish the front end first**, then the backend items below. Front-end work is local
(`~/git/datapai-homepage-next`, `npm run dev -- -p 3000`); deploy to EC2 only on approval.

## A. Front end (in progress, local only — EC2 still serves the original home)
- [ ] Home v3 sign-off: compare `/`, `/legacy-home`, `/legacy-home-v2`; decide what to keep from the original; delete the losing snapshots.
- [ ] Product pages adopt the dark palette + gradient keyword style so the site reads as one: `/data-governance`, `/data-ai-pipelines`, `/ai-governance`, `/architecture`, `/solutions`, `/integrations`.
- [ ] Hero diagram: proper Athena / ClickHouse / Trino logos (text today); mobile stacking check.
- [ ] Cross-links: `/ai-governance` → `/data-governance`; `/architecture` → `/data-ai-pipelines`.
- [ ] Copy pass for dbt-neutral wording on every page ("no dbt yet? you get one" / "reads what you have"); no vendor comparisons in public; no fixed-time or fixed-price promises — effort reduction only.
- [ ] Localisation of the site itself (8 APAC languages) — at least hero + capability cards; language switch in header.
- [ ] Deploy approved version to EC2 (additive rsync excl. `.next/`, `npm run build`, `systemctl restart homepage` with confirmation), verify, journal.

## B. Engine neutrality (proves "any engine")
- [ ] **Athena adapter** for the generator: dbt-athena ≥1.11, `table_type='iceberg'`, `format='parquet'`, Athena engine v3, **unique S3 location per table** (`s3_data_dir` / per-model location), Glue as catalog. Run the stock domain on Athena over the same Iceberg files.
- [ ] Engine adapter interface: dbt config fragment + sqlglot dialect + connection per engine (Snowflake, Athena/Trino, Databricks, ClickHouse, BigQuery). Remove remaining Snowflake / `DATAPAI` / `STOCK` defaults from code.
- [ ] Catalog choice: Glue or REST catalog instead of Snowflake-managed catalog for true neutrality (needed for Company A/B).

## C. Lineage interoperability
- [ ] **OpenLineage emitter** (`platform-be/lineage/openlineage_emit.py`): one START/COMPLETE run per model per build; `schema` + `columnLineage` facets + custom sensitivity facet; dataset namespaces `snowflake://…`, `awsathena://…`; HTTP transport (Marquez / DataHub / OpenMetadata) from `sys_common_config`; CLI `--emit-openlineage`; hook into the build loop so every green build republishes. Prove on a local Marquez container on EC2.
- [ ] **DataZone transport** (AWS lineage API, OpenLineage-shaped events) — Company A's catalog. Needs their IAM.
- [ ] Lineage page reads `sys_ai_column_lineage` via a small governance-api endpoint (today: manifest / `target/lineage.json`).
- [ ] Generate schema YAML for models without one (dbt-codegen) so every model is governable by the guardrail.

## D. Generator quality (stock is the test bed, not the product)
- [ ] Wire `lineage` and `pii` into the LangGraph run after `build_repair`; use schema-strict lineage failures as a pre-build repair signal.
- [ ] Planner rarely emits dbt_expectations → deterministic baseline expectations from the profile (ranges, sets, nulls).
- [ ] Planner validator: non-PUBLIC source column ⇒ staging hashes/redacts unless whitelisted (today a prompt instruction).
- [ ] Fix the screener model's undeclared-column defect; diff validator vs `ta_indicators` (30-day window, ≥95%).
- [ ] `PipelineBrief` from instruction documents (docx / pdf / xlsx / text) with citations + HITL; `domains/stock.json` becomes a brief.
- [ ] Estate introspectors for non-dbt sources: Snowflake views/tasks/procedures, Glue jobs, **Dataflow** jobs, Databricks notebooks → `LegacyPlan`; sqlglot transpile first, LLM for the tail; parity diff as acceptance.
- [ ] Sovereign AI switch: full run on Ollama or Bedrock Sydney, no external calls, recorded.

## E. Governance
- [ ] Apply masking as a pipeline node, HITL-gated for `pii_hitl_required_levels`; record applied policies.
- [ ] AU identifiers (TFN, Medicare, ABN) as configurable detection patterns; optional Presidio.
- [ ] Compliance pack export (lineage + PII inventory + test results per run) as PDF; regulatory mart with a regulator schema as contract.

## F. Commercial / positioning (from 2026-09-11 discussions)
- Layer on top of the customer's platform; never "replace". Iceberg core; any engine; sovereignty in the customer's cloud and region; APAC localisation.
- Encourage dbt, never assume it. Non-dbt teams (Glue, Dataflow, notebooks, SQL) are a primary market: "convert to a governed dbt project on Iceberg" with AI doing the bulk of the effort.
- Engagements are effort-based, not fixed price or fixed time; delivery clock starts when access is granted.
- Company A (GCP Dataflow → AWS Iceberg/Glue/Athena, DataZone catalog): both — we generate pipelines + lineage, publish into DataZone via OpenLineage.
- No public comparison with dbt Cloud; dbt Cloud → Core is a private conversation.
