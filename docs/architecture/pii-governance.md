# PII governance on top of column-level lineage — design

**Date:** 2026-09-10 · **Status:** detection, propagation, inventory, DB-driven policy (`sys_common_config` type `pii`, migration 005), guardrail integration (meta.datapai) shipped; masking DDL dry-run + gated apply; HITL approval flow next
**Why it matters:** every licensed financial business in Australia reports to ASIC / APRA / AUSTRAC / ATO and is bound by the Privacy Act APPs.
"Where is the personal data, where did it flow, who can see it, prove it" is the compliance officer's question. Catalog vendors answer it
with a platform; we answer it from the dbt artefacts the customer already has, on any engine, generated on every build.

## 1. Pipeline
```
detect            propagate                      decide                         enforce                       prove
raw columns  ──►  column_lineage (sqlglot)  ──►  policy (level → strategy,  ──►  engine adapter (masking   ──►  inventory report,
name+value        max(sensitivity) over          allowed roles) — per            DDL), dbt tags/meta,          sys_ai_column_lineage,
heuristics        every upstream hop             tenant in sys_common_config     guardrail restricted list     lineage page colour bar
```
| Stage | Where | State |
|---|---|---|
| Detect | `_vendored/pii_classifier.py`: column-name heuristics (credentials, gov IDs, cards, bank, health, contact, identity, demographics, location) + value-regex on samples; numeric types skipped | shipped |
| Propagate | `column_lineage.py`: sensitivity = max over upstream columns, each hop; roots = declared sources, seeds, or untraced nodes classified by name | shipped (dbt-demo: 49 sensitive columns from 15 roots, e.g. `customer.email` → `stg_customer` → `dim_customer` → `obt_invoice.customer_email`) |
| Decide | `pii.load_policy()` reads `sys_common_config` config_type `pii` (strategy/roles per level, `pii_masking_apply_mode`, `pii_hitl_required_levels`, guardrail answer modes); code defaults are the fallback. One policy per deployment (single-customer VPC, no tenants) | shipped (migration 005 applied) |
| Enforce | `pii.masking_ddl()` Snowflake masking DDL; `pii.apply_masking()` executes ONLY when `pii_masking_apply_mode='apply'`, skipping HITL levels; **guardrail**: `annotate_yaml` writes `meta.datapai.{pii,security_class,masking_rule,answer_mode,allowed_in_output}` per column and `{contains_pii,sensitivity_level,default_answer_mode}` per model — the vocabulary `guardrail/policy_compiler.py` reads. Verified: `stg_customer.customer_email` → pii direct / hash / masked in the platform catalog | shipped |
| Prove | `pii.report()` + `to_markdown()`: inventory by level, by model, roots with blast radius; `sys_ai_column_lineage.to_sensitivity`; lineage page colour bar; `v_sys_ai_pii_columns` (migration 004) | shipped |

## 2. What is new versus the usual "PII scanner"
- **Propagation, not just detection.** A scanner tags `customer.email`. Lineage tells you the 6 mart columns that are *derived* from it, including the renamed `customer_email` in an OBT that a scanner would still catch by name — and the hashed/concatenated ones it would not.
- **Blast radius per root.** The report ranks roots by how many downstream columns inherit them: the shortest path to reducing exposure is masking at the root.
- **Generated pipelines get it for free.** Because we produce staging, the conform/mask step can be *planned*: the planner already receives sensitivity per source column and is told to mask/hash non-PUBLIC columns in staging. Enforcement at generation time, not after the fact.
- **Engine-portable.** Masking policy is decided once; the adapter emits Snowflake `MASKING POLICY`, Databricks `ALTER TABLE … ALTER COLUMN … SET MASK`, Athena/Lake Formation data filters. Same lineage, same inventory.

## 3. Regulatory mapping (Australia first)
| Obligation | What the framework produces |
|---|---|
| Privacy Act 1988, APP 1/6/11 (open management, use/disclosure, security) | inventory of personal-information columns per system + who can see them (policy roles) + masking evidence |
| APRA CPS 234 (information security) / CPS 230 (operational risk) | data classification per column, lineage from source to report field, reproducible per build (`run_id`) |
| ASIC derivative transaction / breach reporting | column lineage from source systems to each reported field; PII in reporting marts flagged |
| AUSTRAC record-keeping (7 yrs) | classification + retention rule slot in the brief; Iceberg snapshots give point-in-time evidence |
| GDPR (EU clients of AU brokers) Art. 30 records of processing | same inventory export, per pipeline |

## 3a. Guardrail integration — how it behaves
The platform guardrail is **deny-by-default**: a model with no `meta.datapai.ai_access_level` (or `deny`) has every column
short-circuited to DENY before any pii meta is read. Our lineage/PII steps therefore never approve a model for AI; they only
attach the classification. A human opts models in explicitly (`lineage-project --set-ai-access restricted|approved`,
or by editing the YAML). Once eligible, PII columns answer `masked` with the configured `masking_rule`, PII_HIGH `deny`,
FINANCIAL `aggregate_only`. Models without a schema YAML (e.g. chinook `obt_invoice`) are invisible to the guardrail —
`dbt docs`/codegen-generated YAML is a prerequisite.

## 4. Gaps and next steps (ordered)
1. ~~Policy in DB~~ done (migration 005). 2. ~~Guardrail integration~~ done (meta.datapai).
2. **Apply masking** as a pipeline node after `build_repair`, HITL approval for `pii_hitl_required_levels` via the platform HITL service; record applied policies (`sys_ai_masking_applied`).
3. **Generate schema YAML for models that lack it** (dbt-codegen `generate_model_yaml`) so every model is governable.
4. **Planner rule hardening**: non-PUBLIC source column ⇒ staging must hash/redact unless the brief whitelists it; validator enforces (today it is a prompt instruction).
5. **Detection depth**: optional Presidio NER on samples; customer-specific patterns (TFN, Medicare, ABN, passport formats) as `sys_common_config` regex rows — AU identifiers are not in the default set yet.
6. **Value-level checks**: dbt_expectations tests generated for sensitive columns (`expect_column_values_to_match_regex` for hashed form) so a regression that un-hashes a column fails the build.
7. **Report distribution**: PII inventory as a dbt exposure + PDF export for the compliance pack; OpenLineage facets with sensitivity for catalogs.

## 5. Related
- `docs/architecture/column-level-lineage.md`, `docs/demo/2026-09-10-column-level-lineage-demo.md`
- `migrations/002_sys_ai_column_lineage.sql`, `migrations/004_v_sys_ai_pii_columns.sql`
- platform-be: `agents/dbt_guardrail_agent.py`, `guardrail/governed_action.py`, `agents/ai_governance_guardrail/`
- dbt-demo `seeds/ai_mart/frameworks/` (APRA CPS 230 control seeds) — candidates to join classification to controls
