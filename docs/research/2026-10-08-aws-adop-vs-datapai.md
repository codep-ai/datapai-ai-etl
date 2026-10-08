# AWS ADOP (Agentic Data Onboarding Platform) vs DATAP.AI — what level to compare at, what to borrow

Source: github.com/aws-samples/sample-Agentic-Ai-Data-Operations (MIT-0, created 2026-03-26, last push 2026-10-05, 36 stars,
Python). Read 2026-10-08: README (1,129 lines), CLAUDE.md, TOOL_ROUTING.md, SKILLS.md, docs/determinism.md,
docs/governance-framework.md, contracts/v1, file tree (559 paths).

## What ADOP is

A **Claude Code project**, not a service. You open Claude Code in the repo, paste "onboard claims data from s3://… daily, HIPAA",
and a main agent plus sub-agents produce a `workloads/<dataset>/` folder: Glue PySpark scripts for Bronze→Silver→Gold, quality
checks, an Airflow (MWAA) DAG, a `semantic.yaml`, OWL/R2RML ontology files, tests, and optionally CloudFormation/Terraform/CDK.
Agents run in dev only; artifacts are promoted by CI/CD. AWS-only by construction: S3/S3 Tables, Glue, Athena, Lake Formation
LF-Tags, MWAA, SageMaker Catalog, KMS, 13 MCP servers (9 awslabs + 4 custom FastMCP).

Its strongest ideas (all in code, not slides):
1. **Human-in-the-loop discovery gate** (CLAUDE.md): no code until a human answers zone questions; "NEVER guess dedup / nulls /
   PII / schedule". Hook-enforced.
2. **Spec → template → artifact determinism** (`contracts/v1/*.schema.json`, `shared/codegen/renderer.py`, `drift_validator.py`):
   JSON-schema specs, Jinja2 StrictUndefined templates, 5-line provenance header (spec_hash, template_id, template_hash,
   schema_version, rendered_at), a PreToolUse hook that blocks any Write into `workloads/*/(scripts|dags|sql)` unless the
   renderer set a token. Same spec ⇒ bit-identical artifact; drift detectable.
3. **Cedar policy guardrails**: 16 forbid policies (no Bronze mutation, no quality bypass) + 7 agent-authorization policies;
   local cedarpy or AWS Verified Permissions.
4. **Three-layer agent tracing** and **cognitive traces** (`decisions[]` with alternatives and rejection reasons per sub-agent).
5. **Workload memory** per dataset; **prompt intelligence** agent that rewrites prompts from recurring failures.
6. **Tool routing doc + registry** validated in CI so the routing docs cannot drift from `.mcp.json`.
7. Model routing by regulation (Opus for HIPAA/SOX/PCI builds; Sonnet otherwise; Haiku for checks; Opus adversarial reviewer).

What it does **not** do: engine-neutral storage/compute (Glue PySpark, Athena, Redshift Spectrum only); dbt; cross-engine or
cross-repository lineage; migration from legacy ETL/schedulers; Text-to-SQL (explicitly deferred to AWS Semantic Layer); any
runtime product — it is a generator, and "status: design-complete, implementation pending" still sits in CLAUDE.md. 780 tests,
11 sample workloads, no customer evidence.

## Where to compare — three levels, and only one is a real overlap

| Level | ADOP | DATAP.AI | Verdict |
|---|---|---|---|
| **A. Pipeline generation from a natural-language ask** (new dataset → Bronze/Silver/Gold + DQ + DAG) | Core. Claude Code sub-agents, Glue PySpark output, AWS-only | `agents/*` pipeline agents (dlt ingest, dbt agent, governance gate) producing **dbt** models, any adapter; console UI | **Direct overlap.** Their generator is more mature as a *Claude Code workflow* (gate, determinism, Cedar, tracing). Ours is a *platform* (runs in the customer's account, UI, lineage, any engine). Different output: PySpark vs dbt. |
| **B. Governance** | PII detection → LF-Tags → Lake Formation TBAC → CloudTrail. Single-account AWS | Column lineage + classification from the dbt build, stitched across engines/repos; AI governance gate per LLM call; control library mapped to APRA/ISO/NIST | **Different axis.** ADOP governs *within* AWS catalog. We govern *across* engines and the AI call path. Not competitors; they could feed each other (their LF-Tags are a classification source for us). |
| **C. Migration / modernisation** (Informatica, Control-M, cross-engine re-platforming) | Absent | Converters measured on real exports; one Iceberg copy, four engines | **No overlap.** Our wedge is untouched by ADOP. |

So: compare at **Level A** only, and even there the honest framing is "ADOP shows how AWS thinks agentic pipeline generation
should be governed; DATAP.AI delivers the same outcome as dbt on any engine, inside a running platform, plus the layers ADOP
does not attempt (cross-engine lineage, migration, model-neutral AI governance)."

Competitive note for the website/sales: never position *against* ADOP. It is an aws-samples repo (MIT-0, no support, dev-only),
AWS is a channel partner, and its existence validates the category ("AWS itself says data operations should be agentified with
human gates and deterministic codegen"). Use it as a reference point, not a rival.

## What to leverage — ranked by value to us

1. **Spec → template → artifact with provenance headers and a drift validator.** Our Informatica converter already is
   deterministic, but the *generated dbt models* carry no spec hash / template version header, and nothing detects a hand edit.
   Borrow: 5-line header on every generated `.sql`/`.yml`/DAG (export file hash, mapping name, converter version, rendered_at);
   `--verify-drift` in `report`/`bench`. Cheap, and it is exactly the parity/acceptance evidence a bank asks for.
2. **The discovery gate as code, not prose.** Their CLAUDE.md gate + `check-discovery-gate.sh` hook is a cleaner version of our
   governance gate idea for the pipeline agents. Borrow the "NEVER infer" list for our dbt agent (dedup key, null policy, PII,
   schedule+timezone must be human-provided) and enforce it with a hook/validator, not a prompt.
3. **Cedar policies for agent authorisation.** We log every agent action; we do not *forbid* classes of action declaratively.
   16 forbid rules in Cedar evaluated locally is a day of work and a strong governance story (APRA CPS 234 "preventive controls").
4. **Cognitive traces (`decisions[]`) in the Tier 2 agent output.** Our `informatica_submit_result` takes models + resolved +
   remaining; add `decisions[]` (choice, alternatives, why rejected) so the human review reads the reasoning, and it lands in
   the governance evidence table.
5. **Tool-registry validation test.** Our `agents/tooling` registry has no test that the documented tools match the registered
   ones. Copy the pattern (18 tests in theirs).
6. **Regulation-specific prompt packs** (`runbooks/data-onboarding-agent/regulation/*`): GDPR/CCPA/HIPAA/SOX/PCI. We have
   APRA/ISO/NIST controls in the AI-governance library; map their structure for an **APRA CPS 234 / CPG 235 / Privacy Act**
   pack — the AU regulators they lack, which is our beachhead.
7. **Model routing by sensitivity** (Opus for compliance-critical, Sonnet default, Haiku checks). Our policy router selects by
   data residency; add a sensitivity tier → model tier rule.
8. **Workload memory per dataset** — lower priority; our run evidence tables partly cover it.

Not worth borrowing: Glue PySpark output (we are dbt-only by decision), MWAA assumptions, the 13-MCP-server AWS surface (our
platform is engine-neutral; AWS-native MCP would re-couple us), OWL/R2RML ontology handoff (no customer pull yet).

## How ADOP could become a channel asset rather than a competitor

- Offer **"ADOP → dbt"**: take a `workloads/<name>/config/*.yaml` spec (their JSON-schema contracts are public) and render dbt
  models + sources + tests from it with our writer. Their spec, our portable output. It demonstrates engine neutrality on
  AWS's own artefact and gives an AWS-aligned entry conversation.
- Position our cross-engine lineage as the layer that picks up where ADOP stops (single AWS account, Glue catalog only).

## Decision asked of Donny

Adopt items 1–4 into the converter and pipeline agents over the next sprint (each is a day or less, all deterministic code);
treat 5–7 as backlog; park 8 and the ADOP→dbt idea until a partner conversation wants it.
