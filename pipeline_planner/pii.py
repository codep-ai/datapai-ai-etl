"""
pii — PII / sensitivity inventory and enforcement artefacts derived from column-level lineage.

Detection (where a sensitivity comes from)      → _vendored/pii_classifier (name heuristics + value regex)
Propagation (where it flows)                    → column_lineage (max over upstream columns, every hop)
This module (what to DO about it):
  report()          inventory: sensitive root columns → every downstream column, by level, by model
  masking_ddl()     engine-specific masking policy DDL (Snowflake now; adapter-keyed for Databricks/Athena later)
  restricted_list() the restricted-column set for the governance gate (agents/dbt_guardrail_agent)
  dbt_patch()       tags/meta to add to model YAML so dbt tests, docs and Lightdash see it

Everything is deterministic and derived from the lineage result; no LLM.
Design: docs/architecture/pii-governance.md
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, List, Optional

import logging
import os

from ._vendored.pii_classifier import SENSITIVITY_RANK

logger = logging.getLogger(__name__)
LEVELS = ["PII_HIGH", "FINANCIAL", "PII_MEDIUM", "PII_LOW"]

# Default masking per level. Engine adapters override the SQL; the *policy* (who sees what) is
# framework-level and should move to sys_common_config per tenant (DB-driven defaults).
DEFAULT_POLICY = {
    "PII_HIGH":   {"strategy": "redact",  "allowed_roles": ["PII_ADMIN"]},
    "FINANCIAL":  {"strategy": "redact",  "allowed_roles": ["PII_ADMIN", "FINANCE_ANALYST"]},
    "PII_MEDIUM": {"strategy": "hash",    "allowed_roles": ["PII_ADMIN", "DATA_ENGINEER"]},
    "PII_LOW":    {"strategy": "partial", "allowed_roles": ["PII_ADMIN", "DATA_ENGINEER", "ANALYST"]},
}


# Mapping to datapai-platform-be/guardrail/metadata_schema.py enums (read by PolicyCompiler from meta.datapai.*)
_GUARDRAIL_MAP = {
    "PII_HIGH":   {"pii": "direct",   "security_class": "restricted",   "masking_rule": "redact"},
    "FINANCIAL":  {"pii": "none",     "security_class": "confidential", "masking_rule": "redact"},
    "PII_MEDIUM": {"pii": "direct",   "security_class": "confidential", "masking_rule": "hash"},
    "PII_LOW":    {"pii": "indirect", "security_class": "internal",     "masking_rule": "partial_mask"},
}
_STRATEGY_TO_RULE = {"redact": "redact", "hash": "hash", "partial": "partial_mask", "tokenise": "tokenise"}
# Permissive by default (Donny 2026-09-10: "don't make too many restrictions as default unless the customer requires").
# Columns are CLASSIFIED and the recommended masking strategy is recorded, but AI answers are full and nothing is
# blocked until a customer flips sys_common_config (pii_guardrail_answer_mode_<LEVEL>, pii_masking_apply_mode, pii_hitl_required_levels).
_DEFAULT_ANSWER_MODE = {"PII_HIGH": "full", "FINANCIAL": "full", "PII_MEDIUM": "full", "PII_LOW": "full"}
_MODEL_SENSITIVITY = {"PII_HIGH": "restricted", "FINANCIAL": "confidential", "PII_MEDIUM": "confidential", "PII_LOW": "internal"}

_POLICY_CACHE: Optional[Dict[str, Any]] = None


def load_policy(prefix: str = "FRAMEWORK_DB", refresh: bool = False) -> Dict[str, Any]:
    """
    Policy from datapai.sys_common_config (config_type='pii', migration 005) with code defaults as fallback.
    Single deployment per customer VPC → one policy, keyed by level. Returns
      {"levels": {LEVEL: {strategy, allowed_roles, answer_mode}}, "apply_mode": "dry_run|apply",
       "hitl_levels": [...], "source": "db|defaults"}
    """
    global _POLICY_CACHE
    if _POLICY_CACHE and not refresh:
        return _POLICY_CACHE
    levels = {lvl: {**DEFAULT_POLICY[lvl], "answer_mode": _DEFAULT_ANSWER_MODE[lvl]} for lvl in LEVELS}
    pol: Dict[str, Any] = {"levels": levels, "apply_mode": "dry_run", "hitl_levels": [], "source": "defaults"}
    try:
        import psycopg2
        conn = psycopg2.connect(host=os.getenv(f"{prefix}_HOST", "localhost"), port=int(os.getenv(f"{prefix}_PORT", "5433")),
                                dbname=os.getenv(f"{prefix}_NAME", "postgres"), user=os.getenv(f"{prefix}_USER", "postgres"),
                                password=os.getenv(f"{prefix}_PASSWORD", "postgres"), connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT config_key, config_value FROM datapai.sys_common_config WHERE config_type='pii' "
                    "AND (effective_to IS NULL OR effective_to >= CURRENT_DATE)")
        rows = dict(cur.fetchall()); cur.close(); conn.close()
        if rows:
            for lvl in LEVELS:
                if f"pii_{lvl}_strategy" in rows: levels[lvl]["strategy"] = rows[f"pii_{lvl}_strategy"].strip()
                if f"pii_{lvl}_roles" in rows: levels[lvl]["allowed_roles"] = json.loads(rows[f"pii_{lvl}_roles"])
                if f"pii_guardrail_answer_mode_{lvl}" in rows: levels[lvl]["answer_mode"] = rows[f"pii_guardrail_answer_mode_{lvl}"].strip()
            pol["apply_mode"] = rows.get("pii_masking_apply_mode", "dry_run").strip()
            if "pii_hitl_required_levels" in rows: pol["hitl_levels"] = json.loads(rows["pii_hitl_required_levels"])
            pol["source"] = "db"
    except Exception as exc:  # availability over strictness
        logger.warning("pii policy: sys_common_config unavailable (%s); using code defaults", exc)
    _POLICY_CACHE = pol
    return pol


def guardrail_column_meta(level: str, policy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """meta.datapai block for ONE column, in the platform guardrail's vocabulary (PolicyCompiler._col_meta)."""
    if level == "PUBLIC":
        return {}
    pol = (policy or load_policy())["levels"][level]
    base = dict(_GUARDRAIL_MAP[level])
    base["masking_rule"] = _STRATEGY_TO_RULE.get(pol["strategy"], base["masking_rule"])
    base["answer_mode"] = pol["answer_mode"]
    base["allowed_in_output"] = pol["answer_mode"] in ("masked", "full")
    base["allowed_in_export"] = pol["answer_mode"] == "full"
    base["ai_sensitivity_source"] = "datapai-ai-etl column lineage"
    return base


def guardrail_model_meta(levels_in_model: List[str]) -> Dict[str, Any]:
    """meta.datapai block for a model that carries sensitive columns (PolicyCompiler._model_meta)."""
    non_public = [l for l in levels_in_model if l != "PUBLIC"]
    if not non_public:
        return {}
    top = max(non_public, key=lambda l: SENSITIVITY_RANK[l])
    modes = [load_policy()["levels"][l]["answer_mode"] for l in non_public]
    order = ["full", "masked", "aggregate_only", "metadata_only", "deny"]
    return {"contains_pii": any(l != "FINANCIAL" for l in non_public), "sensitivity_level": _MODEL_SENSITIVITY[top],
            "default_answer_mode": max(modes, key=order.index)}


def report(lineage: Dict[str, Any]) -> Dict[str, Any]:
    """Sensitive-column inventory with provenance. Roots = columns whose sensitivity is not inherited."""
    cols = lineage["columns"]
    by_level: Dict[str, List[str]] = defaultdict(list)
    by_model: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    roots: Dict[str, Dict[str, Any]] = {}
    downstream_of_root: Dict[str, List[str]] = defaultdict(list)

    for model, cs in cols.items():
        for col, info in cs.items():
            lvl = info.get("sensitivity", "PUBLIC")
            if lvl == "PUBLIC":
                continue
            key = f"{model}.{col}"
            by_level[lvl].append(key)
            by_model[model][lvl] += 1
            # walk to the raw roots that carry the sensitivity
            stack = [(model, col)]
            seen = set()
            while stack:
                m, c = stack.pop()
                if (m, c) in seen:
                    continue
                seen.add((m, c))
                srcs = cols.get(m, {}).get(c, {}).get("sources", [])
                if not srcs or all(s["node"] not in cols for s in srcs):
                    for s in (srcs or [{"node": m, "column": c}]):
                        rk = f"{s['node']}.{s['column']}"
                        roots.setdefault(rk, {"node": s["node"], "column": s["column"], "sensitivity": lvl, "downstream": 0})
                        if key not in downstream_of_root[rk]:
                            downstream_of_root[rk].append(key)
                for s in srcs:
                    if s["node"] in cols:
                        stack.append((s["node"], s["column"]))
    for rk, r in roots.items():
        r["downstream"] = len(downstream_of_root[rk])
        r["downstream_columns"] = sorted(downstream_of_root[rk])

    total = sum(len(v) for v in by_level.values())
    return {
        "total_sensitive_columns": total,
        "by_level": {lvl: sorted(by_level.get(lvl, [])) for lvl in LEVELS if by_level.get(lvl)},
        "by_model": {m: dict(v) for m, v in sorted(by_model.items())},
        "roots": sorted(roots.values(), key=lambda r: (-SENSITIVITY_RANK[r["sensitivity"]], -r["downstream"], r["node"])),
        "models_with_pii": sorted(by_model),
    }


def restricted_list(lineage: Dict[str, Any], min_level: str = "PII_LOW") -> List[Dict[str, str]]:
    """Restricted columns for the governance gate: [{model, column, sensitivity}] at or above min_level."""
    thr = SENSITIVITY_RANK[min_level]
    out = []
    for model, cs in lineage["columns"].items():
        for col, info in cs.items():
            lvl = info.get("sensitivity", "PUBLIC")
            if SENSITIVITY_RANK.get(lvl, 0) >= thr and lvl != "PUBLIC":
                out.append({"model": model, "column": col, "sensitivity": lvl})
    return sorted(out, key=lambda r: (r["model"], r["column"]))


# ── engine adapters for masking ───────────────────────────────────────────────────────────────────

def _snowflake_policy_sql(level: str, strategy: str, roles: List[str], schema: str) -> str:
    role_pred = " OR ".join(f"CURRENT_ROLE() = '{r}'" for r in roles) or "FALSE"
    body = {
        "redact":  "'***REDACTED***'",
        "hash":    "'H:' || LEFT(SHA2(val, 256), 16)",
        "partial": "LEFT(val, 2) || REPEAT('*', GREATEST(LENGTH(val) - 2, 0))",
    }[strategy]
    return (f"CREATE MASKING POLICY IF NOT EXISTS {schema}.MASK_{level} AS (val VARCHAR) RETURNS VARCHAR ->\n"
            f"  CASE WHEN {role_pred} THEN val ELSE {body} END;")


def masking_ddl(lineage: Dict[str, Any], relations: Dict[str, str], engine: str = "snowflake",
                policy: Optional[Dict[str, Dict[str, Any]]] = None, min_level: str = "PII_LOW") -> List[str]:
    """
    DDL to apply column masking for every sensitive column of the selected models.
    relations: {model_name: "DB.SCHEMA.TABLE"} (from manifest). Returns statements; caller decides to run.
    Only VARCHAR-typed masking is emitted (numeric FINANCIAL columns need a per-type policy → listed as TODO comments).
    """
    policy = policy or load_policy()["levels"]
    if engine != "snowflake":
        raise NotImplementedError(f"masking adapter for {engine} not implemented yet (Databricks: column masks via ALTER TABLE ... SET MASK; Athena/Lake Formation: data filters)")
    stmts: List[str] = []
    schemas = {rel.rsplit(".", 1)[0] for rel in relations.values()}
    levels_used = {r["sensitivity"] for r in restricted_list(lineage, min_level)}
    for sch in sorted(schemas):
        for lvl in LEVELS:
            if lvl in levels_used:
                p = policy[lvl]
                stmts.append(_snowflake_policy_sql(lvl, p["strategy"], p["allowed_roles"], sch))
    for r in restricted_list(lineage, min_level):
        rel = relations.get(r["model"])
        if not rel:
            continue
        sch = rel.rsplit(".", 1)[0]
        stmts.append(f'ALTER TABLE {rel} MODIFY COLUMN "{r["column"].upper()}" SET MASKING POLICY {sch}.MASK_{r["sensitivity"]};'
                     f'  -- {r["sensitivity"]} (VARCHAR assumed; verify type)')
    return stmts


def apply_masking(statements: List[str], policy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute masking DDL ONLY when sys_common_config pii_masking_apply_mode='apply'. Statements touching HITL levels
    are skipped and reported (approval flow lands with the HITL node). Returns {applied, skipped_hitl, errors}.
    """
    pol = policy or load_policy()
    if pol["apply_mode"] != "apply":
        return {"applied": 0, "skipped": len(statements), "reason": f"apply_mode={pol['apply_mode']} (set pii_masking_apply_mode='apply' in sys_common_config)"}
    from . import snowflake_conn
    conn = snowflake_conn.connect(); cur = conn.cursor()
    applied, skipped_hitl, errors = 0, [], []
    try:
        for st in statements:
            if any(f"MASK_{lvl}" in st for lvl in pol["hitl_levels"]) and st.strip().upper().startswith("ALTER"):
                skipped_hitl.append(st); continue
            try:
                cur.execute(st.split("  --")[0]); applied += 1
            except Exception as exc:
                errors.append({"sql": st[:160], "error": str(exc)[:200]})
        conn.commit()
    finally:
        cur.close(); conn.close()
    return {"applied": applied, "skipped_hitl": len(skipped_hitl), "errors": errors}


def dbt_patch(lineage: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Per model: tags + column meta to add so PII is visible to dbt docs / Lightdash / selectors (`dbt build --select tag:pii`)."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in restricted_list(lineage):
        m = out.setdefault(r["model"], {"tags": ["pii"], "columns": {}})
        m["columns"][r["column"]] = {"meta": {"sensitivity": r["sensitivity"], "datapai": guardrail_column_meta(r["sensitivity"])}, "tags": ["pii", r["sensitivity"].lower()]}
    return out


def to_markdown(rep: Dict[str, Any], title: str = "PII inventory") -> str:
    lines = [f"# {title}", "", f"Sensitive columns: **{rep['total_sensitive_columns']}** across {len(rep['models_with_pii'])} models", ""]
    lines.append("| Level | Columns |"); lines.append("|---|---:|")
    for lvl, cols in rep["by_level"].items():
        lines.append(f"| {lvl} | {len(cols)} |")
    lines += ["", "## Sources of sensitivity (roots) and blast radius", "", "| Root column | Level | Downstream columns |", "|---|---|---:|"]
    for r in rep["roots"][:40]:
        lines.append(f"| `{r['node']}.{r['column']}` | {r['sensitivity']} | {r['downstream']} |")
    lines += ["", "## Models carrying PII", "", "| Model | " + " | ".join(LEVELS) + " |", "|---|" + "---:|" * len(LEVELS)]
    for m, counts in rep["by_model"].items():
        lines.append(f"| {m} | " + " | ".join(str(counts.get(l, 0)) for l in LEVELS) + " |")
    return "\n".join(lines) + "\n"
