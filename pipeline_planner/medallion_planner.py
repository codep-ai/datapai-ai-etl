"""
medallion_planner — LLM plans the whole raw → staging → intermediate → dim/fact DAG
as one JSON document; Python validates it and emits a topological build order.

Extends datapai-platform-be/agents/dbt_agent.py (DBT_AGENT_SYSTEM_PROMPT / DbtPlan)
from "sources + staging, independent per table" to a lineage-aware four-layer plan.
`_extract_first_json_object` is reused from dbt_agent when the platform checkout is
present, with a local fallback so the planner is importable on a source-only Mac.

Flow (per decision "whole-DAG plan first, then per-node generation"):
  request  = catalog.compact() + business goal + required outputs + constraints
  llm      = RouterChatClient (via llm.get_router_chat) — ChatFn injectable
  validate = MedallionPlan.validate_graph(catalog, required)  → errors
  repair   = re-prompt with the error list, ≤ MAX_PLAN_REPAIRS times
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from pydantic import ValidationError

from .llm import ChatFn, get_router_chat
from .models import MedallionPlan, SourceCatalog

logger = logging.getLogger(__name__)

MAX_PLAN_REPAIRS = int(os.getenv("AI_ETL_MAX_PLAN_REPAIRS", "2"))
FEW_SHOT_PATH = Path(__file__).parent / "few_shot" / "chinook_medallion_plan.json"

try:  # reuse platform helper when available
    import platform_init  # noqa: F401
    from agents.dbt_agent import _extract_first_json_object  # type: ignore
except Exception:  # pragma: no cover - source-only Mac
    def _extract_first_json_object(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:]
        start = text.find("{")
        if start == -1:
            raise ValueError("No JSON object found in model output.")
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        raise ValueError("Unbalanced JSON braces in model output.")


# ═══════════════════════════════════════════════════════════════════════════════
# Prompt
# ═══════════════════════════════════════════════════════════════════════════════

MEDALLION_PLANNER_SYSTEM_PROMPT = """
You are a senior analytics engineer designing a dbt medallion pipeline
(raw -> staging -> intermediate -> dimensions/facts) on an Apache Iceberg lakehouse.

You receive a SOURCE CATALOG (tables, columns, types, keys, inferred foreign keys,
row counts, PII sensitivity) plus a business goal and a list of REQUIRED output models.
Return ONLY one JSON object (no prose, no markdown fences) with this schema:

{
  "domain": "<domain>",
  "source_name": "src_<domain>",
  "target": "snowflake_iceberg",
  "tag": "<tag given in the request>",
  "models": [
    {
      "name": "stg_<table> | int_<topic> | dim_<entity> | fct_<event>",
      "layer": "staging | intermediate | dimension | fact",
      "description": "one sentence, states the grain",
      "sources": ["<raw table>"],          // staging ONLY, exactly one raw table
      "refs": ["<model name>"],            // every non-staging model; names must exist in this plan
      "grain": ["col", ...],               // columns that make one row unique
      "natural_key": ["col", ...],         // dims
      "surrogate_key": "<col>_key",        // dims (optional)
      "scd_type": 1,                       // dims: 1 or 2
      "foreign_keys": [{"column": "...", "ref_model": "dim_x", "ref_column": "..."}],   // facts
      "measures": ["..."],                 // facts / intermediate
      "columns": ["..."],                  // output column contract when a target_contract must be matched
      "logic": "precise plain-English transform spec a SQL generator can implement without guessing: joins (keys), filters, window functions with exact windows/ordering, formulas",
      "tests": [{"type": "not_null|unique|accepted_values|relationships|unique_combination_of_columns|<dbt_expectations test>",
                 "column": "...", "columns": [...], "values": [...], "to": "...", "field": "...", "params": {...}, "severity": "warn|error"}]
    }
  ],
  "notes": "assumptions and open questions"
}

Rules:
- Create exactly one staging model per raw table you use; staging has NO joins and NO refs.
- Non-staging models read ONLY via refs (never raw sources). Refs must be acyclic.
- Layer discipline: staging < intermediate < dimension < fact. A model may only ref models in the SAME or a
  LOWER layer: intermediate refs staging/intermediate only; dimensions ref staging/intermediate only;
  facts may ref intermediate and dimensions. Never make an intermediate depend on a dim or a fact.
- A fact that refs a dimension MUST join it and declare a foreign_keys entry (plus relationships test) for it.
- Every REQUIRED model must be present with exactly that name. Add intermediate models only when they simplify a fact.
- Facts join to dimensions on the dimension's key; declare foreign_keys and a relationships test for each.
- Dimensions: declare natural_key, prefer scd_type 1 unless history is clearly needed.
- dim_date must be generated from the fact date range (calendar spine), not copied from a source.
- When a target_contract is supplied for a required model, its "columns" MUST list every contract column
  in order and "logic" MUST define how each is computed. Windows must be exact (e.g. "SMA over the trailing
  20 trading days per (ticker, exchange) ordered by trade_date").
- Use only tables and columns that exist in the catalog. Never invent columns.
- DATA QUALITY / KEY CONFORMANCE: foreign keys carry a measured "match_rate", a per-group breakdown
  ("match_rate_by"), "better_join" alternative parent columns and "conform_rules" (normalisation expressions that
  were TESTED against the data, with their match rate). When a conform rule exists you MUST:
    (a) apply it in the STAGING model of the child table: keep the raw value as <col>_raw and expose the conformed
        value under the original column name (e.g. ticker_raw = 'BHP.AX', ticker = split_part(ticker_raw,'.',1));
        state the exact expression in "logic";
    (b) join downstream on the conformed key only;
    (c) add a relationships test from the consuming fact to the dimension so the conformance is verified on every run.
  If several rules/alternatives exist, choose the highest match_rate; if none reaches ~100%, say so in notes and
  LEFT JOIN so no rows are lost.
- Propagate PII sensitivity: if a source column is not PUBLIC, mention masking/hashing in the consuming model's logic.
- Baseline tests: not_null + unique (or unique_combination_of_columns) on every grain/key.
- DATA QUALITY (dbt_expectations is installed): add value-range and set tests derived from the catalog profile and
  domain knowledge, e.g. {"type":"expect_column_values_to_be_between","column":"rsi_14","params":{"min_value":0,"max_value":100}},
  {"type":"expect_column_values_to_be_between","column":"close","params":{"min_value":0,"strictly":true}},
  {"type":"expect_column_values_to_be_in_set","column":"exchange","values":[...from catalog samples...]},
  {"type":"expect_table_row_count_to_equal_other_table","to":"<upstream model at same grain>"} on facts/intermediates,
  {"type":"expect_column_values_to_not_be_null","column":"<measure>","severity":"warn"} for measures that may be null
  during warm-up windows. Use severity "warn" for statistical expectations, "error" for structural ones.
- Names: snake_case; prefixes stg_/int_/dim_/fct_ exactly.
- Output budget: return MINIFIED JSON (no indentation, no line breaks) and keep "description"/"logic"
  dense. Put the detail into "logic" of the required models; staging models need one short sentence.
""".strip()


def _load_few_shot() -> Dict[str, Any]:
    with open(FEW_SHOT_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    data.pop("_comment", None)
    return data


def build_messages(
    catalog: SourceCatalog,
    goal: str,
    required_models: Sequence[str],
    tag: str,
    source_name: Optional[str] = None,
    extra_constraints: Sequence[str] = (),
    prior_errors: Sequence[str] = (),
    prior_plan: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    few = _load_few_shot()
    request = {
        "domain": catalog.domain,
        "source_name": source_name or f"src_{catalog.domain}",
        "tag": tag,
        "business_goal": goal,
        "required_models": list(required_models),
        "constraints": [
            "every model materializes as an Iceberg table (table_format=iceberg, external_volume=DATAPAI_S3_VOL)",
            "engine for this run is Snowflake; SQL must also be portable to Trino/Athena where possible",
            *extra_constraints,
        ],
        "source_catalog": catalog.compact(),
    }
    msgs: List[Dict[str, str]] = [
        {"role": "system", "content": MEDALLION_PLANNER_SYSTEM_PROMPT},
        {"role": "user", "content": "EXAMPLE of a valid plan for a different domain (shape reference only):\n" + json.dumps(few, separators=(",", ":"))},
        {"role": "user", "content": "REQUEST:\n" + json.dumps(request, separators=(",", ":"), default=str)},
    ]
    if prior_errors:
        msgs.append(
            {
                "role": "user",
                "content": (
                    "Your previous plan failed validation. Fix ONLY these problems and return the full corrected minified JSON:\n- "
                    + "\n- ".join(prior_errors)
                    + ("\n\nPREVIOUS PLAN:\n" + json.dumps(prior_plan, separators=(",", ":")) if prior_plan else "")
                ),
            }
        )
    return msgs


# ═══════════════════════════════════════════════════════════════════════════════
# Parse + validate
# ═══════════════════════════════════════════════════════════════════════════════

def parse_plan(raw_text: str) -> MedallionPlan:
    obj = json.loads(_extract_first_json_object(raw_text))
    return MedallionPlan.model_validate(obj)


def validate_plan(plan: MedallionPlan, catalog: Optional[SourceCatalog], required_models: Sequence[str]) -> List[str]:
    return plan.validate_graph(catalog=catalog, required_models=list(required_models))


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

class PlanningError(RuntimeError):
    def __init__(self, msg: str, errors: List[str], last_raw: str = ""):
        super().__init__(msg)
        self.errors = errors
        self.last_raw = last_raw


def plan(
    catalog: SourceCatalog,
    goal: str,
    required_models: Sequence[str],
    tag: str,
    source_name: Optional[str] = None,
    chat: Optional[ChatFn] = None,
    extra_constraints: Sequence[str] = (),
    max_repairs: int = MAX_PLAN_REPAIRS,
) -> MedallionPlan:
    """
    Ask the LLM for a MedallionPlan, validate, and re-prompt with the error list
    up to `max_repairs` times. Returns a plan with `build_order` filled.
    """
    chat = chat or get_router_chat()
    errors: List[str] = []
    prior: Optional[Dict[str, Any]] = None
    raw = ""
    attempts: List[Dict[str, Any]] = []

    for attempt in range(max_repairs + 1):
        msgs = build_messages(catalog, goal, required_models, tag, source_name,
                              extra_constraints, prior_errors=errors, prior_plan=prior)
        raw = chat(msgs)
        try:
            candidate = parse_plan(raw)
        except (ValueError, ValidationError, json.JSONDecodeError) as exc:
            errors = [f"response was not a valid MedallionPlan JSON: {exc}"]
            prior = None
            attempts.append({"attempt": attempt, "errors": errors})
            logger.warning("plan attempt %d unparseable: %s", attempt, exc)
            continue

        errors = validate_plan(candidate, catalog, required_models)
        attempts.append({"attempt": attempt, "errors": errors, "models": len(candidate.models)})
        if not errors:
            candidate.planner_meta = {
                "attempts": attempts,
                "few_shot": FEW_SHOT_PATH.name,
                "required_models": list(required_models),
                "catalog_tables": [t.name for t in catalog.tables],
                "introspected_at": catalog.introspected_at,
            }
            logger.info("plan accepted after %d attempt(s): %d models", attempt + 1, len(candidate.models))
            return candidate
        logger.warning("plan attempt %d rejected: %s", attempt, errors)
        prior = candidate.model_dump(exclude={"build_order", "planner_meta"})

    raise PlanningError(f"planner failed after {max_repairs + 1} attempts", errors, raw)


def load_plan(path: str) -> MedallionPlan:
    with open(path, "r", encoding="utf-8") as fh:
        return MedallionPlan.model_validate(json.load(fh))


def save_plan(plan_obj: MedallionPlan, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(plan_obj.model_dump(), fh, indent=2, default=str)
