"""
model_generator — turn one MedallionPlan node into dbt files.

  LLM  → SQL body + output column list (JSON)
  Python → deterministic config() header (Iceberg, schema, tags), YAML with tests,
           file placement under <dbt project>/<models_subdir>/{staging,intermediate,marts}/

Reuses datapai-platform-be/agents/dbt_agent.py generators where they fit
(`generate_source_yaml_text`) and follows its "LLM plans, Python executes" rule:
the model never decides materialization, storage format or where files go.
"""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml

from .llm import ChatFn, get_router_chat
from .models import MedallionPlan, ModelSpec, SourceCatalog, TestSpec
from .raw_loader import BRONZE_DATABASE, BRONZE_SCHEMA, EXTERNAL_VOLUME, bronze_columns, sf_types_for

logger = logging.getLogger(__name__)

try:
    import platform_init  # noqa: F401
    from agents.dbt_agent import _extract_first_json_object, generate_source_yaml_text  # type: ignore
except Exception:  # pragma: no cover
    from .medallion_planner import _extract_first_json_object  # type: ignore
    generate_source_yaml_text = None  # type: ignore

LAYER_DIR = {"staging": "staging", "intermediate": "intermediate", "dimension": "marts", "fact": "marts"}
LAYER_SCHEMA_SUFFIX = {"staging": "STAGING", "intermediate": "INTERMEDIATE", "dimension": "MARTS", "fact": "MARTS"}


class GenerationTarget:
    """Where files land + how they are configured. Values come from the domain config / env."""

    def __init__(self, project_dir: str, models_subdir: str, tag: str,
                 database: str = BRONZE_DATABASE, schema_prefix: str = BRONZE_SCHEMA,
                 external_volume: str = EXTERNAL_VOLUME):
        self.project_dir = project_dir
        self.models_subdir = models_subdir
        self.tag = tag
        self.database = database
        self.schema_prefix = schema_prefix
        self.external_volume = external_volume

    def dir_for(self, layer: str) -> Path:
        return Path(self.project_dir) / self.models_subdir / LAYER_DIR[layer]

    def schema_for(self, layer: str) -> str:
        return f"{self.schema_prefix}_{LAYER_SCHEMA_SUFFIX[layer]}"

    def config_header(self, spec: ModelSpec) -> str:
        tags = sorted({self.tag, spec.layer, *spec.tags})
        return (
            "{{ config(\n"
            f"    materialized    = '{spec.materialized or 'table'}',\n"
            f"    schema          = '{self.schema_for(spec.layer)}',\n"
            "    table_format    = 'iceberg',\n"
            f"    external_volume = '{self.external_volume}',\n"
            f"    tags            = {json.dumps(tags)}\n"
            ") }}\n"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Prompt
# ═══════════════════════════════════════════════════════════════════════════════

MODEL_GENERATOR_SYSTEM_PROMPT = """
You write production dbt models for Snowflake over Apache Iceberg tables.
You receive ONE model spec from an approved plan plus the exact columns (and types) of every
upstream relation it may read. Return ONLY one JSON object:

{"sql": "<dbt model SQL body>", "columns": [{"name": "...", "data_type": "...", "description": "..."}]}

Rules:
- Do NOT emit a {{ config(...) }} block — the executor adds it.
- Read upstreams ONLY through {{ ref('model') }} / {{ source('src', 'table') }} exactly as listed in the spec.
- Use only columns that exist upstream; never invent columns. Output columns = the spec's "columns" contract
  when given (same names, same order), otherwise a sensible set including the grain/key columns.
- Snowflake SQL, lowercase, CTE style (`with ... as (...)`, final `select` last). No semicolon at the end.
- Iceberg-compatible types only: cast timestamps to TIMESTAMP_NTZ or TIMESTAMP_LTZ (never TIMESTAMP_TZ),
  no VARIANT/OBJECT/ARRAY output columns.
- Surrogate keys: md5(concat_ws('|', coalesce(cast(col1 as varchar),''), ...)). Do NOT use dbt_utils.generate_surrogate_key.
- Text dates in staging must be cast: try_to_date(col) for 'YYYY-MM-DD' strings.
- Window functions: always PARTITION BY the spec's entity keys and ORDER BY the date column; use
  ROWS BETWEEN n PRECEDING AND CURRENT ROW for trailing windows, exactly the window sizes in the spec.
- Recursive CTEs are allowed for date spines (Snowflake supports `with recursive`).
- Never filter rows unless the spec's logic explicitly says so.
- Keep it deterministic; no current_timestamp except where the contract has a computed_at column.
- Follow the spec's "logic" literally. If the spec is ambiguous, pick the simplest reading and note it in the
  description of the affected column.
""".strip()


def _upstream_context(spec: ModelSpec, plan: MedallionPlan, catalog: SourceCatalog,
                      generated: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ups: Dict[str, Any] = {}
    if spec.layer == "staging":
        for s in spec.sources:
            t = catalog.table(s)
            if t:
                actual = bronze_columns(s)  # lake truth wins over Postgres-mapped types
                mapped = sf_types_for(t)
                ups[f"source('{plan.source_name}', '{s}')"] = [
                    {"name": c.name, "type": actual.get(c.name.lower(), mapped[c.name]),
                     **({"sensitivity": c.sensitivity} if c.sensitivity != "PUBLIC" else {})}
                    for c in t.columns
                ]
    for r in spec.refs:
        g = generated.get(r)
        if g and g.get("columns"):
            ups[f"ref('{r}')"] = [{"name": c["name"], "type": c.get("data_type", "")} for c in g["columns"]]
        else:
            up_spec = plan.model(r)
            ups[f"ref('{r}')"] = {"note": "columns not yet known", "grain": up_spec.grain if up_spec else [],
                                  "measures": up_spec.measures if up_spec else []}
    return ups


def build_messages(spec: ModelSpec, plan: MedallionPlan, catalog: SourceCatalog,
                   generated: Dict[str, Dict[str, Any]], repair: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    req = {
        "model": spec.model_dump(exclude={"tags", "materialized"}),
        "upstreams": _upstream_context(spec, plan, catalog, generated),
        "plan_notes": plan.notes,
    }
    msgs = [
        {"role": "system", "content": MODEL_GENERATOR_SYSTEM_PROMPT},
        {"role": "user", "content": "SPEC:\n" + json.dumps(req, separators=(",", ":"), default=str)},
    ]
    if repair:
        msgs.append({
            "role": "user",
            "content": (
                "The previous version of this model FAILED in dbt. Fix the root cause and return the full corrected JSON.\n"
                f"dbt status: {repair.get('status')}\n"
                f"dbt message:\n{repair.get('message')}\n\n"
                f"PREVIOUS SQL:\n{repair.get('sql')}"
            ),
        })
    return msgs


# ═══════════════════════════════════════════════════════════════════════════════
# Executors
# ═══════════════════════════════════════════════════════════════════════════════

_CONFIG_RE = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.S)


def strip_config(sql: str) -> str:
    return _CONFIG_RE.sub("", sql).strip().rstrip(";").strip() + "\n"


def _dbt_tests_for(spec: ModelSpec, column: str) -> List[Any]:
    out: List[Any] = []
    for t in spec.tests:
        if t.column != column:
            continue
        if t.type in ("not_null", "unique"):
            out.append(t.type)
        elif t.type == "accepted_values" and t.values:
            out.append({"accepted_values": {"values": t.values}})
        elif t.type == "relationships" and t.to:
            out.append({"relationships": {"to": f"ref('{t.to}')", "field": t.field or column}})
    return out


def _model_level_tests(spec: ModelSpec) -> List[Any]:
    out: List[Any] = []
    for t in spec.tests:
        if t.type == "unique_combination_of_columns" and t.columns:
            out.append({"dbt_utils.unique_combination_of_columns": {"combination_of_columns": t.columns}})
    # baseline: composite grain without an explicit combo test
    if len(spec.grain) > 1 and not any(t.type == "unique_combination_of_columns" for t in spec.tests):
        out.append({"dbt_utils.unique_combination_of_columns": {"combination_of_columns": spec.grain}})
    return out


def model_yaml(spec: ModelSpec, columns: List[Dict[str, Any]], tag: str) -> str:
    cols = []
    known = {c["name"] for c in columns}
    for c in columns:
        obj: Dict[str, Any] = {"name": c["name"]}
        if c.get("description"):
            obj["description"] = c["description"]
        tests = _dbt_tests_for(spec, c["name"])
        if tests:
            obj["tests"] = tests
        cols.append(obj)
    # tests the plan declared on columns the LLM did not emit → keep them visible as a doc note
    orphan = sorted({t.column for t in spec.tests if t.column and t.column not in known})
    desc = spec.description + (f"\n\nNOTE: planned tests reference columns not emitted: {orphan}" if orphan else "")
    doc = {"version": 2, "models": [{"name": spec.name, "description": desc,
                                     "config": {"tags": sorted({tag, spec.layer})},
                                     **({"tests": _model_level_tests(spec)} if _model_level_tests(spec) else {}),
                                     "columns": cols}]}
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120)


def sources_yaml(plan: MedallionPlan, catalog: SourceCatalog, target: GenerationTarget) -> str:
    used = sorted({s for m in plan.by_layer("staging") for s in m.sources})
    cols = {t.name: t.column_names() for t in catalog.tables if t.name in used}
    if generate_source_yaml_text:
        return generate_source_yaml_text(plan.source_name, target.schema_prefix, used, database=target.database,
                                         include_columns=True, tables_columns=cols)
    return yaml.safe_dump({"version": 2, "sources": [{"name": plan.source_name, "database": target.database,
                                                     "schema": target.schema_prefix, "tables": [{"name": u} for u in used]}]},
                          sort_keys=False)


def write_sources(plan: MedallionPlan, catalog: SourceCatalog, target: GenerationTarget) -> str:
    path = target.dir_for("staging") / f"_{plan.source_name}.yml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sources_yaml(plan, catalog, target), encoding="utf-8")
    return str(path)


def generate_model(spec: ModelSpec, plan: MedallionPlan, catalog: SourceCatalog, target: GenerationTarget,
                   generated: Dict[str, Dict[str, Any]], chat: Optional[ChatFn] = None,
                   repair: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """LLM → files. Returns {"sql_path","yml_path","sql","columns"}."""
    chat = chat or get_router_chat()
    raw = chat(build_messages(spec, plan, catalog, generated, repair))
    obj = json.loads(_extract_first_json_object(raw))
    body = strip_config(str(obj.get("sql") or ""))
    if not body.strip():
        raise ValueError(f"{spec.name}: LLM returned empty SQL")
    columns = [c for c in (obj.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    for c in columns:
        c["name"] = str(c["name"]).lower()
    sql = target.config_header(spec) + "\n" + body
    d = target.dir_for(spec.layer)
    d.mkdir(parents=True, exist_ok=True)
    sql_path = d / f"{spec.name}.sql"
    yml_path = d / f"_{spec.name}.yml"
    sql_path.write_text(sql, encoding="utf-8")
    yml_path.write_text(model_yaml(spec, columns, target.tag), encoding="utf-8")
    logger.info("generated %s → %s (%d columns)", spec.name, sql_path, len(columns))
    return {"sql_path": str(sql_path), "yml_path": str(yml_path), "sql": sql, "columns": columns}


def name_collisions(plan: MedallionPlan, target: GenerationTarget) -> Dict[str, str]:
    """
    dbt requires model names to be unique per project. Scan every models/ subtree
    outside our landing subdir for <name>.sql that would shadow a planned model.
    (Found the hard way 2026-09-09: dbt-demo/models/chinook/intermediate/dim_date.sql.)
    """
    root = Path(target.project_dir) / "models"
    ours = (Path(target.project_dir) / target.models_subdir).resolve()
    planned = {m.name for m in plan.models}
    hits: Dict[str, str] = {}
    for p in root.rglob("*.sql"):
        if ours in p.resolve().parents:
            continue
        if p.stem in planned:
            hits[p.stem] = str(p.relative_to(target.project_dir))
    return hits


def generate_all(plan: MedallionPlan, catalog: SourceCatalog, target: GenerationTarget,
                 chat: Optional[ChatFn] = None, only: Optional[Sequence[str]] = None,
                 generated: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
    generated = dict(generated or {})
    hits = name_collisions(plan, target)
    if hits:
        raise ValueError(
            "planned model names already exist elsewhere in the dbt project (dbt needs unique names): "
            + ", ".join(f"{k} ← {v}" for k, v in hits.items())
            + ". Rename the legacy model or choose different required_models."
        )
    write_sources(plan, catalog, target)
    for name in plan.build_order:
        if only and name not in only:
            continue
        spec = plan.model(name)
        if spec:
            generated[name] = generate_model(spec, plan, catalog, target, generated, chat)
    return generated
