"""
column_lineage (datapai-ai-etl) — generator-specific entry point over the platform lineage package.

The engine lives in datapai-platform-be/lineage/ (moved 2026-09-10; imported via DATAPAI_PLATFORM_DIR / platform_init).
This module keeps only what needs the generator's state (MedallionPlan, SourceCatalog, declared columns):
  compute_lineage()      — lineage for a plan the generator just built (schema from generator state + introspector catalog)
  write_lineage_table()  — optional Iceberg projection into the warehouse (needs pipeline_planner.snowflake_conn)
and re-exports the platform functions so existing imports keep working.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import platform_init  # noqa: F401
from lineage.column_lineage import (  # noqa: F401  re-exports
    DOCS_INJECT_MARKER, _leaf_sources, _load_manifest, _node_by_relation, _relation_key, annotate_yaml,
    lineage_from_artifacts, patch_dbt_docs, set_ai_access_level, to_mermaid, write_lineage_pg, yaml_paths_from_manifest,
)
from lineage.pii_classifier import max_sensitivity

from .models import MedallionPlan, SourceCatalog

logger = logging.getLogger(__name__)


def _schema_for_sqlglot(manifest: Dict[str, Any], generated: Dict[str, Dict[str, Any]], catalog: SourceCatalog,
                        plan: MedallionPlan, dialect: str):
    """
    sqlglot MappingSchema {catalog: {db: {table: {column: type}}}} for every relation compiled SQL may
    reference: generated models (generator's declared columns; catalog.json when present) and sources
    (introspector catalog). `normalize=True` makes identifier case follow the dialect (Snowflake upper-cases
    unquoted names) — without it nothing matched and `select *` stayed unexpanded (2026-09-10).
    """
    from sqlglot.schema import MappingSchema

    nested: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
    catalog_json = Path(manifest.get("metadata", {}).get("project_root", "") or "") / "target" / "catalog.json"
    cat_cols: Dict[str, Dict[str, str]] = {}
    if catalog_json.exists():
        try:
            cj = json.loads(catalog_json.read_text(encoding="utf-8"))
            for uid, n in {**cj.get("nodes", {}), **cj.get("sources", {})}.items():
                cat_cols[uid] = {c["name"].lower(): c["type"] for c in n.get("columns", {}).values()}
        except Exception as exc:  # catalog.json is optional
            logger.debug("catalog.json unreadable: %s", exc)

    def put(db: str, sch: str, tbl: str, cols: Dict[str, str]) -> None:
        if cols:
            nested.setdefault(db, {}).setdefault(sch, {})[tbl] = cols

    for uid, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        cols = cat_cols.get(uid) or ({c["name"].lower(): (c.get("data_type") or "unknown") for c in generated.get(name, {}).get("columns", [])})
        put(str(node.get("database") or ""), str(node.get("schema") or ""), str(node.get("alias") or name), cols)
    for uid, src in manifest.get("sources", {}).items():
        t = catalog.table(src.get("name", ""))
        cols = cat_cols.get(uid) or ({c.name.lower(): c.data_type for c in t.columns} if t else {})
        put(str(src.get("database") or ""), str(src.get("schema") or ""), str(src.get("identifier") or src.get("name")), cols)
    return MappingSchema(nested, dialect=dialect, normalize=True)



def compute_lineage(project_dir: str, plan: MedallionPlan, catalog: SourceCatalog,
                    generated: Dict[str, Dict[str, Any]], dialect: str = "snowflake") -> Dict[str, Any]:
    import sqlglot
    from sqlglot.lineage import lineage as sg_lineage

    manifest = _load_manifest(project_dir)
    manifest.setdefault("metadata", {})["project_root"] = project_dir
    schema = _schema_for_sqlglot(manifest, generated, catalog, plan, dialect)
    rel2name = _node_by_relation(manifest)
    src_sensitivity: Dict[str, Dict[str, str]] = {
        f"source:{t.name}": {c.name.lower(): c.sensitivity for c in t.columns} for t in catalog.tables
    }
    edges: List[Dict[str, str]] = []
    columns: Dict[str, Dict[str, Dict[str, Any]]] = {}
    unresolved: List[Dict[str, str]] = []

    nodes_by_name = {n["name"]: n for n in manifest.get("nodes", {}).values() if n.get("resource_type") == "model"}
    for model_name in plan.build_order:
        node = nodes_by_name.get(model_name)
        gen = generated.get(model_name, {})
        if not node or not node.get("compiled_code"):
            unresolved.append({"model": model_name, "column": "*", "error": "no compiled_code in manifest"})
            continue
        sql = node["compiled_code"]
        out_cols = [c["name"].lower() for c in gen.get("columns", [])]
        if not out_cols:
            try:
                parsed = sqlglot.parse_one(sql, read=dialect)
                out_cols = [e.alias_or_name.lower() for e in parsed.selects] if hasattr(parsed, "selects") else []
            except Exception as exc:
                unresolved.append({"model": model_name, "column": "*", "error": f"parse: {exc}"[:300]})
                continue
        columns[model_name] = {}
        for col in out_cols:
            try:
                degraded = False
                try:
                    tree = sg_lineage(col, sql, schema=schema, dialect=dialect)
                except Exception as first:
                    # Schema-strict pass failed (typically the model references a column its upstream never
                    # declared — a real defect the build will hit too). Retry without schema so explicit
                    # column references still resolve; record the strict error as a warning.
                    tree = sg_lineage(col, sql, dialect=dialect)
                    degraded = True
                    unresolved.append({"model": model_name, "column": col, "error": f"schema-strict: {first}"[:300], "degraded": True})
                leaves = [lf for lf in _leaf_sources(tree, dialect) if lf["table"] in rel2name]  # drop CTE/literal leaves
                srcs = []
                for leaf in leaves:
                    up = rel2name[leaf["table"]]
                    srcs.append({"node": up, "column": leaf["column"]})
                    edges.append({"from_node": up, "from_column": leaf["column"], "to_node": model_name, "to_column": col,
                                  "kind": "passthrough" if len(leaves) == 1 and leaf["column"] == col else "derived",
                                  **({"degraded": True} if degraded else {})})
                # sensitivity = max over upstream columns (sources first, then already-resolved models)
                sens = []
                for s in srcs:
                    if s["node"] in src_sensitivity:
                        sens.append(src_sensitivity[s["node"]].get(s["column"], "PUBLIC"))
                    elif s["node"] in columns:
                        sens.append(columns[s["node"]].get(s["column"], {}).get("sensitivity", "PUBLIC"))
                columns[model_name][col] = {"sources": srcs, "sensitivity": max_sensitivity(sens) if sens else "PUBLIC"}
            except Exception as exc:
                unresolved.append({"model": model_name, "column": col, "error": str(exc)[:300]})
        logger.info("lineage %s: %d columns, %d unresolved so far", model_name, len(columns[model_name]), len(unresolved))

    restricted = sorted({f"{m}.{c}" for m, cols in columns.items() for c, v in cols.items() if v["sensitivity"] != "PUBLIC"})
    hard = [u for u in unresolved if not u.get("degraded")]
    return {"dialect": dialect, "models": len(columns), "edges": edges, "columns": columns,
            "unresolved": hard, "warnings": [u for u in unresolved if u.get("degraded")],
            "restricted_columns": restricted,
            "summary": {m: {"columns": len(c), "traced": sum(1 for v in c.values() if v["sources"])} for m, c in columns.items()}}



def write_lineage_table(lineage: Dict[str, Any], pipeline_id: str, run_id: str,
                        database: Optional[str] = None, schema: Optional[str] = None,
                        table: str = "DBT_COLUMN_LINEAGE") -> Dict[str, Any]:
    """
    OPTIONAL read-only projection into the customer's warehouse (Iceberg) for BI joins. The system of
    record is write_lineage_pg(). Off by default; enable with `cli lineage --to-warehouse`.
    """
    from datetime import datetime, timezone
    from . import snowflake_conn
    from .raw_loader import BRONZE_DATABASE, EXTERNAL_VOLUME

    database = database or BRONZE_DATABASE
    schema = schema or os.getenv("AI_ETL_LINEAGE_SCHEMA", "STOCK_MARTS")
    fq = f'{database}.{schema}."{table}"'
    ddl = (
        f"CREATE ICEBERG TABLE IF NOT EXISTS {fq} (\n"
        '  "PIPELINE_ID" VARCHAR, "RUN_ID" VARCHAR, "RUN_AT" TIMESTAMP_NTZ(6), "DIALECT" VARCHAR,\n'
        '  "FROM_NODE" VARCHAR, "FROM_COLUMN" VARCHAR, "TO_NODE" VARCHAR, "TO_COLUMN" VARCHAR,\n'
        '  "KIND" VARCHAR, "TO_SENSITIVITY" VARCHAR, "DEGRADED" BOOLEAN\n'
        f")\n EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}'\n CATALOG = 'SNOWFLAKE'\n"
        f" BASE_LOCATION = 'ai_etl/{schema}/{table.lower()}/'"
    )
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for e in lineage["edges"]:
        sens = lineage["columns"].get(e["to_node"], {}).get(e["to_column"], {}).get("sensitivity", "PUBLIC")
        rows.append((pipeline_id, run_id, run_at, lineage["dialect"], e["from_node"], e["from_column"],
                     e["to_node"], e["to_column"], e["kind"], sens, bool(e.get("degraded", False))))
    conn = snowflake_conn.connect()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        cur.execute(ddl)
        cur.execute(f'DELETE FROM {fq} WHERE "PIPELINE_ID" = %s AND "RUN_ID" = %s', (pipeline_id, run_id))
        if rows:
            cur.executemany(
                f'INSERT INTO {fq} ("PIPELINE_ID","RUN_ID","RUN_AT","DIALECT","FROM_NODE","FROM_COLUMN","TO_NODE","TO_COLUMN","KIND","TO_SENSITIVITY","DEGRADED") '
                f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
        conn.commit()
        cur.execute(f'SELECT COUNT(*) FROM {fq} WHERE "PIPELINE_ID" = %s AND "RUN_ID" = %s', (pipeline_id, run_id))
        n = cur.fetchone()[0]
    finally:
        cur.close(); conn.close()
    logger.info("lineage table %s: %d edges for %s/%s", fq, n, pipeline_id, run_id)
    return {"table": fq, "rows": int(n), "pipeline_id": pipeline_id, "run_id": run_id}


