"""
column_lineage — column-level lineage for the generated dbt project, via sqlglot.

dbt-core only knows table-level `ref()` edges. After `dbt compile`/`build`, this reads
`target/manifest.json` (compiled SQL per node) and resolves every output column of every
generated model back to upstream (relation, column) pairs with `sqlglot.lineage`.

Outputs (per pipeline):
  edges:      [{from_node, from_column, to_node, to_column, kind}]   kind = passthrough | derived
  columns:    {model: {column: {"sources": [...], "sensitivity": "PII_HIGH|...|PUBLIC"}}}
  unresolved: [{model, column, error}]                               parse problems (availability > strictness)

Sensitivity propagation: a column inherits the max sensitivity of the source columns it
derives from (introspector's PII classification) — this is what feeds the governance
gate a restricted-column list for the *generated* marts.

Design: docs/architecture/column-level-lineage.md
"""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import MedallionPlan, SourceCatalog
from ._vendored.pii_classifier import max_sensitivity

logger = logging.getLogger(__name__)


def _load_manifest(project_dir: str) -> Dict[str, Any]:
    p = Path(project_dir) / "target" / "manifest.json"
    if not p.exists():
        raise FileNotFoundError(f"{p} not found — run dbt compile/build first")
    return json.loads(p.read_text(encoding="utf-8"))


def _relation_key(node: Dict[str, Any]) -> str:
    """database.schema.identifier in lower case, as it appears in compiled SQL."""
    return ".".join(str(node.get(k) or "").lower() for k in ("database", "schema", "alias")).strip(".")


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


def _node_by_relation(manifest: Dict[str, Any]) -> Dict[str, str]:
    out = {}
    for uid, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            out[_relation_key(node)] = node["name"]
    for uid, src in manifest.get("sources", {}).items():
        key = ".".join(str(src.get(k) or "").lower() for k in ("database", "schema", "identifier")).strip(".")
        out[key] = f"source:{src.get('name')}"
    return out


def _leaf_sources(node, dialect: str) -> List[Dict[str, str]]:
    """Walk a sqlglot lineage Node tree and collect (table, column) leaves."""
    from sqlglot import exp

    leaves: List[Dict[str, str]] = []
    stack = [node]
    seen = set()
    while stack:
        n = stack.pop()
        if id(n) in seen:
            continue
        seen.add(id(n))
        if n.downstream:
            stack.extend(n.downstream)
            continue
        src = n.source
        col_name = n.name.split(".")[-1]
        if isinstance(n.expression, exp.Column):
            col_name = n.expression.name
        elif isinstance(n.expression, exp.Alias) and isinstance(n.expression.this, exp.Column):
            col_name = n.expression.this.name
        table = None
        if isinstance(src, exp.Table):
            table = ".".join(p for p in (src.catalog, src.db, src.name) if p).lower()
        elif n.name and n.name.count(".") >= 1 and not n.downstream:
            # fully-qualified leaf name "db.schema.table.column" when source is not an exp.Table
            parts = n.name.split(".")
            table, col_name = ".".join(parts[:-1]).lower(), parts[-1]
        if table:
            leaves.append({"table": table.replace('"', ""), "column": (col_name or "").lower().strip('"')})
    return leaves


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


def annotate_yaml(lineage: Dict[str, Any], generated: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Write lineage back into the generated dbt YAML as column `meta` (dbt-native: shows in dbt docs,
    readable by Lightdash / Elementary / catalog ingesters):
        meta: {lineage: ["source:prices.ticker"], sensitivity: "PUBLIC"}
    Returns the YAML paths touched.
    """
    import yaml

    touched: List[str] = []
    for model, cols in lineage["columns"].items():
        yml_path = generated.get(model, {}).get("yml_path")
        if not yml_path or not Path(yml_path).exists():
            continue
        doc = yaml.safe_load(Path(yml_path).read_text(encoding="utf-8")) or {}
        changed = False
        for m in doc.get("models", []):
            if m.get("name") != model:
                continue
            for c in m.get("columns", []) or []:
                info = cols.get(str(c.get("name", "")).lower())
                if not info:
                    continue
                c.setdefault("meta", {})["lineage"] = [f"{s['node']}.{s['column']}" for s in info["sources"]]
                c["meta"]["sensitivity"] = info["sensitivity"]
                changed = True
            m.setdefault("meta", {})["column_lineage_dialect"] = lineage["dialect"]
        if changed:
            Path(yml_path).write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120), encoding="utf-8")
            touched.append(yml_path)
    return touched


def _edge_rows(lineage: Dict[str, Any], pipeline_id: str, run_id: str, run_at) -> List[tuple]:
    rows = []
    for e in lineage["edges"]:
        sens = lineage["columns"].get(e["to_node"], {}).get(e["to_column"], {}).get("sensitivity", "PUBLIC")
        rows.append((pipeline_id, run_id, run_at, lineage["dialect"], e["from_node"], e["from_column"],
                     e["to_node"], e["to_column"], e["kind"], sens, bool(e.get("degraded", False))))
    return rows


def write_lineage_pg(lineage: Dict[str, Any], pipeline_id: str, run_id: str, prefix: str = "FRAMEWORK_DB") -> Dict[str, Any]:
    """
    SYSTEM store (decision 2026-09-10): datapai.sys_ai_column_lineage in the framework Postgres
    (migrations/002_sys_ai_column_lineage.sql). Engine-independent, joinable to sys_ai_pipeline /
    guardrail / audit, cheap per-run upserts. Idempotent per (pipeline_id, run_id).
    Connection from <prefix>_HOST/PORT/NAME/USER/PASSWORD (.env.dev: FRAMEWORK_DB_*).
    """
    from datetime import datetime, timezone
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host=os.getenv(f"{prefix}_HOST", "localhost"), port=int(os.getenv(f"{prefix}_PORT", "5433")),
        dbname=os.getenv(f"{prefix}_NAME", "postgres"), user=os.getenv(f"{prefix}_USER", "postgres"),
        password=os.getenv(f"{prefix}_PASSWORD", "postgres"), connect_timeout=10,
    )
    run_at = datetime.now(timezone.utc)
    rows = _edge_rows(lineage, pipeline_id, run_id, run_at)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM datapai.sys_ai_column_lineage WHERE pipeline_id=%s AND run_id=%s", (pipeline_id, run_id))
        if rows:
            execute_values(cur,
                "INSERT INTO datapai.sys_ai_column_lineage (pipeline_id, run_id, run_at, dialect, from_node, from_column, "
                "to_node, to_column, kind, to_sensitivity, degraded) VALUES %s", rows, page_size=500)
        conn.commit()
        cur.execute("SELECT count(*) FROM datapai.sys_ai_column_lineage WHERE pipeline_id=%s AND run_id=%s", (pipeline_id, run_id))
        n = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()
    logger.info("sys_ai_column_lineage: %d edges for %s/%s", n, pipeline_id, run_id)
    return {"table": "datapai.sys_ai_column_lineage", "rows": int(n), "pipeline_id": pipeline_id, "run_id": run_id}


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


def to_mermaid(lineage: Dict[str, Any], model: Optional[str] = None, max_edges: int = 400) -> str:
    """Quick visual for the demo: column-level edges (optionally only into one model)."""
    lines = ["graph LR"]
    n = 0
    for e in lineage["edges"]:
        if model and e["to_node"] != model:
            continue
        a = re.sub(r"[^A-Za-z0-9_]", "_", f"{e['from_node']}__{e['from_column']}")
        b = re.sub(r"[^A-Za-z0-9_]", "_", f"{e['to_node']}__{e['to_column']}")
        lines.append(f'  {a}["{e["from_node"]}.{e["from_column"]}"] --> {b}["{e["to_node"]}.{e["to_column"]}"]')
        n += 1
        if n >= max_edges:
            break
    return "\n".join(lines)
