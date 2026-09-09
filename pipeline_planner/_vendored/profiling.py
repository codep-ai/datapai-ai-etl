"""
Data-quality profiling over a DB-API connection.

Ported from ag2_pipeline/tools.py:profile_table / check_primary_key (2026-09-09).
DuckDB-specific SQL and the AG2 SwarmResult contract are removed; the functions
take a DB-API 2.0 connection and return plain dicts.

Large tables (datapai.prices ≈ 21M rows across 15 LIST partitions) are profiled
on a bounded sample (`sample_rows`) so a catalog build stays in seconds.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


def _q(ident: str) -> str:
    """Double-quote an identifier (Postgres/Snowflake/Athena compatible)."""
    return '"' + ident.replace('"', '""') + '"'


def _qualified(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}" if schema else _q(table)


def profile_table(
    conn,
    schema: str,
    table: str,
    columns: Sequence[str],
    row_count: int,
    sample_rows: int = 200_000,
    sample_values: int = 5,
) -> Dict[str, Any]:
    """
    Per-column null rate, distinct count, low-cardinality flag and sample values,
    computed on a bounded sample. Returns
      {"table", "sampled_rows", "columns": {col: {...}}, "issues": [...]}
    """
    fq = _qualified(schema, table)
    cur = conn.cursor()
    issues: List[str] = []
    col_stats: Dict[str, Dict[str, Any]] = {}

    # One pass: null counts + approx distinct for every column on a sample.
    sample_sql = f"(SELECT * FROM {fq} LIMIT {int(sample_rows)}) s"
    agg_parts = ["COUNT(*)"]
    for c in columns:
        agg_parts.append(f"COUNT(*) - COUNT({_q(c)})")
        agg_parts.append(f"COUNT(DISTINCT {_q(c)})")
    cur.execute(f"SELECT {', '.join(agg_parts)} FROM {sample_sql}")
    row = cur.fetchone()
    sampled = int(row[0] or 0)

    for i, c in enumerate(columns):
        null_count = int(row[1 + i * 2] or 0)
        distinct = int(row[2 + i * 2] or 0)
        null_pct = round(null_count / max(sampled, 1) * 100, 1)
        stats: Dict[str, Any] = {"null_count": null_count, "null_pct": null_pct, "distinct_count": distinct}
        if null_pct > 0:
            issues.append(f"'{c}': {null_pct}% nulls in sample")
        if sampled > 1 and distinct < max(sampled * 0.01, 2):
            stats["low_cardinality"] = True
        col_stats[c] = stats

    # Sample values per column (distinct, non-null, small)
    for c in columns:
        try:
            cur.execute(
                f"SELECT DISTINCT {_q(c)}::text FROM (SELECT {_q(c)} FROM {fq} WHERE {_q(c)} IS NOT NULL LIMIT 2000) s LIMIT {int(sample_values)}"
            )
            col_stats[c]["sample_values"] = [str(r[0])[:64] for r in cur.fetchall()]
        except Exception:
            conn.rollback()
            col_stats[c]["sample_values"] = []

    cur.close()
    return {"table": table, "row_count": row_count, "sampled_rows": sampled, "columns": col_stats, "issues": issues}


def check_primary_key(
    conn,
    schema: str,
    table: str,
    columns: Sequence[str],
    sample_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Verify a (composite) key: distinct(cols) == rows and no nulls.
    On very large tables pass sample_rows to check within a bounded subset.
    """
    fq = _qualified(schema, table)
    src = f"(SELECT * FROM {fq} LIMIT {int(sample_rows)}) s" if sample_rows else fq
    cols = ", ".join(_q(c) for c in columns)
    null_pred = " OR ".join(f"{_q(c)} IS NULL" for c in columns)
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT ({cols})), COUNT(*) FILTER (WHERE {null_pred}) FROM {src}"
    )
    total, distinct, nulls = cur.fetchone()
    cur.close()
    is_pk = total == distinct and nulls == 0
    return {
        "columns": list(columns),
        "rows": int(total),
        "distinct": int(distinct),
        "null_rows": int(nulls),
        "is_valid_key": bool(is_pk),
        "sampled": bool(sample_rows),
    }
