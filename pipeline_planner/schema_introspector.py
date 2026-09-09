"""
schema_introspector — read raw source schemas into a SourceCatalog.

Week-1 scope: Postgres (the stock raw domain lives in docker `datapai_stock_db`,
schema `datapai`). Iceberg/Snowflake sources plug in later behind the same
`introspect()` signature (the catalog is engine-neutral).

What it captures per table
  • columns + types + nullability          (information_schema)
  • declared PK / unique indexes            (pg_index — includes partitioned parents)
  • declared FKs                            (information_schema.referential_constraints)
  • inferred FKs by column-name matching    (e.g. prices.exchange → exchange_ref.exchange)
  • row counts, partition-aware             (pg_stat on child partitions for LIST-partitioned parents)
  • sampled null%/distinct/samples          (vendored profiling.profile_table)
  • PII sensitivity per column              (vendored pii_classifier)
  • optional key verification on a sample   (vendored profiling.check_primary_key)

Connection env (same convention as datapai-stock-be/scripts/lib/db_helpers.py):
  DATAPAI_PG_HOST (localhost) DATAPAI_PG_PORT (5434) DATAPAI_PG_DB (postgres)
  DATAPAI_PG_USER (postgres)  DATAPAI_PG_PASSWORD
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, List, Optional, Sequence

from .models import (
    ColumnProfile,
    ForeignKeyCandidate,
    KeyCandidate,
    SourceCatalog,
    TableProfile,
)
from ._vendored.pii_classifier import classify_column
from ._vendored.profiling import check_primary_key, profile_table

logger = logging.getLogger(__name__)

# Tables above this size are profiled on a sample and keys are verified on a sample.
LARGE_TABLE_ROWS = int(os.getenv("AI_ETL_LARGE_TABLE_ROWS", "2000000"))
PROFILE_SAMPLE_ROWS = int(os.getenv("AI_ETL_PROFILE_SAMPLE_ROWS", "200000"))


# ═══════════════════════════════════════════════════════════════════════════════
# Connection
# ═══════════════════════════════════════════════════════════════════════════════

def get_pg_conn(prefix: str = "DATAPAI_PG"):
    """psycopg2 connection from <prefix>_HOST/PORT/DB/USER/PASSWORD env vars."""
    import psycopg2  # lazy: only on the runtime host

    return psycopg2.connect(
        host=os.getenv(f"{prefix}_HOST", "localhost"),
        port=int(os.getenv(f"{prefix}_PORT", "5434")),
        dbname=os.getenv(f"{prefix}_DB", "postgres"),
        user=os.getenv(f"{prefix}_USER", "postgres"),
        password=os.getenv(f"{prefix}_PASSWORD", "postgres"),
        connect_timeout=10,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Postgres catalog queries
# ═══════════════════════════════════════════════════════════════════════════════

def _columns(cur, schema: str, table: str) -> List[ColumnProfile]:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable = 'YES'
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [ColumnProfile(name=r[0], data_type=r[1], nullable=bool(r[2])) for r in cur.fetchall()]


def _keys(cur, schema: str, table: str) -> List[KeyCandidate]:
    """PK + unique indexes via pg_index (works for partitioned parents, unlike table_constraints)."""
    cur.execute(
        """
        SELECT i.indisprimary,
               array_agg(a.attname ORDER BY k.ord) AS cols
        FROM pg_index i
        JOIN pg_class c        ON c.oid = i.indrelid
        JOIN pg_namespace n    ON n.oid = c.relnamespace
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a    ON a.attrelid = c.oid AND a.attnum = k.attnum
        WHERE n.nspname = %s AND c.relname = %s AND i.indisunique
          AND i.indpred IS NULL
        GROUP BY i.indexrelid, i.indisprimary
        ORDER BY i.indisprimary DESC, cols
        """,
        (schema, table),
    )
    seen = set()
    out: List[KeyCandidate] = []
    for is_pk, cols in cur.fetchall():
        key = tuple(cols)
        if key in seen:
            continue
        seen.add(key)
        out.append(KeyCandidate(columns=list(cols), kind="primary_key" if is_pk else "unique_index"))
    return out


def _declared_fks(cur, schema: str, table: str) -> List[ForeignKeyCandidate]:
    cur.execute(
        """
        SELECT kcu.column_name, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s AND tc.table_name = %s
        """,
        (schema, table),
    )
    return [
        ForeignKeyCandidate(columns=[r[0]], ref_table=r[1], ref_columns=[r[2]], source="constraint")
        for r in cur.fetchall()
    ]


def _partitions(cur, schema: str, table: str) -> List[str]:
    cur.execute(
        """
        SELECT c.relname
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = (SELECT oid FROM pg_class WHERE relname = %s
                             AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s))
        ORDER BY 1
        """,
        (table, schema),
    )
    return [r[0] for r in cur.fetchall()]


def _row_count(cur, schema: str, table: str, partitions: List[str], exact_limit: int) -> tuple[int, str]:
    """
    Partitioned parents report 0 in pg_stat_user_tables — sum the children.
    Small tables get an exact COUNT(*); large ones use planner stats.
    """
    if partitions:
        cur.execute(
            "SELECT COALESCE(SUM(n_live_tup),0) FROM pg_stat_user_tables WHERE schemaname=%s AND relname = ANY(%s)",
            (schema, partitions),
        )
        return int(cur.fetchone()[0]), "partition_stats"
    cur.execute("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname=%s AND relname=%s", (schema, table))
    row = cur.fetchone()
    est = int(row[0]) if row and row[0] is not None else 0
    if est <= exact_limit:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return int(cur.fetchone()[0]), "exact"
    return est, "stats"


# ═══════════════════════════════════════════════════════════════════════════════
# Inference
# ═══════════════════════════════════════════════════════════════════════════════

def infer_foreign_keys(tables: List[TableProfile]) -> None:
    """
    Add inferred FKs: if table A has column(s) equal to the full key of table B
    (and A != B), propose A.cols → B.key. Column-name equality is the signal;
    the planner gets `source="inferred"` + confidence so it can decide.

    Example: prices(ticker, exchange) → ticker_universe(ticker, exchange);
             prices(exchange) → exchange_ref(exchange).
    """
    for a in tables:
        a_cols = set(a.column_names())
        declared = {(tuple(f.columns), f.ref_table) for f in a.foreign_keys}
        for b in tables:
            if a.name == b.name:
                continue
            for k in b.keys:
                if k.kind not in ("primary_key", "unique_index"):
                    continue
                if set(k.columns) <= a_cols and (tuple(k.columns), b.name) not in declared:
                    # Composite keys are stronger evidence than single generic columns
                    conf = 0.9 if len(k.columns) > 1 else 0.7
                    a.foreign_keys.append(
                        ForeignKeyCandidate(
                            columns=list(k.columns), ref_table=b.name, ref_columns=list(k.columns),
                            source="inferred", confidence=conf,
                        )
                    )
                    declared.add((tuple(k.columns), b.name))


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def introspect(
    tables: Sequence[str],
    schema: str = "datapai",
    domain: str = "stock",
    conn=None,
    target_contracts: Sequence[str] = (),
    profile: bool = True,
    verify_keys: bool = True,
) -> SourceCatalog:
    """
    Build a SourceCatalog for `tables` in `schema`.

    target_contracts: existing tables whose column list a generated mart should
    reproduce (e.g. datapai.screener_metrics). Only columns/types are captured.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT current_database()")
    database = cur.fetchone()[0]

    profiles: List[TableProfile] = []
    for t in tables:
        cols = _columns(cur, schema, t)
        if not cols:
            raise ValueError(f"table {schema}.{t} not found or has no columns")
        parts = _partitions(cur, schema, t)
        rows, method = _row_count(cur, schema, t, parts, exact_limit=LARGE_TABLE_ROWS)
        tp = TableProfile(
            schema_name=schema, name=t, row_count=rows, row_count_method=method,
            is_partitioned=bool(parts), partitions=parts, columns=cols,
            keys=_keys(cur, schema, t), foreign_keys=_declared_fks(cur, schema, t),
        )
        profiles.append(tp)
        logger.info("introspected %s.%s rows=%s(%s) cols=%d keys=%d parts=%d",
                    schema, t, rows, method, len(cols), len(tp.keys), len(parts))

    infer_foreign_keys(profiles)

    if profile:
        for tp in profiles:
            try:
                sample = PROFILE_SAMPLE_ROWS if tp.row_count > LARGE_TABLE_ROWS else max(tp.row_count, 1)
                rep = profile_table(conn, schema, tp.name, tp.column_names(), tp.row_count, sample_rows=sample)
                for c in tp.columns:
                    st = rep["columns"].get(c.name, {})
                    c.null_pct = st.get("null_pct")
                    c.distinct_count = st.get("distinct_count")
                    c.low_cardinality = bool(st.get("low_cardinality", False))
                    c.sample_values = st.get("sample_values", [])
                    c.pii_type, c.sensitivity = classify_column(c.name, c.sample_values, c.data_type)
            except Exception as exc:  # availability over strictness: degrade, don't block
                conn.rollback()
                logger.warning("profile skipped for %s: %s", tp.name, exc)
                for c in tp.columns:
                    c.pii_type, c.sensitivity = classify_column(c.name, [], c.data_type)

    if verify_keys:
        for tp in profiles:
            for k in tp.keys:
                try:
                    res = check_primary_key(
                        conn, schema, tp.name, k.columns,
                        sample_rows=PROFILE_SAMPLE_ROWS if tp.row_count > LARGE_TABLE_ROWS else None,
                    )
                    k.verified = res["is_valid_key"]
                except Exception as exc:
                    conn.rollback()
                    logger.warning("key check skipped for %s%s: %s", tp.name, k.columns, exc)

    contracts: List[TableProfile] = []
    for t in target_contracts:
        cols = _columns(cur, schema, t)
        if cols:
            contracts.append(TableProfile(schema_name=schema, name=t, columns=cols,
                                          description="existing target whose columns a mart should match"))

    cur.close()
    if own_conn:
        conn.close()

    notes = []
    for tp in profiles:
        if tp.is_partitioned:
            notes.append(f"{tp.name} is LIST-partitioned by exchange into {len(tp.partitions)} partitions")
        if not tp.keys:
            notes.append(f"{tp.name} has no declared unique key")
    return SourceCatalog(source_system="postgres", database=database, schema_name=schema,
                         domain=domain, tables=profiles, target_contracts=contracts, notes=notes)
