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


def _q(c: str) -> str:
    return '"' + c + '"'


# Candidate normalisations of a child key, tried against the parent key when the raw join is weak.
# `{col}` is substituted; expressions are ANSI/Postgres and portable to Snowflake.
CONFORM_CANDIDATES = [
    ("strip_suffix_after_dot", "split_part({col}, '.', 1)"),
    ("strip_suffix_after_colon", "split_part({col}, ':', 1)"),
    ("upper_trim", "upper(trim({col}))"),
    ("strip_leading_zeros", "ltrim({col}, '0')"),
    ("digits_only", "regexp_replace({col}, '[^0-9]', '', 'g')"),
    ("strip_exchange_suffix", "regexp_replace({col}, '\\.[A-Za-z]{{1,3}}$', '')"),
]


def verify_foreign_keys(conn, schema: str, tables: List[TableProfile], sample_rows: int = 50_000) -> None:
    """
    Measure how many sampled child rows actually find a parent (match_rate), broken down
    by the child's low-cardinality partition-like column when there is one (e.g. exchange).
    For weak matches, try the child key against every other text column of the parent and
    report the best alternative(s) — this is how `prices.ticker='BHP.AX'` vs
    `ticker_universe.ticker='BHP'` / `yf_symbol='BHP.AX'` becomes visible to the planner.
    """
    by_name = {t.name: t for t in tables}
    cur = conn.cursor()
    for child in tables:
        # Group column = the FK column with the FEWEST distinct values (≤ 100), e.g. exchange — never ticker.
        fk_cols = {k for fk in child.foreign_keys for k in fk.columns}
        cands = [c for c in child.columns if c.name in fk_cols and c.distinct_count is not None and c.distinct_count <= 100]
        group_col = min(cands, key=lambda c: c.distinct_count).name if cands else None
        # A plain LIMIT on a LIST-partitioned table reads one partition only (all-ASX sample, 2026-09-10) →
        # use TABLESAMPLE so every partition contributes.
        if child.row_count > sample_rows * 4:
            pct = max(min(sample_rows / child.row_count * 100 * 4, 100.0), 0.05)  # over-sample; LIMIT trims
            child_src = f'(SELECT * FROM {_q(schema)}.{_q(child.name)} TABLESAMPLE SYSTEM ({pct:.4f}) LIMIT {int(sample_rows)}) c'
        else:
            child_src = f'(SELECT * FROM {_q(schema)}.{_q(child.name)} LIMIT {int(sample_rows)}) c'
        for fk in child.foreign_keys:
            parent = by_name.get(fk.ref_table)
            if not parent:
                continue
            on = " AND ".join(f"c.{_q(a)} = p.{_q(b)}" for a, b in zip(fk.columns, fk.ref_columns))
            parent_src = f'{_q(schema)}.{_q(parent.name)} p'
            try:
                cur.execute(f"SELECT COUNT(*), COUNT(p.{_q(fk.ref_columns[0])}) FROM {child_src} LEFT JOIN {parent_src} ON {on}")
                total, hit = cur.fetchone()
                fk.match_rate = round(hit / total, 4) if total else None
                if group_col:
                    cur.execute(f"SELECT c.{_q(group_col)}, COUNT(*), COUNT(p.{_q(fk.ref_columns[0])}) FROM {child_src} "
                                f"LEFT JOIN {parent_src} ON {on} GROUP BY 1 ORDER BY 2 DESC LIMIT 30")
                    fk.match_rate_by = {str(g): round(h / n, 4) for g, n, h in cur.fetchall() if n}
                if fk.match_rate is not None and fk.match_rate < 0.95 and len(fk.columns) >= 1:
                    # try the first (identifying) child column against other text columns of the parent
                    weak_child = fk.columns[0]
                    others = [c.name for c in parent.columns if c.data_type.startswith(("text", "char")) and c.name not in fk.ref_columns]
                    rest = [(a, b) for a, b in zip(fk.columns, fk.ref_columns) if a != weak_child]
                    for alt in others:
                        alt_on = " AND ".join([f"c.{_q(weak_child)} = p.{_q(alt)}"] + [f"c.{_q(a)} = p.{_q(b)}" for a, b in rest])
                        cur.execute(f"SELECT COUNT(*), COUNT(p.{_q(alt)}) FROM {child_src} LEFT JOIN {parent_src} ON {alt_on}")
                        n, h = cur.fetchone()
                        rate = round(h / n, 4) if n else 0.0
                        if rate > (fk.match_rate or 0):
                            fk.alternatives.append({"ref_columns": [alt] + [b for _, b in rest], "match_rate": rate})
                    fk.alternatives.sort(key=lambda a: -a["match_rate"])
                    fk.alternatives = fk.alternatives[:2]
                    # Conforming-rule discovery: which normalisation of the child key makes it match the parent key?
                    # (BHP.AX vs BHP, 2026-09-10 — previously a human wrote this rule by hand.)
                    parent_key = fk.ref_columns[0]
                    for label, expr in CONFORM_CANDIDATES:
                        e = expr.format(col=f"c.{_q(weak_child)}")
                        c_on = " AND ".join([f"{e} = p.{_q(parent_key)}"] + [f"c.{_q(a)} = p.{_q(b)}" for a, b in rest])
                        try:
                            if group_col:
                                cur.execute(f"SELECT c.{_q(group_col)}, COUNT(*), COUNT(p.{_q(parent_key)}) FROM {child_src} "
                                            f"LEFT JOIN {parent_src} ON {c_on} GROUP BY 1")
                                rows = cur.fetchall()
                                n = sum(r[1] for r in rows); h = sum(r[2] for r in rows)
                                by = {str(g): round(hh / nn, 4) for g, nn, hh in rows if nn}
                            else:
                                cur.execute(f"SELECT COUNT(*), COUNT(p.{_q(parent_key)}) FROM {child_src} LEFT JOIN {parent_src} ON {c_on}")
                                n, h = cur.fetchone(); by = {}
                            rate = round(h / n, 4) if n else 0.0
                            if rate > (fk.match_rate or 0):
                                fk.conform_rules.append({"expr": expr.format(col=weak_child), "label": label,
                                                         "match_rate": rate, "match_rate_by": by})
                        except Exception as exc:
                            conn.rollback()
                            logger.debug("conform candidate %s failed: %s", label, exc)
                    fk.conform_rules.sort(key=lambda r: -r["match_rate"])
                    fk.conform_rules = fk.conform_rules[:2]
            except Exception as exc:
                conn.rollback()
                logger.warning("fk verify skipped %s%s→%s: %s", child.name, fk.columns, fk.ref_table, exc)
    cur.close()


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
        verify_foreign_keys(conn, schema, profiles)
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
        for fk in tp.foreign_keys:
            if fk.match_rate is not None and fk.match_rate < 0.95:
                hint = f"; better join: {fk.alternatives[0]['ref_columns']} matches {fk.alternatives[0]['match_rate']:.0%}" if fk.alternatives else ""
                if fk.conform_rules:
                    hint += f"; CONFORM RULE: {fk.conform_rules[0]['expr']} matches {fk.conform_rules[0]['match_rate']:.0%}"
                notes.append(f"{tp.name}({','.join(fk.columns)}) -> {fk.ref_table}({','.join(fk.ref_columns)}) matches only "
                             f"{fk.match_rate:.0%} of rows" + (f" (by group: {fk.match_rate_by})" if fk.match_rate_by else "") + hint)
    return SourceCatalog(source_system="postgres", database=database, schema_name=schema,
                         domain=domain, tables=profiles, target_contracts=contracts, notes=notes)
