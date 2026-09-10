"""
raw_loader — land small raw reference tables as Snowflake-managed Iceberg tables
(bronze) so the generated staging layer has every source in the lake.

Why this exists: datapai-stock-be/scripts/sync_snowflake_iceberg.py already lands
`prices` + `ohlcv_intraday` as Iceberg (S3 bronze via DATAPAI_S3_VOL). The four
reference tables (ticker_universe, exchange_ref, fx_rates_daily,
stock_market_closed_dates; 10–28k rows) were never synced. They are small enough
to load Postgres → Snowflake directly with executemany; no S3 parquet hop needed
(and pyarrow cannot build on the EC2 glibc 2.26 host).

Result: DATAPAI.STOCK.<TABLE> as ICEBERG TABLE, EXTERNAL_VOLUME=DATAPAI_S3_VOL,
BASE_LOCATION='<table>/STOCK/<table>/' — same layout as PRICES.
Iceberg is the system of record; Snowflake only writes it (decision #4).
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, List, Sequence

from .models import SourceCatalog, TableProfile
from .schema_introspector import get_pg_conn
from . import snowflake_conn

logger = logging.getLogger(__name__)

BRONZE_DATABASE = os.getenv("AI_ETL_SNOWFLAKE_DATABASE", "DATAPAI")
BRONZE_SCHEMA = os.getenv("AI_ETL_BRONZE_SCHEMA", "STOCK")
EXTERNAL_VOLUME = os.getenv("AI_ETL_EXTERNAL_VOLUME", "DATAPAI_S3_VOL")
LARGE_SKIP_ROWS = int(os.getenv("AI_ETL_LOADER_MAX_ROWS", "500000"))

# Postgres → Snowflake Iceberg-compatible types (no TIMESTAMP_TZ / VARIANT in Iceberg)
_PG_TO_SF: Dict[str, str] = {
    "text": "VARCHAR", "character varying": "VARCHAR", "character": "VARCHAR", "uuid": "VARCHAR",
    "numeric": "NUMBER(28,10)", "decimal": "NUMBER(28,10)",
    "double precision": "FLOAT", "real": "FLOAT",
    "integer": "NUMBER(38,0)", "bigint": "NUMBER(38,0)", "smallint": "NUMBER(38,0)",
    "boolean": "BOOLEAN", "date": "DATE",
    "timestamp with time zone": "TIMESTAMP_LTZ(6)", "timestamp without time zone": "TIMESTAMP_NTZ(6)",
    "json": "VARCHAR", "jsonb": "VARCHAR",
}


def sf_type(pg_type: str) -> str:
    return _PG_TO_SF.get(pg_type.lower(), "VARCHAR")


def sf_types_for(table: TableProfile) -> Dict[str, str]:
    """Column → Snowflake type, as the generated staging layer will see it."""
    return {c.name: sf_type(c.data_type) for c in table.columns}


_BRONZE_COLS_CACHE: Dict[str, Dict[str, str]] = {}


def bronze_columns(table_name: str) -> Dict[str, str]:
    """
    Actual Snowflake column types of a bronze table (lower-cased names), or {} when
    the table is absent / Snowflake unreachable. Prices was landed by stock-be's sync
    with TRADE_DATE already DATE, while Postgres has text — the generator must see
    the lake's truth, not the source system's.
    """
    key = table_name.lower()
    if key in _BRONZE_COLS_CACHE:
        return _BRONZE_COLS_CACHE[key]
    cols: Dict[str, str] = {}
    try:
        conn = snowflake_conn.connect()
        cur = conn.cursor()
        cur.execute(
            f"SELECT column_name, data_type FROM {BRONZE_DATABASE}.information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (BRONZE_SCHEMA, table_name.upper()),
        )
        cols = {r[0].lower(): r[1] for r in cur.fetchall()}
        cur.close(); conn.close()
    except Exception as exc:  # degrade to catalog-mapped types
        logger.warning("bronze_columns(%s) unavailable: %s", table_name, exc)
    _BRONZE_COLS_CACHE[key] = cols
    return cols


def create_iceberg_ddl(table: TableProfile) -> str:
    cols = ",\n  ".join(f'"{c.name.upper()}" {sf_type(c.data_type)}' for c in table.columns)
    fq = f'{BRONZE_DATABASE}.{BRONZE_SCHEMA}."{table.name.upper()}"'
    return (
        f"CREATE ICEBERG TABLE IF NOT EXISTS {fq} (\n  {cols}\n)\n"
        f" EXTERNAL_VOLUME = '{EXTERNAL_VOLUME}'\n CATALOG = 'SNOWFLAKE'\n"
        f" BASE_LOCATION = '{table.name}/{BRONZE_SCHEMA}/{table.name}/'"
    )


def load_table(table: TableProfile, pg_conn, sf_cur, batch: int = 5000) -> int:
    fq = f'{BRONZE_DATABASE}.{BRONZE_SCHEMA}."{table.name.upper()}"'
    sf_cur.execute(create_iceberg_ddl(table))
    sf_cur.execute(f"DELETE FROM {fq}")
    cols = [c.name for c in table.columns]
    col_list = ", ".join(f'"{c.upper()}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert = f"INSERT INTO {fq} ({col_list}) VALUES ({placeholders})"

    pg_cur = pg_conn.cursor(name=f"ai_etl_{table.name}")
    pg_cur.itersize = batch
    pg_cur.execute(f'SELECT {", ".join(chr(34)+c+chr(34) for c in cols)} FROM "{table.schema_name}"."{table.name}"')
    total = 0
    while True:
        rows = pg_cur.fetchmany(batch)
        if not rows:
            break
        sf_cur.executemany(insert, [tuple(r) for r in rows])
        total += len(rows)
    pg_cur.close()
    sf_cur.execute(f"SELECT COUNT(*) FROM {fq}")
    landed = sf_cur.fetchone()[0]
    logger.info("bronze %s: %d rows loaded, %d in Snowflake Iceberg", table.name, total, landed)
    return landed


def load_bronze(catalog: SourceCatalog, only: Sequence[str] | None = None, skip_existing_large: bool = True) -> Dict[str, int]:
    """
    Load every catalog table below LARGE_SKIP_ROWS into bronze Iceberg.
    Large tables (prices) are expected to be landed by the existing stock-be sync.
    """
    results: Dict[str, int] = {}
    pg = get_pg_conn()
    sf = snowflake_conn.connect()
    cur = sf.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_DATABASE}.{BRONZE_SCHEMA}")
        for t in catalog.tables:
            if only and t.name not in only:
                continue
            if t.row_count > LARGE_SKIP_ROWS and skip_existing_large:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                    (BRONZE_SCHEMA, t.name.upper()),
                )
                exists = cur.fetchone()[0] > 0
                logger.info("skip large table %s (%d rows); exists in bronze=%s", t.name, t.row_count, exists)
                results[t.name] = -1 if exists else -2
                continue
            results[t.name] = load_table(t, pg, cur)
        sf.commit()
    finally:
        cur.close(); sf.close(); pg.close()
    return results
