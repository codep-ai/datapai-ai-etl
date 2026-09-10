# Demo — column-level lineage on an AI-generated dbt medallion (dbt-core + sqlglot)

**Date:** 2026-09-10 · **Runtime:** EC2 `platform.datap.ai`, venv `/home/ec2-user/venv_ai_etl` · **Project:** `dbt-demo/models/stock` (tag `stock_ai`)

## What the demo shows
dbt-core gives table-level lineage only. This adds **column-level** lineage on top of the standard dbt artefacts, for
every model the framework generated, and writes it back into dbt-native `meta` so dbt docs, Lightdash, Elementary and
catalog tools (DataHub / OpenMetadata) pick it up. No dbt Cloud, no SQLMesh.

## 60-second script
```bash
ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai
set -a; source ~/.env.dev; set +a
cd /home/ec2-user/git/datapai-ai-etl; PY=/home/ec2-user/venv_ai_etl/bin/python

# 1. lineage for the whole generated project (reads dbt-demo/target/manifest.json)
$PY -m pipeline_planner.cli lineage --catalog out/stock_catalog.json --plan out/stock_plan.json \
    --state out/stock_generated.json --out out/stock_lineage.json \
    --mermaid out/lineage.mmd --model fct_daily_price --annotate
#    → models=11 edges=166 unresolved=0 ; annotated 11 YAML files

# 2. show one number's provenance, source to mart
$PY - <<'PY'
import json; L=json.load(open('out/stock_lineage.json'))
def chain(m,c,d=0):
    for s in L['columns'][m][c]['sources']:
        print('  '*d+f"{m}.{c} <- {s['node']}.{s['column']}")
        if not s['node'].startswith('source:'): chain(s['node'],s['column'],d+1)
chain('fct_daily_price','close_usd')
PY
```
Output:
```
fct_daily_price.close_usd <- int_price_with_indicators.close_usd
  int_price_with_indicators.close_usd <- stg_exchange_ref.currency_code
    stg_exchange_ref.currency_code <- source:exchange_ref.currency_code
  int_price_with_indicators.close_usd <- stg_fx_rates.rate
    stg_fx_rates.rate <- source:fx_rates_daily.rate
  int_price_with_indicators.close_usd <- stg_prices.trade_date
    stg_prices.trade_date <- source:prices.trade_date
  int_price_with_indicators.close_usd <- stg_prices.close
    stg_prices.close <- source:prices.close
```
One USD close price = raw close × FX rate for that trade date, looked up by the exchange's currency. Four raw columns, three
source tables, two layers — traced automatically.

## 3. It is inside dbt, not beside it
`dbt-demo/models/stock/marts/_fct_daily_price.yml` after `--annotate`:
```yaml
  - name: close_usd
    description: The closing price in USD.
    meta:
      lineage:
      - int_price_with_indicators.close_usd
      sensitivity: PUBLIC
```
`dbt docs generate && dbt docs serve` shows it per column; `meta.sensitivity` is the PII class propagated from the
introspector's scan of the raw columns (max over upstream columns) — the restricted-column list the governance gate uses.

## 4. Interactive column graph — https://dbt.datap.ai/assets/lineage.html
Static overlay inside the dbt docs site (`dbt-demo/assets/lineage.html`, source `pipeline_planner/static/lineage.html`). Reads the same
`manifest.json`, draws columns as nodes grouped by model and layered by depth, no libraries. Search a column or model, click a column to
highlight upstream (orange) and downstream (green) paths, dashed = derived, colour bar = PII sensitivity, detail panel lists the raw source
columns reached. Deep link: `#fct_daily_price.close_usd`. Survives `dbt docs generate` because dbt copies `asset-paths` into `target/`.

Mermaid export also available: `docs/samples/2026-09-10-stock_column_lineage_fct_daily_price.mmd`.

## 5. Queryable in the warehouse — `DATAPAI.STOCK_MARTS.DBT_COLUMN_LINEAGE` (Iceberg)
`lineage --to-warehouse` persists every edge per run (`pipeline_id`, `run_id`, `run_at`, from/to node+column, kind, sensitivity).
Recursive trace in plain SQL (also a ready-made Lightdash explore):
```sql
with recursive up as (
  select from_node, from_column, to_node, to_column, 1 as hop
  from DATAPAI.STOCK_MARTS.DBT_COLUMN_LINEAGE where to_node='fct_daily_price' and to_column='close_usd'
  union all
  select l.from_node, l.from_column, l.to_node, l.to_column, up.hop+1
  from DATAPAI.STOCK_MARTS.DBT_COLUMN_LINEAGE l join up on l.to_node=up.from_node and l.to_column=up.from_column
  where up.hop < 6)
select hop, from_node||'.'||from_column as upstream from up order by 1, 2;
-- 1 int_price_with_indicators.close_usd
-- 2 stg_exchange_ref.currency_code / stg_fx_rates.rate / stg_prices.close / stg_prices.trade_date
-- 3 source:exchange_ref.currency_code / source:fx_rates_daily.rate / source:prices.close / source:prices.trade_date
```
Regulator question "where did this number come from" = one query, with run history.

## Coverage on this run
| Model | columns | traced to upstream |
|---|---:|---:|
| stg_* (5) | 27 | 27 |
| int_price_with_indicators | 10 | 10 |
| dim_date / dim_exchange / dim_ticker | 15 | 15 |
| fct_daily_price | 9 | 8 (surrogate key is a literal expression) |
| fct_screener_metrics | 57 | 56, **degraded**: resolves to `int_price_with_indicators.*` only |

The screener degradation is deliberate signal, not a lineage bug: this run's generated SQL references `volume`, a column
the upstream model never declared. Schema-strict lineage fails on it *before* `dbt build` does — the lineage pass doubles as
a pre-build check and will feed the repair node.

## How it works (`pipeline_planner/column_lineage.py`)
1. `target/manifest.json` → compiled SQL per model (Jinja already resolved by dbt).
2. Schema for sqlglot = generator's declared columns ∪ introspector catalog ∪ `target/catalog.json` when present, as a
   normalising `MappingSchema` (Snowflake upper-cases unquoted identifiers — without normalisation nothing matched).
3. `sqlglot.lineage(column, sql, schema, dialect)` per output column → leaf `(relation, column)` pairs, mapped back to dbt
   node names; CTE and literal leaves dropped. Strict pass first, schema-less fallback second (flagged `degraded`).
4. Sensitivity = max over upstream columns. Edges + per-column sources + restricted columns → JSON, Mermaid, YAML `meta`.
5. Dialect is a parameter (`snowflake` today; `trino` for Athena, `databricks`, `clickhouse`, `bigquery`) — same code for every engine over the Iceberg floor.

## Next (to make it a product feature)
- OpenLineage column-facet emit → Marquez / DataHub / OpenMetadata.
- `dbt_column_lineage` table in the warehouse via `run-operation` for BI and audit queries.
- Wire into the graph between `generate` and `build_repair`; feed strict failures to `repair`.
- Regulatory marts: contract = regulator schema, lineage = "where did this reported number come from" evidence.
