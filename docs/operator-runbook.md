# Operator runbook — datapai-ai-etl

Zero-code-change operations for the AI ETL framework and the surfaces it touches on EC2 `platform.datap.ai`.

## dbt docs server (`dbt.datap.ai`, embedded at `platform.datap.ai/di/dbt`)

**Service:** `dbtdocs.service` → `dbt docs serve --port 8035` in `/home/ec2-user/git/dbt-demo`, nginx proxies `dbt.datap.ai` → 8035.

**2026-09-10 change:** upgraded from the 2024 `~/.local/bin/dbt` (python3.8, dbt 1.5.8) to `/home/ec2-user/venv_ai_etl/bin/dbt` (1.9.8) via a systemd drop-in.
Why: dbt-demo now carries `flags:` in `dbt_project.yml` (required by dbt-snowflake ≥1.9 for `table_format='iceberg'`), which dbt 1.5.8 rejects → the site returned 502. Only dbt ≥1.9 can build Iceberg models, so 1.9 is the baseline for every consumer of dbt-demo (Airflow is on 1.11.7). The 1.5.8 wrapper is left untouched for rollback.

Drop-in: `/etc/systemd/system/dbtdocs.service.d/override.conf`
```
[Service]
ExecStart=
ExecStart=/bin/bash -c "set -a; source /home/ec2-user/.env.dev; set +a; source /home/ec2-user/.bash_profile >/dev/null 2>&1; export PATH=/usr/local/bin:/usr/bin:/bin DBT_PROFILES_DIR=/home/ec2-user/.dbt; exec /home/ec2-user/venv_ai_etl/bin/dbt docs serve --port 8035 --no-browser --profile datapai_snowflake_svc --target prod"
Restart=on-failure
RestartSec=10
```
Gotchas baked into that line: the svc profile needs `SNOWFLAKE_*_SVC` from `~/.env.dev`; `~/.env.dev` also sets `DBT_PROFILES_DIR` to `dbt-demo/profiles.yml` (which still has a deprecated `config:` block that conflicts with `flags:`), so `DBT_PROFILES_DIR=~/.dbt` must be exported *after* sourcing; systemd ignores `export` lines in `EnvironmentFile`.

**Refresh the docs after a build**
```bash
cd /home/ec2-user/git/dbt-demo
set -a; source ~/.env.dev; set +a; export DBT_PROFILES_DIR=~/.dbt
sudo chown -R ec2-user:ec2-user target          # Airflow's bind mount writes compiled files as root
/home/ec2-user/venv_ai_etl/bin/dbt docs generate --profile datapai_snowflake_svc --target prod
```
`docs serve` reads `target/` on each request; no restart needed.

**Verify**
```bash
curl -s -o /dev/null -w "%{http_code}\n" https://dbt.datap.ai/          # 200
curl -s https://dbt.datap.ai/manifest.json | python3 -c "import sys,json; print(json.load(sys.stdin)['metadata']['dbt_version'])"   # 1.9.8
```

**Rollback to dbt 1.5.8**
```bash
sudo rm /etc/systemd/system/dbtdocs.service.d/override.conf
sudo systemctl daemon-reload && sudo systemctl restart dbtdocs.service
```
(Will 502 again while `flags:` exists in `dbt_project.yml`; that block cannot be removed without losing Iceberg builds.)

## Column-level lineage refresh
```bash
cd /home/ec2-user/git/datapai-ai-etl; set -a; source ~/.env.dev; set +a
/home/ec2-user/venv_ai_etl/bin/python -m pipeline_planner.cli lineage --catalog out/stock_catalog.json \
  --plan out/stock_plan.json --state out/stock_generated.json --out out/stock_lineage.json --annotate
```
`--annotate` writes `meta.lineage` / `meta.sensitivity` into the generated YAML; then run the docs refresh above so `dbt.datap.ai` shows it.
Add `--to-db` to persist edges into the SYSTEM table `datapai.sys_ai_column_lineage` in `datapai_auth_db` (framework Postgres :5433, view `v_sys_ai_column_lineage_latest`; migration 002). `--to-warehouse` is an optional read-only Iceberg projection for BI, off by default.

## Viewing lineage — column graph
https://dbt.datap.ai/assets/lineage.html (deep link `#<model>.<column>`). Source of truth `datapai-ai-etl/pipeline_planner/static/lineage.html`;
deploy = copy to `dbt-demo/assets/lineage.html` (dbt copies it to `target/assets/` on `docs generate`; copy to `target/assets/` too for an
immediate refresh). Reads `../manifest.json`; needs the `--annotate` lineage step + `dbt docs generate` to have run.

## Viewing lineage (option 1: dbt docs)
1. Open https://platform.datap.ai/di/dbt (or https://dbt.datap.ai).
2. Search `fct_daily_price` → open the model → Columns → expand `close_usd` → **meta** shows `lineage` and `sensitivity`.
3. Search box also matches column names across models. "Lineage Graph" (bottom right) = table-level DAG; filter with `+fct_daily_price+`.
