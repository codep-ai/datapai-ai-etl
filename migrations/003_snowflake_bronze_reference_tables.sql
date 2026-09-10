-- 003_snowflake_bronze_reference_tables.sql
-- Bronze (raw) Iceberg copies of the four stock reference tables, loaded from Postgres datapai.*.
--
-- Target: Snowflake DATAPAI.STOCK, Snowflake-managed ICEBERG tables on DATAPAI_S3_VOL
--         (same layout as PRICES, which datapai-stock-be/scripts/sync_snowflake_iceberg.py owns).
-- Applied by: pipeline_planner/raw_loader.py:load_bronze() — CREATE IF NOT EXISTS + full reload
--             (`python -m pipeline_planner.cli load-bronze --catalog out/stock_catalog.json`).
--             Column types are derived from the Postgres catalog (text→VARCHAR, numeric→NUMBER(28,10),
--             bigint→NUMBER(38,0), boolean→BOOLEAN, date→DATE, timestamptz→TIMESTAMP_LTZ(6)).
-- First applied: 2026-09-10 (28,465 / 10 / 5,137 / 636 rows).

CREATE SCHEMA IF NOT EXISTS DATAPAI.STOCK;

CREATE ICEBERG TABLE IF NOT EXISTS DATAPAI.STOCK."TICKER_UNIVERSE" (
  "TICKER" VARCHAR, "EXCHANGE" VARCHAR, "YF_SYMBOL" VARCHAR, "COMPANY_NAME" VARCHAR, "SECTOR" VARCHAR,
  "IS_ACTIVE" BOOLEAN, "SOURCE" VARCHAR, "ADDED_AT" TIMESTAMP_LTZ(6), "UPDATED_AT" TIMESTAMP_LTZ(6),
  "ASSET_TYPE" VARCHAR, "REGION" VARCHAR, "IS_FEATURED" BOOLEAN, "FEATURED_ORDER" NUMBER(38,0)
) EXTERNAL_VOLUME = 'DATAPAI_S3_VOL' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'ticker_universe/STOCK/ticker_universe/';

CREATE ICEBERG TABLE IF NOT EXISTS DATAPAI.STOCK."EXCHANGE_REF" (
  "EXCHANGE" VARCHAR, "CURRENCY_CODE" VARCHAR, "CURRENCY_SYMBOL" VARCHAR, "CURRENCY_NAME_EN" VARCHAR, "COUNTRY" VARCHAR
) EXTERNAL_VOLUME = 'DATAPAI_S3_VOL' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'exchange_ref/STOCK/exchange_ref/';

CREATE ICEBERG TABLE IF NOT EXISTS DATAPAI.STOCK."FX_RATES_DAILY" (
  "BASE_CURRENCY" VARCHAR, "QUOTE_CURRENCY" VARCHAR, "RATE" NUMBER(28,10), "TRADE_DATE" VARCHAR, "SOURCE" VARCHAR
) EXTERNAL_VOLUME = 'DATAPAI_S3_VOL' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'fx_rates_daily/STOCK/fx_rates_daily/';

CREATE ICEBERG TABLE IF NOT EXISTS DATAPAI.STOCK."STOCK_MARKET_CLOSED_DATES" (
  "MARKET_CODE" VARCHAR, "CLOSED_DATE" DATE, "REASON" VARCHAR
) EXTERNAL_VOLUME = 'DATAPAI_S3_VOL' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'stock_market_closed_dates/STOCK/stock_market_closed_dates/';

-- Verification
-- SELECT table_name, row_count, is_iceberg FROM DATAPAI.information_schema.tables
--  WHERE table_schema='STOCK' AND table_name IN ('TICKER_UNIVERSE','EXCHANGE_REF','FX_RATES_DAILY','STOCK_MARKET_CLOSED_DATES');

-- Rollback (data is a copy of Postgres; safe to drop and reload)
-- DROP ICEBERG TABLE IF EXISTS DATAPAI.STOCK."TICKER_UNIVERSE";
-- DROP ICEBERG TABLE IF EXISTS DATAPAI.STOCK."EXCHANGE_REF";
-- DROP ICEBERG TABLE IF EXISTS DATAPAI.STOCK."FX_RATES_DAILY";
-- DROP ICEBERG TABLE IF EXISTS DATAPAI.STOCK."STOCK_MARKET_CLOSED_DATES";

-- Generated schemas (created by dbt from the generated models, not by this file):
--   DATAPAI.STOCK_STAGING, DATAPAI.STOCK_INTERMEDIATE, DATAPAI.STOCK_MARTS — DDL lives in dbt-demo/models/stock/**.sql
