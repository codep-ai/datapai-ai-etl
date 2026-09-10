-- 004_v_sys_ai_pii_columns.sql
-- PII inventory view over the column-lineage system table.
--
-- Target database: docker datapai_framework_db (EC2 :5433), DATABASE datapai_auth_db, schema datapai
--   ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai \
--     "docker exec -i datapai_framework_db psql -U postgres -d datapai_auth_db" < migrations/004_v_sys_ai_pii_columns.sql
-- Status: APPLIED 2026-09-10.

-- Latest-run sensitive columns per pipeline, with the raw roots they inherit from (up to 8 hops).
CREATE OR REPLACE VIEW datapai.v_sys_ai_pii_columns AS
WITH RECURSIVE latest AS (
    SELECT * FROM datapai.v_sys_ai_column_lineage_latest
), sensitive AS (
    SELECT DISTINCT pipeline_id, run_id, to_node AS model, to_column AS column_name, to_sensitivity AS sensitivity
    FROM latest WHERE to_sensitivity <> 'PUBLIC'
), walk AS (
    SELECT s.pipeline_id, s.run_id, s.model, s.column_name, s.sensitivity,
           l.from_node, l.from_column, 1 AS hop
    FROM sensitive s JOIN latest l ON l.pipeline_id = s.pipeline_id AND l.to_node = s.model AND l.to_column = s.column_name
    UNION ALL
    SELECT w.pipeline_id, w.run_id, w.model, w.column_name, w.sensitivity, l.from_node, l.from_column, w.hop + 1
    FROM walk w JOIN latest l ON l.pipeline_id = w.pipeline_id AND l.to_node = w.from_node AND l.to_column = w.from_column
    WHERE w.hop < 8
)
SELECT pipeline_id, run_id, model, column_name, sensitivity,
       array_agg(DISTINCT from_node || '.' || from_column) FILTER (WHERE from_node LIKE 'source:%' OR from_node NOT IN (SELECT DISTINCT to_node FROM latest)) AS root_columns,
       max(hop) AS depth
FROM walk
GROUP BY 1,2,3,4,5;

COMMENT ON VIEW datapai.v_sys_ai_pii_columns IS 'Sensitive (non-PUBLIC) columns per pipeline latest run with the raw root columns they inherit sensitivity from.';

-- Verification
-- SELECT sensitivity, count(*) FROM datapai.v_sys_ai_pii_columns GROUP BY 1;
-- SELECT * FROM datapai.v_sys_ai_pii_columns WHERE model='obt_invoice';

-- Rollback
-- DROP VIEW IF EXISTS datapai.v_sys_ai_pii_columns;
