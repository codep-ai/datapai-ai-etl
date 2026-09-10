-- 006_sys_common_config_guardrail_default_access.sql
-- Deployment default for AI access to dbt models in the platform guardrail (DB-driven).
--
-- Target database: docker datapai_framework_db (EC2 :5433), DATABASE datapai_auth_db, schema datapai
--   ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai \
--     "docker exec -i datapai_framework_db psql -U postgres -d datapai_auth_db" < migrations/006_sys_common_config_guardrail_default_access.sql
-- Status: APPLIED 2026-09-10.
--
-- Decision (Donny 2026-09-10): do NOT default to deny. Early AI adoption → models are AI-eligible by default;
-- protection is column-level (PII masked / aggregate_only / deny from lineage metadata), not model-level blocking.
-- Read by datapai-platform-be/guardrail/policy_compiler.py:_default_ai_access() (env DATAPAI_GUARDRAIL_DEFAULT_ACCESS overrides).
-- Values: approved | restricted | internal | deny.

INSERT INTO datapai.sys_common_config (config_type, config_key, config_value, description, created_by)
SELECT 'guardrail', 'guardrail_default_ai_access_level', 'approved',
       'ai_access_level for dbt models with no explicit meta.datapai.ai_access_level. approved (pilot default) | restricted | internal | deny (fail-closed)',
       'datapai-ai-etl'
WHERE NOT EXISTS (SELECT 1 FROM datapai.sys_common_config WHERE config_type='guardrail' AND config_key='guardrail_default_ai_access_level');

-- Operator: switch to fail-closed for a regulated deployment
-- UPDATE datapai.sys_common_config SET config_value='deny', updated_at=now() WHERE config_type='guardrail' AND config_key='guardrail_default_ai_access_level';

-- Verification
-- SELECT config_value FROM datapai.sys_common_config WHERE config_type='guardrail' AND config_key='guardrail_default_ai_access_level';

-- Rollback
-- DELETE FROM datapai.sys_common_config WHERE config_type='guardrail' AND config_key='guardrail_default_ai_access_level' AND created_by='datapai-ai-etl';
