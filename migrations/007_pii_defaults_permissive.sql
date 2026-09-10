-- 007_pii_defaults_permissive.sql
-- Relax the PII defaults seeded by 005: classify everything, restrict nothing until the customer asks.
--
-- Target database: docker datapai_framework_db (EC2 :5433), DATABASE datapai_auth_db, schema datapai
-- Status: APPLIED 2026-09-10.
-- Decision (Donny 2026-09-10): "don't make too many restrictions for us or as default — unless the customer has the requirement."
-- Column classification + recommended masking strategy stay recorded (meta.datapai.pii / masking_rule); AI answer mode is
-- 'full' for every level, no HITL levels, masking DDL stays dry_run. A customer requirement flips the rows below.

UPDATE datapai.sys_common_config SET config_value='full', updated_at=now()
 WHERE config_type='pii' AND config_key IN ('pii_guardrail_answer_mode_PII_HIGH','pii_guardrail_answer_mode_FINANCIAL',
                                            'pii_guardrail_answer_mode_PII_MEDIUM','pii_guardrail_answer_mode_PII_LOW');
UPDATE datapai.sys_common_config SET config_value='[]', updated_at=now()
 WHERE config_type='pii' AND config_key='pii_hitl_required_levels';

-- To restrict later (customer requirement), e.g. mask emails/phones in AI answers and require approval for PII_HIGH masking:
-- UPDATE datapai.sys_common_config SET config_value='masked' WHERE config_type='pii' AND config_key='pii_guardrail_answer_mode_PII_MEDIUM';
-- UPDATE datapai.sys_common_config SET config_value='["PII_HIGH"]' WHERE config_type='pii' AND config_key='pii_hitl_required_levels';
-- then: lineage-project --annotate && dbt docs generate

-- Verification
-- SELECT config_key, config_value FROM datapai.sys_common_config WHERE config_type='pii' ORDER BY 1;
