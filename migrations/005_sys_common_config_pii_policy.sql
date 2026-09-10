-- 005_sys_common_config_pii_policy.sql
-- PII masking policy as DB-driven config (CLAUDE.md rule: no hardcoded policy in code).
--
-- Target database: docker datapai_framework_db (EC2 :5433), DATABASE datapai_auth_db, schema datapai
--   ssh -i ~/.ssh/Linux-CodeCambat.pem ec2-user@platform.datap.ai \
--     "docker exec -i datapai_framework_db psql -U postgres -d datapai_auth_db" < migrations/005_sys_common_config_pii_policy.sql
-- Status: APPLIED 2026-09-10.
--
-- Deployment model (Donny 2026-09-10): single customer per VPC, no SaaS / multi-tenant → one policy per deployment,
-- keyed by sensitivity level only. Read by pipeline_planner/pii.py:load_policy(); code defaults are the fallback
-- when a row is missing or the DB is unreachable (availability over strictness).
--
-- Keys (config_type = 'pii'):
--   pii_<LEVEL>_strategy      redact | hash | partial | tokenise      (LEVEL ∈ PII_HIGH, FINANCIAL, PII_MEDIUM, PII_LOW)
--   pii_<LEVEL>_roles         JSON list of warehouse roles allowed to see clear values
--   pii_masking_apply_mode    dry_run | apply     (apply = pii-report --apply-masking may execute the DDL)
--   pii_hitl_required_levels  JSON list of levels whose masking DDL needs HITL approval before apply
--   pii_guardrail_answer_mode_<LEVEL>  masked | aggregate_only | deny   (what the AI guardrail may return)

INSERT INTO datapai.sys_common_config (config_type, config_key, config_value, description, created_by)
SELECT v.config_type, v.config_key, v.config_value, v.description, 'datapai-ai-etl'
FROM (VALUES
  ('pii', 'pii_PII_HIGH_strategy',    'redact',                          'Gov IDs, credentials, cards, health: replace with ***REDACTED***'),
  ('pii', 'pii_PII_HIGH_roles',       '["PII_ADMIN"]',                   'Roles that see PII_HIGH in clear'),
  ('pii', 'pii_FINANCIAL_strategy',   'redact',                          'Account numbers, salary, balances'),
  ('pii', 'pii_FINANCIAL_roles',      '["PII_ADMIN","FINANCE_ANALYST"]', 'Roles that see FINANCIAL in clear'),
  ('pii', 'pii_PII_MEDIUM_strategy',  'hash',                            'Email, phone, names, DOB: one-way SHA-256 (referential integrity kept)'),
  ('pii', 'pii_PII_MEDIUM_roles',     '["PII_ADMIN","DATA_ENGINEER"]',   'Roles that see PII_MEDIUM in clear'),
  ('pii', 'pii_PII_LOW_strategy',     'partial',                         'Address, postcode, IP, geo: partial mask'),
  ('pii', 'pii_PII_LOW_roles',        '["PII_ADMIN","DATA_ENGINEER","ANALYST"]', 'Roles that see PII_LOW in clear'),
  ('pii', 'pii_masking_apply_mode',   'dry_run',                         'dry_run | apply — whether generated masking DDL may be executed'),
  ('pii', 'pii_hitl_required_levels', '["PII_HIGH","FINANCIAL"]',        'Levels whose masking changes need human approval before apply'),
  ('pii', 'pii_guardrail_answer_mode_PII_HIGH',   'deny',           'AI guardrail answer mode for PII_HIGH columns'),
  ('pii', 'pii_guardrail_answer_mode_FINANCIAL',  'aggregate_only', 'AI guardrail answer mode for FINANCIAL columns'),
  ('pii', 'pii_guardrail_answer_mode_PII_MEDIUM', 'masked',         'AI guardrail answer mode for PII_MEDIUM columns'),
  ('pii', 'pii_guardrail_answer_mode_PII_LOW',    'masked',         'AI guardrail answer mode for PII_LOW columns')
) AS v(config_type, config_key, config_value, description)
WHERE NOT EXISTS (
  SELECT 1 FROM datapai.sys_common_config c WHERE c.config_type = v.config_type AND c.config_key = v.config_key
);

-- Operator changes (zero code): e.g. make PII_MEDIUM redact instead of hash
-- UPDATE datapai.sys_common_config SET config_value='redact', updated_at=now() WHERE config_type='pii' AND config_key='pii_PII_MEDIUM_strategy';
-- Allow applying masking DDL:
-- UPDATE datapai.sys_common_config SET config_value='apply', updated_at=now() WHERE config_type='pii' AND config_key='pii_masking_apply_mode';

-- Verification
-- SELECT config_key, config_value FROM datapai.sys_common_config WHERE config_type='pii' ORDER BY 1;

-- Rollback
-- DELETE FROM datapai.sys_common_config WHERE config_type='pii' AND created_by='datapai-ai-etl';
