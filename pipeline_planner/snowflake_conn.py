"""
Snowflake connection built from the SAME dbt profile the pipeline builds with
(default `datapai_snowflake_svc/prod`, key-pair auth). Password auth on this
account requires MFA (verified 2026-09-09), so never use SNOWFLAKE_PASSWORD here.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict

import yaml

_ENV_RE = re.compile(r".*env_var\('([A-Z0-9_]+)'(?:,\s*'([^']*)')?\).*")


def _resolve(v: Any) -> Any:
    m = _ENV_RE.match(str(v))
    if m:
        return os.environ.get(m.group(1), m.group(2) or "")
    return v


def profile_output(profile: str | None = None, target: str | None = None) -> Dict[str, Any]:
    profile = profile or os.getenv("AI_ETL_DBT_PROFILE", "datapai_snowflake_svc")
    target = target or os.getenv("AI_ETL_DBT_TARGET", "prod")
    profiles_dir = os.path.expanduser(os.getenv("DBT_PROFILES_DIR", "~/.dbt"))
    with open(os.path.join(profiles_dir, "profiles.yml"), "r", encoding="utf-8") as fh:
        prof = yaml.safe_load(fh)[profile]
    out = prof["outputs"][target or prof.get("target")]
    return {k: _resolve(v) for k, v in out.items()}


def connect(profile: str | None = None, target: str | None = None):
    import snowflake.connector  # lazy

    out = profile_output(profile, target)
    kw = {k: out[k] for k in ("account", "user", "role", "warehouse", "database", "schema") if out.get(k)}
    if out.get("private_key_path"):
        kw["private_key_file"] = out["private_key_path"]
    elif out.get("password"):
        kw["password"] = out["password"]
    return snowflake.connector.connect(**kw)
