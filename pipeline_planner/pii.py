"""
pii (datapai-ai-etl) — shim over datapai-platform-be/lineage/pii.py (moved 2026-09-10).
Adds the warehouse connector for apply_masking; everything else is the platform module.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import platform_init  # noqa: F401
from lineage.pii import (  # noqa: F401
    DEFAULT_POLICY, LEVELS, dbt_patch, guardrail_column_meta, guardrail_model_meta, load_policy, masking_ddl,
    report, restricted_list, to_markdown,
)
from lineage import pii as _platform_pii


def apply_masking(statements: List[str], policy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from . import snowflake_conn
    return _platform_pii.apply_masking(statements, connect=snowflake_conn.connect, policy=policy)
