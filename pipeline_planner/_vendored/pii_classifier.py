"""
Shim: PII classifier moved to datapai-platform-be/lineage/pii_classifier.py (2026-09-10).
Kept so existing imports (schema_introspector, models) keep working.
"""
import platform_init  # noqa: F401
from lineage.pii_classifier import SENSITIVITY_RANK, classify_column, max_sensitivity  # noqa: F401
