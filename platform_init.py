"""
Platform framework linkage for datapai-ai-etl.
───────────────────────────────────────────────
Import this module BEFORE any `agents.*`, `guardrail.*` or `traceability.*`
import so the shared datapai-platform-be primitives (RouterChatClient,
dbt_agent, GovernedAction, TraceLedger, ...) resolve via sys.path.

Copied from datapai-stock-be/platform_init.py (same pattern, same env var).

Resolution order:
  1. This repo's own packages (pipeline_planner/)
  2. Platform packages via sys.path (agents/, guardrail/, traceability/)

Environment:
  DATAPAI_PLATFORM_DIR  — path to datapai-platform-be
    Local Mac default:   ../datapai-platform-be   (sibling git clone; source only)
    EC2 (verified 2026-09-09): /home/ec2-user/git/datapai-platform-be
        NOTE: the stock-be default of /home/ec2-user/git/vanna-streamlit does NOT
        contain agents/llm_client.py or agents/dbt_agent.py — do not reuse it here.
    Airflow container:   /opt/datapai
"""
import os
import sys

_PLATFORM_DIR = os.environ.get(
    "DATAPAI_PLATFORM_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "datapai-platform-be"),
)
_PLATFORM_DIR = os.path.abspath(_PLATFORM_DIR)

if _PLATFORM_DIR not in sys.path:
    # Append (not insert) so this repo's packages win over platform packages.
    sys.path.append(_PLATFORM_DIR)


def platform_dir() -> str:
    """Return the resolved datapai-platform-be directory."""
    return _PLATFORM_DIR


def platform_available() -> bool:
    """True when the platform checkout actually exposes agents/llm_client.py."""
    return os.path.isfile(os.path.join(_PLATFORM_DIR, "agents", "llm_client.py"))
