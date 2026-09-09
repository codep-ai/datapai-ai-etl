"""
pipeline_planner — AI-generated medallion (raw → staging → intermediate → dim/fact)
dbt pipelines on an Iceberg floor.

Week-1 surface (2026-09-09):
  schema_introspector.introspect(...)   -> SourceCatalog
  medallion_planner.plan(...)           -> MedallionPlan  (LLM planner + validator)
  graph.build_graph()                   -> LangGraph StateGraph (introspect → plan → validate)
  cli                                   -> `python -m pipeline_planner.cli ...`

Platform primitives (RouterChatClient, dbt_agent, governance, ledger) come from
datapai-platform-be via the repo-root platform_init.py (DATAPAI_PLATFORM_DIR).
"""

__version__ = "0.1.0"
