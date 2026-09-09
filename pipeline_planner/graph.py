"""
LangGraph state graph for the one-click medallion pipeline.

Full shape (plan §3.2):
  introspect → plan → [HITL] → generate(node) → build → parse_results
             → (repair → generate)* → register → emit_dag → diff

Week 1 wires: introspect → plan → validate → END. Later nodes are registered as
explicit stubs so the graph topology is already the final one and a checkpointed
run can resume at the failing node once they land.

Checkpointing: SqliteSaver (AI_ETL_CHECKPOINT=sqlite:///path) or the Postgres
saver (AI_ETL_CHECKPOINT=postgresql://...). No env → in-memory.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from . import medallion_planner, schema_introspector
from .models import MedallionPlan, SourceCatalog

logger = logging.getLogger(__name__)


class PipelineState(TypedDict, total=False):
    # request
    domain: str
    schema_name: str
    tables: List[str]
    target_contracts: List[str]
    goal: str
    required_models: List[str]
    tag: str
    source_name: str
    # produced
    catalog: Dict[str, Any]          # SourceCatalog.model_dump()
    plan: Dict[str, Any]             # MedallionPlan.model_dump()
    plan_errors: List[str]
    generated: Dict[str, Dict[str, str]]  # model → {sql, yml}
    run_results: Dict[str, Any]
    attempts: int
    ledger_ids: List[str]
    status: str
    messages: List[str]


def _log(state: PipelineState, msg: str) -> List[str]:
    logger.info(msg)
    return [*state.get("messages", []), msg]


# ── nodes ──────────────────────────────────────────────────────────────────────

def introspect_node(state: PipelineState) -> Dict[str, Any]:
    catalog = schema_introspector.introspect(
        tables=state["tables"],
        schema=state.get("schema_name", "datapai"),
        domain=state.get("domain", "stock"),
        target_contracts=state.get("target_contracts", []),
    )
    return {
        "catalog": catalog.model_dump(),
        "status": "introspected",
        "messages": _log(state, f"introspect: {len(catalog.tables)} tables, {len(catalog.target_contracts)} contracts"),
    }


def plan_node(state: PipelineState) -> Dict[str, Any]:
    catalog = SourceCatalog.model_validate(state["catalog"])
    try:
        plan = medallion_planner.plan(
            catalog=catalog,
            goal=state["goal"],
            required_models=state["required_models"],
            tag=state["tag"],
            source_name=state.get("source_name"),
        )
        return {
            "plan": plan.model_dump(),
            "plan_errors": [],
            "status": "planned",
            "messages": _log(state, f"plan: {len(plan.models)} models, order={plan.build_order}"),
        }
    except medallion_planner.PlanningError as exc:
        return {
            "plan_errors": exc.errors,
            "status": "plan_failed",
            "messages": _log(state, f"plan failed: {exc.errors}"),
        }


def validate_node(state: PipelineState) -> Dict[str, Any]:
    """Re-validate the persisted plan (covers plans loaded from disk / edited by a human)."""
    if not state.get("plan"):
        return {"status": "plan_failed"}
    plan = MedallionPlan.model_validate(state["plan"])
    errors = plan.validate_graph(SourceCatalog.model_validate(state["catalog"]), state.get("required_models"))
    return {
        "plan": plan.model_dump(),
        "plan_errors": errors,
        "status": "plan_valid" if not errors else "plan_invalid",
        "messages": _log(state, f"validate: {'ok' if not errors else errors}"),
    }


def _not_implemented(name: str):
    def _node(state: PipelineState) -> Dict[str, Any]:
        return {"status": f"{name}_pending", "messages": _log(state, f"{name}: not implemented yet (week 2+)")}
    _node.__name__ = f"{name}_node"
    return _node


# ── graph ──────────────────────────────────────────────────────────────────────

def build_graph(checkpointer: Optional[Any] = None, week1_only: bool = True):
    g = StateGraph(PipelineState)
    g.add_node("introspect", introspect_node)
    g.add_node("plan", plan_node)
    g.add_node("validate", validate_node)
    g.add_edge(START, "introspect")
    g.add_edge("introspect", "plan")
    g.add_edge("plan", "validate")

    if week1_only:
        g.add_edge("validate", END)
    else:  # final topology; nodes are stubs until their week lands
        for n in ("generate", "build", "parse_results", "repair", "register", "emit_dag", "diff"):
            g.add_node(n, _not_implemented(n))
        g.add_conditional_edges("validate", lambda s: "generate" if s.get("status") == "plan_valid" else END,
                                {"generate": "generate", END: END})
        g.add_edge("generate", "build")
        g.add_edge("build", "parse_results")
        g.add_conditional_edges(
            "parse_results",
            lambda s: "repair" if s.get("run_results", {}).get("failed") and s.get("attempts", 0) < 3 else "register",
            {"repair": "repair", "register": "register"},
        )
        g.add_edge("repair", "generate")
        g.add_edge("register", "emit_dag")
        g.add_edge("emit_dag", "diff")
        g.add_edge("diff", END)

    return g.compile(checkpointer=checkpointer)


def get_checkpointer(url: Optional[str] = None):
    """
    AI_ETL_CHECKPOINT=sqlite:///path/to/db  → SqliteSaver
    AI_ETL_CHECKPOINT=postgresql://...       → PostgresSaver (framework DB on EC2)
    unset                                    → None (in-memory)
    Returns a context manager or None.
    """
    url = url or os.getenv("AI_ETL_CHECKPOINT")
    if not url:
        return None
    if url.startswith("sqlite"):
        from langgraph.checkpoint.sqlite import SqliteSaver
        return SqliteSaver.from_conn_string(url.replace("sqlite:///", ""))
    if url.startswith("postgres"):
        from langgraph.checkpoint.postgres import PostgresSaver
        return PostgresSaver.from_conn_string(url)
    raise ValueError(f"unsupported AI_ETL_CHECKPOINT: {url}")
