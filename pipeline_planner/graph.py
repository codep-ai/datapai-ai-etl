"""
LangGraph state graph for the one-click medallion pipeline.

Shape (plan §3.2):
  introspect → plan → validate → [hitl] → load_bronze → generate → build_repair → register → emit_dag → diff

Week 1: introspect, plan, validate.        Week 2: load_bronze, generate, build_repair.
Week 3+: register, emit_dag, diff (stubs return *_pending today so the topology is final).

Checkpointing: AI_ETL_CHECKPOINT=sqlite:///path | postgresql://... ; unset → in-memory.
HITL: when state["hitl_required"] is true the graph interrupts before load_bronze
(LangGraph `interrupt_before`); resume by invoking again with the same thread_id.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from . import build_loop, medallion_planner, model_generator, raw_loader, schema_introspector
from .models import MedallionPlan, SourceCatalog

logger = logging.getLogger(__name__)


class PipelineState(TypedDict, total=False):
    # request (mirrors sys_ai_pipeline columns)
    pipeline_id: str
    domain: str
    schema_name: str
    tables: List[str]
    target_contracts: List[str]
    goal: str
    required_models: List[str]
    tag: str
    source_name: str
    dbt_project_dir: str
    dbt_models_subdir: str
    hitl_required: bool
    # produced
    catalog: Dict[str, Any]
    plan: Dict[str, Any]
    plan_errors: List[str]
    bronze: Dict[str, int]
    generated: Dict[str, Dict[str, Any]]
    build: Dict[str, Any]
    attempts: int
    ledger_ids: List[str]
    status: str
    messages: List[str]


def _log(state: PipelineState, msg: str) -> List[str]:
    logger.info(msg)
    return [*state.get("messages", []), msg]


def _target(state: PipelineState) -> model_generator.GenerationTarget:
    return model_generator.GenerationTarget(
        project_dir=state.get("dbt_project_dir") or os.getenv("AI_ETL_DBT_PROJECT_DIR", "/home/ec2-user/git/dbt-demo"),
        models_subdir=state.get("dbt_models_subdir") or os.getenv("AI_ETL_DBT_MODELS_SUBDIR", "models/stock"),
        tag=state["tag"],
    )


# ── nodes ──────────────────────────────────────────────────────────────────────

def introspect_node(state: PipelineState) -> Dict[str, Any]:
    catalog = schema_introspector.introspect(
        tables=state["tables"], schema=state.get("schema_name", "datapai"),
        domain=state.get("domain", "stock"), target_contracts=state.get("target_contracts", []),
    )
    return {"catalog": catalog.model_dump(), "status": "introspected",
            "messages": _log(state, f"introspect: {len(catalog.tables)} tables, {len(catalog.target_contracts)} contracts")}


def plan_node(state: PipelineState) -> Dict[str, Any]:
    if state.get("plan"):  # plan supplied / already produced → skip the LLM
        return {"status": "planned", "messages": _log(state, "plan: reusing existing plan")}
    catalog = SourceCatalog.model_validate(state["catalog"])
    try:
        plan = medallion_planner.plan(catalog=catalog, goal=state["goal"], required_models=state["required_models"],
                                      tag=state["tag"], source_name=state.get("source_name"))
        return {"plan": plan.model_dump(), "plan_errors": [], "status": "planned",
                "messages": _log(state, f"plan: {len(plan.models)} models, order={plan.build_order}")}
    except medallion_planner.PlanningError as exc:
        return {"plan_errors": exc.errors, "status": "plan_failed", "messages": _log(state, f"plan failed: {exc.errors}")}


def validate_node(state: PipelineState) -> Dict[str, Any]:
    if not state.get("plan"):
        return {"status": "plan_failed"}
    plan = MedallionPlan.model_validate(state["plan"])
    errors = plan.validate_graph(SourceCatalog.model_validate(state["catalog"]), state.get("required_models"))
    return {"plan": plan.model_dump(), "plan_errors": errors, "status": "plan_valid" if not errors else "plan_invalid",
            "messages": _log(state, f"validate: {'ok' if not errors else errors}")}


def load_bronze_node(state: PipelineState) -> Dict[str, Any]:
    catalog = SourceCatalog.model_validate(state["catalog"])
    try:
        res = raw_loader.load_bronze(catalog)
        return {"bronze": res, "status": "bronze_loaded", "messages": _log(state, f"load_bronze: {res}")}
    except Exception as exc:  # availability over strictness: continue, staging will fail loudly if a source is missing
        return {"bronze": {}, "status": "bronze_failed", "messages": _log(state, f"load_bronze failed (continuing): {exc}")}


def generate_node(state: PipelineState) -> Dict[str, Any]:
    plan = MedallionPlan.model_validate(state["plan"])
    catalog = SourceCatalog.model_validate(state["catalog"])
    generated = model_generator.generate_all(plan, catalog, _target(state), generated=state.get("generated"))
    return {"generated": generated, "status": "generated",
            "messages": _log(state, f"generate: {len(generated)} models written under {_target(state).dir_for('staging').parent}")}


def build_repair_node(state: PipelineState) -> Dict[str, Any]:
    plan = MedallionPlan.model_validate(state["plan"])
    catalog = SourceCatalog.model_validate(state["catalog"])
    res = build_loop.build_and_repair(plan, catalog, _target(state), dict(state.get("generated") or {}))
    summary = {k: v for k, v in res.items() if k not in ("generated", "final_results")}
    summary["final_failed"] = [f"{f['resource_type']}:{f['name']}" for f in build_loop.failures(res["final_results"])]
    summary["final_nodes"] = len(res["final_results"])
    return {"generated": res["generated"], "build": summary, "attempts": len(res["attempts"]),
            "status": "built" if res["ok"] else "build_failed",
            "messages": _log(state, f"build: ok={res['ok']} attempts={len(res['attempts'])} repairs={len(res['repairs'])} failed={summary['final_failed']}")}


def _pending(name: str):
    def _node(state: PipelineState) -> Dict[str, Any]:
        return {"status": f"{name}_pending", "messages": _log(state, f"{name}: not implemented yet (week 3+)")}
    _node.__name__ = f"{name}_node"
    return _node


# ── graph ──────────────────────────────────────────────────────────────────────

def build_graph(checkpointer: Optional[Any] = None, stop_after: str = "diff", hitl: bool = False):
    """
    stop_after: 'validate' (week 1), 'build_repair' (week 2) or 'diff' (full).
    hitl: interrupt before load_bronze so a human can review the plan.
    """
    g = StateGraph(PipelineState)
    g.add_node("introspect", introspect_node)
    g.add_node("plan", plan_node)
    g.add_node("validate", validate_node)
    g.add_edge(START, "introspect")
    g.add_edge("introspect", "plan")
    g.add_edge("plan", "validate")
    if stop_after == "validate":
        g.add_edge("validate", END)
        return g.compile(checkpointer=checkpointer)

    g.add_node("load_bronze", load_bronze_node)
    g.add_node("generate", generate_node)
    g.add_node("build_repair", build_repair_node)
    g.add_conditional_edges("validate", lambda s: "load_bronze" if s.get("status") == "plan_valid" else END,
                            {"load_bronze": "load_bronze", END: END})
    g.add_edge("load_bronze", "generate")
    g.add_edge("generate", "build_repair")
    if stop_after == "build_repair":
        g.add_edge("build_repair", END)
    else:
        for n in ("register", "emit_dag", "diff"):
            g.add_node(n, _pending(n))
        g.add_edge("build_repair", "register")
        g.add_edge("register", "emit_dag")
        g.add_edge("emit_dag", "diff")
        g.add_edge("diff", END)
    return g.compile(checkpointer=checkpointer, interrupt_before=["load_bronze"] if hitl else None)


def get_checkpointer(url: Optional[str] = None):
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
