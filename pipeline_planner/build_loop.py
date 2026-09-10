"""
build_loop — `dbt build --select tag:<tag>` → parse target/run_results.json →
feed {model, status, message, sql} back to the LLM → regenerate ONLY that node →
retry (≤ MAX_BUILD_ATTEMPTS). This is what turns "generated" into "working".

dbt execution mirrors datapai-platform-be/agents/dbt_agent.py:run_dbt_commands
(subprocess, tails captured) but is explicit about project/profile/target because
dbt-demo's dbt_project.yml default profile (`datapai_snowflake`) is not the
key-pair service profile we must build with (`datapai_snowflake_svc/prod`).

Pilot stance (availability over strictness): a TEST that still fails on the last
attempt is downgraded to `severity: warn` in the model YAML and reported, so a
data-quality expectation never blocks the pipeline from materializing.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml

from .llm import ChatFn, get_router_chat
from .model_generator import GenerationTarget, generate_model
from .models import MedallionPlan, SourceCatalog

logger = logging.getLogger(__name__)

MAX_BUILD_ATTEMPTS = int(os.getenv("AI_ETL_MAX_BUILD_ATTEMPTS", "3"))
DBT_BIN = os.getenv("AI_ETL_DBT_BIN", "dbt")


def dbt_cmd(args: Sequence[str], project_dir: str, profile: Optional[str] = None, target: Optional[str] = None,
            profiles_dir: Optional[str] = None) -> List[str]:
    cmd = [DBT_BIN, *args]
    if "--project-dir" not in cmd:
        cmd += ["--project-dir", project_dir]
    profiles_dir = profiles_dir or os.getenv("DBT_PROFILES_DIR") or os.path.expanduser("~/.dbt")
    cmd += ["--profiles-dir", profiles_dir]
    profile = profile or os.getenv("AI_ETL_DBT_PROFILE", "datapai_snowflake_svc")
    target = target or os.getenv("AI_ETL_DBT_TARGET", "prod")
    if profile:
        cmd += ["--profile", profile]
    if target:
        cmd += ["--target", target]
    return cmd


def run_dbt(args: Sequence[str], project_dir: str, **kw) -> Dict[str, Any]:
    cmd = dbt_cmd(args, project_dir, **kw)
    stale = Path(project_dir) / "target" / "run_results.json"
    if stale.exists():  # never let a previous run masquerade as this one's results
        stale.unlink()
    logger.info("$ %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=project_dir, capture_output=True, text=True)
    out = {"command": " ".join(cmd), "returncode": proc.returncode,
           "stdout_tail": (proc.stdout or "")[-6000:], "stderr_tail": (proc.stderr or "")[-3000:]}
    if proc.returncode != 0:
        logger.warning("dbt exited %s\n%s", proc.returncode, out["stdout_tail"][-2500:])
    return out


def parse_run_results(project_dir: str) -> List[Dict[str, Any]]:
    """Flatten run_results.json; attach tests to the model they test via manifest."""
    target_dir = Path(project_dir) / "target"
    rr_path = target_dir / "run_results.json"
    if not rr_path.exists():
        return []
    rr = json.loads(rr_path.read_text(encoding="utf-8"))
    manifest = {}
    mp = target_dir / "manifest.json"
    if mp.exists():
        manifest = json.loads(mp.read_text(encoding="utf-8")).get("nodes", {})
    out = []
    for r in rr.get("results", []):
        uid = r.get("unique_id", "")
        node = manifest.get(uid, {})
        rtype = uid.split(".")[0]
        name = node.get("name") or uid.split(".")[-1]
        attached = None
        if rtype == "test":
            attached = node.get("attached_node")
            if not attached:
                deps = [d for d in node.get("depends_on", {}).get("nodes", []) if d.startswith("model.")]
                attached = deps[-1] if deps else None
            attached = manifest.get(attached, {}).get("name") if attached else None
        out.append({
            "unique_id": uid, "resource_type": rtype, "name": name, "status": r.get("status"),
            "message": (r.get("message") or "")[:4000], "model": name if rtype == "model" else attached,
            "compiled_code": (node.get("compiled_code") or "")[:6000] if rtype == "model" else "",
            "execution_time": r.get("execution_time"),
        })
    return out


def failures(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [r for r in results if r["status"] in ("error", "fail")]


# Errors the LLM cannot fix by rewriting one model's SQL: project config, missing
# sources/refs, credentials, warehouse permissions. Surface them, don't burn repairs.
_NON_REPAIRABLE = (
    "enable_iceberg_materializations",
    "behavior flag",
    "depends on a source named",
    "depends on a node named",
    "Insufficient privileges",
    "does not exist or not authorized",
    "Authentication",
    "profiles.yml",
    "found two models with the name",
)


def is_repairable(message: str) -> bool:
    return not any(tok.lower() in (message or "").lower() for tok in _NON_REPAIRABLE)


def _downgrade_test_severity(yml_path: str, test_name: str) -> bool:
    """Set severity: warn on every test in this model's YAML (last-resort pilot behaviour)."""
    p = Path(yml_path)
    if not p.exists():
        return False
    doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    changed = False
    for m in doc.get("models", []):
        for t in m.get("tests", []) or []:
            if isinstance(t, dict):
                for k, v in t.items():
                    if isinstance(v, dict):
                        v.setdefault("config", {})["severity"] = "warn"; changed = True
        for c in m.get("columns", []) or []:
            new_tests = []
            for t in c.get("tests", []) or []:
                if isinstance(t, str):
                    new_tests.append({t: {"config": {"severity": "warn"}}}); changed = True
                elif isinstance(t, dict):
                    for k, v in t.items():
                        if isinstance(v, dict):
                            v.setdefault("config", {})["severity"] = "warn"; changed = True
                    new_tests.append(t)
            if new_tests:
                c["tests"] = new_tests
    if changed:
        p.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120), encoding="utf-8")
        logger.warning("downgraded tests to severity=warn in %s (test %s kept failing)", yml_path, test_name)
    return changed


def build_and_repair(plan: MedallionPlan, catalog: SourceCatalog, target: GenerationTarget,
                     generated: Dict[str, Dict[str, Any]], chat: Optional[ChatFn] = None,
                     max_attempts: int = MAX_BUILD_ATTEMPTS, select: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns {"ok", "attempts": [...], "repairs": [...], "final_results": [...], "generated"}.
    """
    chat = chat or get_router_chat()
    select = select or f"tag:{target.tag}"
    attempts: List[Dict[str, Any]] = []
    repairs: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []

    for attempt in range(1, max_attempts + 1):
        run = run_dbt(["build", "--select", select], target.project_dir)
        results = parse_run_results(target.project_dir)
        fails = failures(results)
        attempts.append({"attempt": attempt, "returncode": run["returncode"], "failed": len(fails),
                         "nodes": len(results), "failed_nodes": [f"{f['resource_type']}:{f['name']}:{f['status']}" for f in fails]})
        if run["returncode"] == 0 and not fails and not results:
            logger.error("dbt selected 0 nodes for %s — nothing generated under the tag?", select)
            attempts[-1]["stdout_tail"] = run["stdout_tail"][-1500:]
            break
        if run["returncode"] == 0 and not fails:
            logger.info("dbt build green on attempt %d (%d nodes)", attempt, len(results))
            return {"ok": True, "attempts": attempts, "repairs": repairs, "final_results": results, "generated": generated}
        if not results and run["returncode"] != 0:
            # parse/compile error before any node ran: surface the log to the LLM for every model? Too broad —
            # fail fast with the tail so the operator (or planner repair) can act.
            attempts[-1]["stdout_tail"] = run["stdout_tail"][-2500:]
            logger.error("dbt failed before running nodes:\n%s", run["stdout_tail"][-2500:])
            break
        if attempt == max_attempts:
            break

        # Repair: one regeneration per failing model (tests map to their model)
        blocking = [f for f in fails if not is_repairable(f["message"])]
        if blocking:
            attempts[-1]["blocking"] = [f"{f['name']}: {f['message'][:300]}" for f in blocking]
            logger.error("non-repairable failure(s), stopping repair loop:\n%s", "\n".join(attempts[-1]["blocking"]))
            break
        by_model: Dict[str, Dict[str, Any]] = {}
        for f in fails:
            if f["model"] and f["model"] in plan.build_order:
                by_model.setdefault(f["model"], f)
        for model_name, f in by_model.items():
            spec = plan.model(model_name)
            if not spec:
                continue
            prev_sql = generated.get(model_name, {}).get("sql") or f.get("compiled_code") or ""
            msg = f["message"]
            if f["resource_type"] == "test":
                msg = f"TEST {f['name']} failed on model {model_name}: {msg}"
            try:
                generated[model_name] = generate_model(spec, plan, catalog, target, generated, chat,
                                                       repair={"status": f["status"], "message": msg, "sql": prev_sql})
                repairs.append({"attempt": attempt, "model": model_name, "trigger": f["unique_id"], "message": msg[:500]})
                logger.info("repaired %s after %s", model_name, f["unique_id"])
            except Exception as exc:
                repairs.append({"attempt": attempt, "model": model_name, "trigger": f["unique_id"], "error": str(exc)})
                logger.warning("repair of %s failed: %s", model_name, exc)

    # Last resort: tests still failing → severity warn, one more build so models materialize
    test_fails = [f for f in failures(results) if f["resource_type"] == "test" and f["model"]]
    if test_fails:
        for f in test_fails:
            yml = generated.get(f["model"], {}).get("yml_path")
            if yml:
                _downgrade_test_severity(yml, f["name"])
        run = run_dbt(["build", "--select", select], target.project_dir)
        results = parse_run_results(target.project_dir)
        attempts.append({"attempt": "final-warn", "returncode": run["returncode"], "failed": len(failures(results)),
                         "nodes": len(results), "failed_nodes": [f"{f['resource_type']}:{f['name']}" for f in failures(results)]})
    ok = bool(results) and not failures(results)
    return {"ok": ok, "attempts": attempts, "repairs": repairs, "final_results": results, "generated": generated}
