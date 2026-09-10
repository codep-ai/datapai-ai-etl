"""
CLI for pipeline_planner (run on EC2 inside /home/ec2-user/venv_ai_etl).

  # 1. Catalog only
  python -m pipeline_planner.cli introspect --tables prices,ticker_universe,exchange_ref,fx_rates_daily,stock_market_closed_dates \
      --contracts screener_metrics --out out/stock_catalog.json

  # 2. Plan from a saved catalog (LLM via RouterChatClient)
  python -m pipeline_planner.cli plan --catalog out/stock_catalog.json --out out/stock_plan.json

  # 3. Whole week-1 graph (introspect → plan → validate) with checkpointing
  AI_ETL_CHECKPOINT=sqlite:///out/checkpoints.db python -m pipeline_planner.cli run --out out/stock_plan.json

  # 4. Validate a hand-edited plan
  python -m pipeline_planner.cli validate --plan out/stock_plan.json --catalog out/stock_catalog.json

Defaults for the stock MVP domain come from --domain-config (JSON) or env, never
from code, per the DB-driven-defaults rule; the stock config ships as
pipeline_planner/domains/stock.json and will move to sys_ai_pipeline (migration 001).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

# repo root on sys.path so `platform_init` resolves when run as a module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_planner import medallion_planner, schema_introspector  # noqa: E402
from pipeline_planner.models import MedallionPlan, SourceCatalog  # noqa: E402

DOMAINS_DIR = Path(__file__).parent / "domains"


def _load_domain(name: str) -> Dict[str, Any]:
    p = DOMAINS_DIR / f"{name}.json"
    if not p.exists():
        raise SystemExit(f"no domain config {p}; pass --tables/--goal/--required explicitly")
    return json.loads(p.read_text(encoding="utf-8"))


def _csv(v: str | None) -> List[str]:
    return [x.strip() for x in v.split(",") if x.strip()] if v else []


def _merge_request(args) -> Dict[str, Any]:
    cfg = _load_domain(args.domain) if (DOMAINS_DIR / f"{args.domain}.json").exists() else {}
    req = {
        "domain": args.domain,
        "schema_name": args.schema or cfg.get("schema_name", "datapai"),
        "tables": _csv(args.tables) or cfg.get("tables", []),
        "target_contracts": _csv(getattr(args, "contracts", None)) or cfg.get("target_contracts", []),
        "goal": getattr(args, "goal", None) or cfg.get("goal", ""),
        "required_models": _csv(getattr(args, "required", None)) or cfg.get("required_models", []),
        "tag": getattr(args, "tag", None) or cfg.get("tag", f"{args.domain}_ai"),
        "source_name": getattr(args, "source_name", None) or cfg.get("source_name", f"src_{args.domain}"),
        "dbt_project_dir": cfg.get("dbt_project_dir") or os.getenv("AI_ETL_DBT_PROJECT_DIR", "/home/ec2-user/git/dbt-demo"),
        "dbt_models_subdir": cfg.get("dbt_models_subdir") or os.getenv("AI_ETL_DBT_MODELS_SUBDIR", f"models/{args.domain}"),
        "hitl_required": bool(cfg.get("hitl_required", False)),
    }
    if not req["tables"]:
        raise SystemExit("no tables: pass --tables or add pipeline_planner/domains/<domain>.json")
    return req


def _write(obj: Any, out: str | None) -> None:
    text = json.dumps(obj, indent=2, default=str)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(text, encoding="utf-8")
        print(f"wrote {out}")
    else:
        print(text)


def cmd_introspect(args) -> int:
    req = _merge_request(args)
    cat = schema_introspector.introspect(
        tables=req["tables"], schema=req["schema_name"], domain=req["domain"],
        target_contracts=req["target_contracts"], profile=not args.no_profile, verify_keys=not args.no_verify,
    )
    _write(cat.model_dump(), args.out)
    for t in cat.tables:
        keys = ";".join("+".join(k.columns) + ("✓" if k.verified else "?") for k in t.keys) or "-"
        fks = ";".join(f"{'+'.join(f.columns)}→{f.ref_table}" for f in t.foreign_keys) or "-"
        pii = [c.name for c in t.columns if c.sensitivity != "PUBLIC"]
        print(f"  {t.name:<28} rows={t.row_count:>10,} cols={len(t.columns):>2} keys={keys} fks={fks} pii={pii}")
    return 0


def cmd_plan(args) -> int:
    req = _merge_request(args)
    cat = SourceCatalog.model_validate(json.loads(Path(args.catalog).read_text(encoding="utf-8")))
    try:
        plan = medallion_planner.plan(
            catalog=cat, goal=req["goal"], required_models=req["required_models"],
            tag=req["tag"], source_name=req["source_name"],
        )
    except medallion_planner.PlanningError as exc:
        print("PLANNING FAILED:", *exc.errors, sep="\n  - ")
        if args.dump_raw:
            Path(args.dump_raw).write_text(exc.last_raw, encoding="utf-8")
            print(f"raw LLM output saved to {args.dump_raw}")
        return 2
    _write(plan.model_dump(), args.out)
    print(plan.summary())
    return 0


def cmd_validate(args) -> int:
    plan = medallion_planner.load_plan(args.plan)
    cat = SourceCatalog.model_validate(json.loads(Path(args.catalog).read_text(encoding="utf-8"))) if args.catalog else None
    required = _csv(args.required) or (_load_domain(args.domain).get("required_models") if (DOMAINS_DIR / f"{args.domain}.json").exists() else [])
    errors = plan.validate_graph(cat, required)
    print(plan.summary())
    if errors:
        print("INVALID:", *errors, sep="\n  - ")
        return 1
    print("VALID")
    if args.out:
        medallion_planner.save_plan(plan, args.out)
    return 0


def _target_from(req):
    from pipeline_planner.model_generator import GenerationTarget
    return GenerationTarget(project_dir=req["dbt_project_dir"], models_subdir=req["dbt_models_subdir"], tag=req["tag"])


def _load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cmd_load_bronze(args) -> int:
    from pipeline_planner import raw_loader
    cat = SourceCatalog.model_validate(_load_json(args.catalog))
    res = raw_loader.load_bronze(cat, only=_csv(args.only) or None)
    print(json.dumps(res, indent=2)); return 0


def cmd_generate(args) -> int:
    from pipeline_planner import model_generator
    req = _merge_request(args)
    cat = SourceCatalog.model_validate(_load_json(args.catalog))
    plan = medallion_planner.load_plan(args.plan)
    errors = plan.validate_graph(cat, req["required_models"])
    if errors:
        print("plan invalid:", *errors, sep="\n  - "); return 1
    prev = _load_json(args.state) if args.state and Path(args.state).exists() else {}
    gen = model_generator.generate_all(plan, cat, _target_from(req), only=_csv(args.only) or None, generated=prev)
    _write(gen, args.state or args.out)
    for n in plan.build_order:
        if n in gen: print(f"  {n:<32} {gen[n]['sql_path']}  cols={len(gen[n]['columns'])}")
    return 0


def cmd_build(args) -> int:
    from pipeline_planner import build_loop
    req = _merge_request(args)
    cat = SourceCatalog.model_validate(_load_json(args.catalog))
    plan = medallion_planner.load_plan(args.plan)
    plan.validate_graph(cat, req["required_models"])
    gen = _load_json(args.state) if args.state and Path(args.state).exists() else {}
    res = build_loop.build_and_repair(plan, cat, _target_from(req), gen, max_attempts=args.max_attempts)
    if args.state:
        _write(res["generated"], args.state)
    summary = {k: v for k, v in res.items() if k not in ("generated", "final_results")}
    summary["final_failed"] = [f"{f['resource_type']}:{f['name']}: {f['message'][:200]}" for f in build_loop.failures(res["final_results"])]
    _write(summary, args.out)
    print("BUILD", "GREEN" if res["ok"] else "RED", f"attempts={len(res['attempts'])} repairs={len(res['repairs'])}")
    return 0 if res["ok"] else 1


def cmd_lineage(args) -> int:
    from pipeline_planner import column_lineage
    req = _merge_request(args)
    cat = SourceCatalog.model_validate(_load_json(args.catalog))
    plan = medallion_planner.load_plan(args.plan)
    plan.validate_graph(cat, req["required_models"])
    gen = _load_json(args.state) if args.state and Path(args.state).exists() else {}
    lin = column_lineage.compute_lineage(req["dbt_project_dir"], plan, cat, gen)
    _write(lin, args.out)
    print(f"models={lin['models']} edges={len(lin['edges'])} unresolved={len(lin['unresolved'])} restricted={lin['restricted_columns'][:10]}")
    if args.mermaid:
        Path(args.mermaid).write_text(column_lineage.to_mermaid(lin, model=args.model), encoding="utf-8"); print(f"wrote {args.mermaid}")
    if args.annotate:
        touched = column_lineage.annotate_yaml(lin, gen)
        print(f"annotated {len(touched)} YAML files with column meta.lineage / meta.sensitivity")
    run_id = args.run_id or __import__("datetime").datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    pipeline_id = f"{req['domain']}_ai_medallion"
    if args.to_db:
        res = column_lineage.write_lineage_pg(lin, pipeline_id=pipeline_id, run_id=run_id)
        print(f"system db: {res['rows']} edges -> {res['table']} (run_id={run_id})")
    if args.to_warehouse:
        res = column_lineage.write_lineage_table(lin, pipeline_id=pipeline_id, run_id=run_id)
        print(f"warehouse projection: {res['rows']} edges -> {res['table']} (run_id={run_id})")
    if args.patch_docs:
        print("dbt docs index.html patched:", column_lineage.patch_dbt_docs(req["dbt_project_dir"]))
    print("traced:", lin["summary"])
    return 0


def cmd_lineage_project(args) -> int:
    """Column lineage for ANY dbt project from manifest.json + catalog.json (no generator state)."""
    from pipeline_planner import column_lineage
    lin = column_lineage.lineage_from_artifacts(args.project_dir, dialect=args.dialect, select=args.select)
    _write(lin, args.out)
    print(f"models={lin['models']} edges={len(lin['edges'])} unresolved={len(lin['unresolved'])} warnings={len(lin['warnings'])} restricted={len(lin['restricted_columns'])}")
    for m, sm in lin["summary"].items():
        print(f"  {m:<36} cols={sm['columns']:>3} traced={sm['traced']:>3}")
    if args.to_target:
        p = Path(args.project_dir) / "target" / "lineage.json"; p.write_text(json.dumps(lin, default=str), encoding="utf-8"); print(f"wrote {p} (read by assets/lineage.html)")
    if args.to_db:
        run_id = __import__("datetime").datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        res = column_lineage.write_lineage_pg(lin, pipeline_id=args.pipeline_id, run_id=run_id); print(f"system db: {res['rows']} edges (run_id={run_id})")
    return 0


def cmd_run(args) -> int:
    from pipeline_planner import graph as g

    req = _merge_request(args)
    cp = g.get_checkpointer()
    thread_id = args.thread_id or f"{req['domain']}-{uuid.uuid4().hex[:8]}"
    config = {"configurable": {"thread_id": thread_id}}

    if args.plan:
        req["plan"] = _load_json(args.plan)

    def _exec(checkpointer):
        app = g.build_graph(checkpointer=checkpointer, stop_after=args.stop_after, hitl=req.get("hitl_required", False))
        return app.invoke(req, config=config)

    if cp is None:
        final = _exec(None)
    else:
        with cp as saver:
            if hasattr(saver, "setup"):
                saver.setup()
            final = _exec(saver)

    print(f"thread_id={thread_id} status={final.get('status')}")
    for m in final.get("messages", []):
        print("  ", m)
    if final.get("plan"):
        plan = MedallionPlan.model_validate(final["plan"])
        _write(plan.model_dump(), args.out)
        print(plan.summary())
    if final.get("build"):
        print(json.dumps(final["build"], indent=2, default=str))
    if final.get("plan_errors"):
        print("errors:", *final["plan_errors"], sep="\n  - ")
        return 1
    return 0 if final.get("status") in ("plan_valid", "built", "diff_pending") else 1


def main(argv=None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="pipeline_planner")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--domain", default="stock")
        sp.add_argument("--schema")
        sp.add_argument("--tables", help="comma-separated raw tables")
        sp.add_argument("--contracts", help="comma-separated existing tables a mart must match")
        sp.add_argument("--goal")
        sp.add_argument("--required", help="comma-separated required model names")
        sp.add_argument("--tag")
        sp.add_argument("--source-name", dest="source_name")
        sp.add_argument("--out")

    sp = sub.add_parser("introspect"); common(sp)
    sp.add_argument("--no-profile", action="store_true"); sp.add_argument("--no-verify", action="store_true")
    sp.set_defaults(fn=cmd_introspect)

    sp = sub.add_parser("plan"); common(sp)
    sp.add_argument("--catalog", required=True); sp.add_argument("--dump-raw")
    sp.set_defaults(fn=cmd_plan)

    sp = sub.add_parser("validate")
    sp.add_argument("--plan", required=True); sp.add_argument("--catalog"); sp.add_argument("--required")
    sp.add_argument("--domain", default="stock"); sp.add_argument("--out")
    sp.set_defaults(fn=cmd_validate)

    sp = sub.add_parser("run"); common(sp)
    sp.add_argument("--thread-id"); sp.add_argument("--plan", help="reuse a saved plan instead of calling the LLM")
    sp.add_argument("--stop-after", default="validate", choices=["validate", "build_repair", "diff"])
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("load-bronze"); sp.add_argument("--catalog", required=True); sp.add_argument("--only")
    sp.set_defaults(fn=cmd_load_bronze)

    sp = sub.add_parser("generate"); common(sp)
    sp.add_argument("--catalog", required=True); sp.add_argument("--plan", required=True)
    sp.add_argument("--only"); sp.add_argument("--state", help="generated-models state JSON (read+write)")
    sp.set_defaults(fn=cmd_generate)

    sp = sub.add_parser("build"); common(sp)
    sp.add_argument("--catalog", required=True); sp.add_argument("--plan", required=True)
    sp.add_argument("--state"); sp.add_argument("--max-attempts", type=int, default=3)
    sp.set_defaults(fn=cmd_build)

    sp = sub.add_parser("lineage"); common(sp)
    sp.add_argument("--catalog", required=True); sp.add_argument("--plan", required=True); sp.add_argument("--state")
    sp.add_argument("--mermaid"); sp.add_argument("--model")
    sp.add_argument("--annotate", action="store_true", help="write meta.lineage/meta.sensitivity into generated YAML")
    sp.add_argument("--to-db", dest="to_db", action="store_true", help="persist edges to datapai.sys_ai_column_lineage (framework Postgres, system of record)")
    sp.add_argument("--to-warehouse", dest="to_warehouse", action="store_true", help="optional read-only Iceberg projection into the warehouse for BI")
    sp.add_argument("--run-id", dest="run_id")
    sp.add_argument("--patch-docs", dest="patch_docs", action="store_true", help="inject the Column lineage link into dbt docs target/index.html")
    sp.set_defaults(fn=cmd_lineage)

    sp = sub.add_parser("lineage-project", help="column lineage for any dbt project from manifest.json + catalog.json")
    sp.add_argument("--project-dir", dest="project_dir", required=True); sp.add_argument("--dialect", default="snowflake")
    sp.add_argument("--select", help="substring / tag:<tag> / path filter on models"); sp.add_argument("--out")
    sp.add_argument("--to-target", dest="to_target", action="store_true", help="write target/lineage.json for the lineage page")
    sp.add_argument("--to-db", dest="to_db", action="store_true"); sp.add_argument("--pipeline-id", dest="pipeline_id", default="dbt_project")
    sp.set_defaults(fn=cmd_lineage_project)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
