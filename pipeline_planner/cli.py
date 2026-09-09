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


def cmd_run(args) -> int:
    from pipeline_planner import graph as g

    req = _merge_request(args)
    cp = g.get_checkpointer()
    thread_id = args.thread_id or f"{req['domain']}-{uuid.uuid4().hex[:8]}"
    config = {"configurable": {"thread_id": thread_id}}

    def _exec(checkpointer):
        app = g.build_graph(checkpointer=checkpointer, week1_only=True)
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
    if final.get("plan_errors"):
        print("errors:", *final["plan_errors"], sep="\n  - ")
        return 1
    return 0


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
    sp.add_argument("--thread-id")
    sp.set_defaults(fn=cmd_run)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
