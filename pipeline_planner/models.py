"""
Typed contracts shared by every pipeline_planner node.

SourceCatalog  — what the introspector learned about the raw tables
MedallionPlan  — what the planner decided to build (the LLM's JSON, validated)

MedallionPlan extends the philosophy of datapai-platform-be/agents/dbt_agent.py:DbtPlan
(LLM = planner JSON, Python = executor) to four layers with explicit ref() lineage.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

Layer = Literal["staging", "intermediate", "dimension", "fact"]
LAYER_ORDER: Dict[str, int] = {"staging": 0, "intermediate": 1, "dimension": 2, "fact": 3}


# ═══════════════════════════════════════════════════════════════════════════════
# SourceCatalog
# ═══════════════════════════════════════════════════════════════════════════════

class ColumnProfile(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    null_pct: Optional[float] = None
    distinct_count: Optional[int] = None
    low_cardinality: bool = False
    sample_values: List[str] = Field(default_factory=list)
    # From vendored compliance_tools heuristics
    pii_type: str = "none"
    sensitivity: str = "PUBLIC"


class KeyCandidate(BaseModel):
    columns: List[str]
    kind: Literal["primary_key", "unique_index", "inferred"]
    verified: bool = False  # True when check_primary_key() confirmed on a sample


class ForeignKeyCandidate(BaseModel):
    columns: List[str]
    ref_table: str
    ref_columns: List[str]
    source: Literal["constraint", "inferred"]
    confidence: float = 1.0
    match_rate: Optional[float] = None          # share of sampled child rows found in the parent
    match_rate_by: Dict[str, float] = Field(default_factory=dict)  # e.g. {"ASX": 0.0, "US": 1.0}
    alternatives: List[Dict[str, Any]] = Field(default_factory=list)  # [{"ref_columns": [...], "match_rate": 0.99}]
    # Data-quality conforming rules discovered by testing normalisations of the child key against the parent:
    # [{"expr": "split_part(ticker, '.', 1)", "match_rate": 1.0, "match_rate_by": {...}}]. The planner must turn the
    # winner into a staging transformation (keep <col>_raw) so keys are consistent before any join.
    conform_rules: List[Dict[str, Any]] = Field(default_factory=list)


class TableProfile(BaseModel):
    schema_name: str
    name: str
    row_count: int = 0
    row_count_method: str = "exact"  # exact | stats | partition_stats
    is_partitioned: bool = False
    partitions: List[str] = Field(default_factory=list)
    columns: List[ColumnProfile] = Field(default_factory=list)
    keys: List[KeyCandidate] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyCandidate] = Field(default_factory=list)
    description: str = ""

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.name}"

    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]


class SourceCatalog(BaseModel):
    source_system: str = "postgres"
    database: str = ""
    schema_name: str = ""
    domain: str = ""
    introspected_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tables: List[TableProfile] = Field(default_factory=list)
    # Optional: existing tables whose column contract a generated mart must match
    target_contracts: List[TableProfile] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)

    def table(self, name: str) -> Optional[TableProfile]:
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def compact(self) -> Dict[str, Any]:
        """Token-lean projection for the LLM prompt."""
        def _tbl(t: TableProfile) -> Dict[str, Any]:
            return {
                "table": t.name,
                "row_count": t.row_count,
                "partitioned": t.is_partitioned,
                "keys": [{"columns": k.columns, "kind": k.kind, "verified": k.verified} for k in t.keys],
                "foreign_keys": [
                    {"columns": f.columns, "ref": f"{f.ref_table}({','.join(f.ref_columns)})", "source": f.source,
                     **({"match_rate": f.match_rate} if f.match_rate is not None else {}),
                     **({"match_rate_by": f.match_rate_by} if f.match_rate_by else {}),
                     **({"better_join": f.alternatives} if f.alternatives else {}),
                     **({"conform_rules": f.conform_rules} if f.conform_rules else {})}
                    for f in t.foreign_keys
                ],
                "columns": [
                    {
                        "name": c.name,
                        "type": c.data_type,
                        "nullable": c.nullable,
                        **({"null_pct": c.null_pct} if c.null_pct else {}),
                        **({"distinct": c.distinct_count} if c.distinct_count is not None else {}),
                        **({"sensitivity": c.sensitivity} if c.sensitivity != "PUBLIC" else {}),
                        **({"samples": c.sample_values[:3]} if c.sample_values else {}),
                    }
                    for c in t.columns
                ],
            }

        return {
            "source_system": self.source_system,
            "schema": self.schema_name,
            "domain": self.domain,
            "tables": [_tbl(t) for t in self.tables],
            "target_contracts": [
                {"table": t.name, "columns": [{"name": c.name, "type": c.data_type} for c in t.columns]}
                for t in self.target_contracts
            ],
            "notes": self.notes,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# MedallionPlan
# ═══════════════════════════════════════════════════════════════════════════════

class TestSpec(BaseModel):
    """
    type: not_null | unique | accepted_values | relationships | unique_combination_of_columns |
          any dbt_expectations test name (with or without the 'dbt_expectations.' prefix), e.g.
          expect_column_values_to_be_between {min_value, max_value}, expect_column_values_to_be_in_set {values},
          expect_column_values_to_not_be_null, expect_column_values_to_match_regex {regex},
          expect_table_row_count_to_equal_other_table {compare_model}, expect_column_mean_to_be_between ...
    params: extra keyword arguments passed verbatim to the dbt_expectations test.
    """
    type: str
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    values: Optional[List[Any]] = None
    to: Optional[str] = None  # relationships: ref model name
    field: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    severity: Optional[Literal["warn", "error"]] = None

    def is_expectation(self) -> bool:
        return self.type.startswith("dbt_expectations.") or self.type.startswith("expect_")

    def expectation_name(self) -> str:
        return self.type if self.type.startswith("dbt_expectations.") else f"dbt_expectations.{self.type}"


class ForeignKeySpec(BaseModel):
    column: str
    ref_model: str
    ref_column: str


class ModelSpec(BaseModel):
    name: str
    layer: Layer
    description: str = ""
    # staging models read raw tables; every other layer reads refs
    sources: List[str] = Field(default_factory=list)   # raw table names (staging only)
    refs: List[str] = Field(default_factory=list)      # upstream model names
    grain: List[str] = Field(default_factory=list)     # columns that make a row unique
    natural_key: List[str] = Field(default_factory=list)
    surrogate_key: Optional[str] = None
    scd_type: Optional[int] = None                     # dims only: 1 or 2
    foreign_keys: List[ForeignKeySpec] = Field(default_factory=list)
    measures: List[str] = Field(default_factory=list)  # facts / intermediate
    columns: List[str] = Field(default_factory=list)   # output column contract (optional)
    logic: str = ""                                    # plain-English transform spec for the generator
    tests: List[TestSpec] = Field(default_factory=list)
    materialized: str = "table"
    tags: List[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _snake(cls, v: str) -> str:
        v = v.strip()
        if not v or not all(ch.isalnum() or ch == "_" for ch in v) or v[0].isdigit():
            raise ValueError(f"model name must be snake_case identifier, got {v!r}")
        return v.lower()


class MedallionPlan(BaseModel):
    domain: str
    source_name: str                        # dbt source name, e.g. src_stock
    target: str = "snowflake_iceberg"       # decision #4/#5
    tag: str                                # e.g. stock_ai — every model carries it
    models: List[ModelSpec] = Field(default_factory=list)
    build_order: List[str] = Field(default_factory=list)  # filled by validate()
    notes: str = ""
    planner_meta: Dict[str, Any] = Field(default_factory=dict)

    # ── lookups ────────────────────────────────────────────────────────────
    def model(self, name: str) -> Optional[ModelSpec]:
        for m in self.models:
            if m.name == name:
                return m
        return None

    def by_layer(self, layer: Layer) -> List[ModelSpec]:
        return [m for m in self.models if m.layer == layer]

    # ── validation ─────────────────────────────────────────────────────────
    def validate_graph(
        self,
        catalog: Optional[SourceCatalog] = None,
        required_models: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Return a list of human-readable problems (empty == valid).
        Also fills self.build_order with a deterministic topological order.
        """
        errors: List[str] = []
        names = [m.name for m in self.models]
        if len(names) != len(set(names)):
            dupes = sorted({n for n in names if names.count(n) > 1})
            errors.append(f"duplicate model names: {dupes}")
        name_set = set(names)
        raw_tables = {t.name for t in catalog.tables} if catalog else None

        for m in self.models:
            if m.layer == "staging":
                if not m.sources:
                    errors.append(f"{m.name}: staging model must declare sources")
                if m.refs:
                    errors.append(f"{m.name}: staging model must not ref() other models (got {m.refs})")
                if raw_tables is not None:
                    for s in m.sources:
                        if s not in raw_tables:
                            errors.append(f"{m.name}: source table {s!r} is not in the catalog")
            else:
                if m.sources:
                    errors.append(f"{m.name}: only staging models may read raw sources (got {m.sources})")
                if not m.refs:
                    errors.append(f"{m.name}: {m.layer} model must ref() at least one upstream model")
                for r in m.refs:
                    if r not in name_set:
                        errors.append(f"{m.name}: ref {r!r} does not exist in the plan")
                    elif r == m.name:
                        errors.append(f"{m.name}: self-reference")
            if m.layer in ("dimension", "fact") and not m.grain and not m.natural_key:
                errors.append(f"{m.name}: {m.layer} must declare grain or natural_key")
            for fk in m.foreign_keys:
                if fk.ref_model not in name_set:
                    errors.append(f"{m.name}: foreign key {fk.column} -> unknown model {fk.ref_model!r}")
            if m.layer == "fact":
                fk_targets = {fk.ref_model for fk in m.foreign_keys}
                for r in m.refs:
                    up = self.model(r)
                    if up and up.layer == "dimension" and r not in fk_targets:
                        errors.append(f"{m.name}: refs dimension {r} but declares no foreign_keys entry to it")
            if self.tag and self.tag not in m.tags:
                m.tags.append(self.tag)

        if required_models:
            missing = [r for r in required_models if r not in name_set]
            if missing:
                errors.append(f"required models missing from plan: {missing}")

        # Layer discipline: a model may only depend on same-or-lower layers,
        # except facts may depend on dims/intermediate and dims on intermediate.
        for m in self.models:
            for r in m.refs:
                up = self.model(r)
                if up and LAYER_ORDER[up.layer] > LAYER_ORDER[m.layer]:
                    errors.append(f"{m.name} ({m.layer}) depends on {r} ({up.layer}): downstream layer referenced upstream")

        order, cycle = self._topo_sort()
        if cycle:
            errors.append(f"cycle detected among models: {cycle}")
        self.build_order = order
        return errors

    def _topo_sort(self):
        """Kahn's algorithm, deterministic (layer order, then name)."""
        indeg = {m.name: 0 for m in self.models}
        children: Dict[str, List[str]] = {m.name: [] for m in self.models}
        for m in self.models:
            for r in m.refs:
                if r in indeg:
                    indeg[m.name] += 1
                    children[r].append(m.name)

        def key(n: str):
            mm = self.model(n)
            return (LAYER_ORDER[mm.layer] if mm else 99, n)

        ready = sorted([n for n, d in indeg.items() if d == 0], key=key)
        order: List[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for c in children[n]:
                indeg[c] -= 1
                if indeg[c] == 0:
                    ready.append(c)
            ready.sort(key=key)
        cycle = sorted([n for n, d in indeg.items() if d > 0])
        return order, cycle

    # ── views ──────────────────────────────────────────────────────────────
    def lineage_edges(self) -> List[Dict[str, str]]:
        edges = []
        for m in self.models:
            for s in m.sources:
                edges.append({"from": f"source:{s}", "to": m.name})
            for r in m.refs:
                edges.append({"from": r, "to": m.name})
        return edges

    def summary(self) -> str:
        lines = [f"MedallionPlan domain={self.domain} tag={self.tag} target={self.target} models={len(self.models)}"]
        for i, n in enumerate(self.build_order, 1):
            m = self.model(n)
            if not m:
                continue
            deps = m.sources if m.layer == "staging" else m.refs
            lines.append(f"  {i:>2}. [{m.layer:<12}] {m.name:<32} <- {', '.join(deps)}")
        return "\n".join(lines)
