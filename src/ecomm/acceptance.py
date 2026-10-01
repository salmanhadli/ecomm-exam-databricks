"""The brief's acceptance criteria, and which notebook proves each one.

Every proving notebook records a metric `acceptance_<id>` with value PASS or FAIL in
ops.measurements. The maintenance job's `acceptance_checks` task reads the latest value
of each and fails the run if a required criterion is missing or failed.

Required criteria are produced by jobs in the orchestrated run. The others come from
the manual jobs (evolution A6/A7, audit A11) and are reported when present.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Criterion:
    id: str
    title: str
    proved_by: str          # notebook step name, as recorded in ops.measurements.step
    required: bool          # produced by the orchestrated run?


CRITERIA: tuple[Criterion, ...] = (
    Criterion("A1", "Two independent feeds built", "harness_price_catalog", True),
    Criterion("A2", ">= 200 products with a > 15% price change", "harness_price_catalog", True),
    Criterion("A3", "Own 30-minute sessions; disagreement with user_session quantified", "sessionize", True),
    Criterion("A4", "Temporal join: >= 2 optimisations implemented and compared", "temporal_join", True),
    Criterion("A5", "price_match rate reported and explained", "temporal_join", True),
    Criterion("A6", "Schema evolution: no data files rewritten", "evolution", False),
    Criterion("A7", "Clustering (partition) evolution: zero files rewritten", "evolution", False),
    Criterion("A8", "Compaction: before/after measured, clustering applied", "optimize", True),
    Criterion("A9", "Q1: windows normalised, floor stated, confounders named", "q1_elasticity", True),
    Criterion("A10", "Q2: both revenue definitions reported, with a recommendation", "q2_abandonment", True),
    Criterion("A11", "Q3: frozen copy, 4-step immutability proof, retention verified",
              "retention_check", False),
    Criterion("A12", "Serving: stats, materialized view, three-way comparison", "serving", True),
)
