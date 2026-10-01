"""Unity Catalog naming: one catalog, one schema per medallion layer, one volume for files.

    <catalog>
    ├── raw      volume `landing`: source CSVs, clickstream micro-batches, price catalog feed
    ├── bronze   append-only tables written by the Lakeflow pipeline
    ├── silver   deduplicated, sessionized, price-bound events and price intervals
    ├── gold     business aggregates for Q1 and Q2
    ├── audit    frozen, immutable copies for the Q3 audit
    └── ops      the evidence pack (ops.measurements)

Every notebook builds names through `Project`, so the catalog is a single job
parameter and no SQL string is assembled by hand anywhere else.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

SCHEMAS: dict[str, str] = {
    "raw": "Landing volume: source CSVs, harness micro-batches, price catalog feed.",
    "bronze": "Append-only ingestion tables written by the Lakeflow pipeline.",
    "silver": "Deduplicated, sessionized, price-bound events and price intervals.",
    "gold": "Business aggregates for Q1 (elasticity) and Q2 (cart abandonment).",
    "audit": "Frozen, immutable copies for the Q3 Black Friday audit.",
    "ops": "Evidence pack: every measurement a notebook records.",
}

LANDING_VOLUME = "landing"

# Plain identifiers only: a hyphenated catalog would need backquotes in every SQL
# statement, and one forgotten pair fails at run time.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Project:
    """Names and paths for one deployment of the project, keyed by catalog."""

    catalog: str

    def __post_init__(self) -> None:
        if not _IDENTIFIER.match(self.catalog):
            raise ValueError(
                f"catalog {self.catalog!r} is not a plain identifier; use letters, digits "
                f"and underscores only (e.g. exam_ecommerce)")

    # --- tables -------------------------------------------------------------
    def schema(self, name: str) -> str:
        if name not in SCHEMAS:
            raise KeyError(f"unknown schema {name!r}; expected one of {sorted(SCHEMAS)}")
        return f"{self.catalog}.{name}"

    def table(self, schema: str, name: str) -> str:
        return f"{self.schema(schema)}.{name}"

    # --- files (Unity Catalog volume paths) -----------------------------------
    @property
    def landing(self) -> str:
        return f"/Volumes/{self.catalog}/raw/{LANDING_VOLUME}"

    @property
    def source_dir(self) -> str:
        """The five Kaggle CSVs."""
        return f"{self.landing}/source/cosmetics"

    @property
    def clickstream_dir(self) -> str:
        """Stream A: one JSON file per 5-minute interval under dt=/hh=/min5=."""
        return f"{self.landing}/clickstream"

    @property
    def price_catalog_dir(self) -> str:
        """Stream B: the derived price-interval feed, Parquet."""
        return f"{self.landing}/price_catalog"


def audit_table_name(audit_name: str) -> str:
    """Name of the frozen copy of gold.product_conversion_hourly for one audit, e.g.
    'black_friday_2026_final' -> 'product_conversion_hourly_black_friday_2026_final'."""
    if not _IDENTIFIER.match(audit_name):
        raise ValueError(f"audit name {audit_name!r}: use letters, digits and underscores only")
    return f"product_conversion_hourly_{audit_name}"
