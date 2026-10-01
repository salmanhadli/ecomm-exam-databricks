"""Table-history retention policy: how long every Delta table keeps its previous versions.

Why this module exists
----------------------
The AWS build expired history after every write (`expire_snapshots(retain_last=1)`,
plus metadata deleted on commit), so no table could time-travel. On Delta the same
habit would be `VACUUM ... RETAIN 0 HOURS` or a shortened retention property. This
project does the opposite, and enforces it:

1. Every table is created with the retention properties below — this module is the
   only place they are defined.
2. No code in this bundle runs VACUUM with a RETAIN clause, shortens retention, or
   touches `spark.databricks.delta.retentionDurationCheck.enabled`.
   `tests/unit/test_history_policy.py` scans the source and fails if one appears.
3. Clean-up is left to predictive optimization, which honours
   `delta.deletedFileRetentionDuration` when it runs VACUUM.
4. `violations()` lets a job check any table and fail if its history was shortened.

How Delta history works
-----------------------
Every write creates a new table version. Time travel to version N needs both:
- the transaction-log entries for N  -> kept for `delta.logRetentionDuration`
- the data files N referenced        -> VACUUM may delete them once they have been
                                        unreferenced for `delta.deletedFileRetentionDuration`
Both are set to the same horizon, so the whole history window stays readable.
"""

from __future__ import annotations

# Days of time travel per schema. Gold and audit hold the Q3 numbers an auditor may ask
# for "months later"; the other layers can be rebuilt from the landing volume, so 30
# days (Delta's default log retention) is enough to debug and re-process.
RETENTION_DAYS: dict[str, int] = {
    "bronze": 30,
    "silver": 30,
    "gold": 365,
    "audit": 365,
    "ops": 365,
}

_LOG = "delta.logRetentionDuration"
_FILES = "delta.deletedFileRetentionDuration"


def table_properties(schema: str) -> dict[str, str]:
    """Retention properties every table in `schema` is created with."""
    interval = f"interval {RETENTION_DAYS[schema]} days"
    return {_LOG: interval, _FILES: interval}


def tblproperties_sql(schema: str, extra: dict[str, str] | None = None) -> str:
    """The retention properties (plus any `extra` ones) as a `TBLPROPERTIES (...)` clause."""
    properties = {**table_properties(schema), **(extra or {})}
    pairs = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    return f"TBLPROPERTIES ({pairs})"


def apply_sql(table: str, schema: str) -> str:
    """ALTER TABLE statement that sets the policy on an existing table (e.g. a new clone)."""
    return f"ALTER TABLE {table} SET {tblproperties_sql(schema)}"


def _days(interval: str | None) -> float:
    """Parse Delta's 'interval N weeks|days|hours' into days.

    A missing property counts as 0: the policy requires the property to be set
    explicitly, not to rely on Delta's defaults (7 days for deleted files).
    """
    if not interval:
        return 0.0
    tokens = interval.lower().replace("interval", "").split()
    value, unit = float(tokens[0]), tokens[1]
    per_unit = {"week": 7.0, "day": 1.0, "hour": 1 / 24}
    return next((value * f for u, f in per_unit.items() if unit.startswith(u)), 0.0)


def violations(spark, table: str, schema: str) -> list[str]:
    """Return why `table` keeps less history than the policy requires (empty list = compliant)."""
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    required = RETENTION_DAYS[schema]
    problems = []
    for key in (_LOG, _FILES):
        have = _days(props.get(key))
        if have < required:
            problems.append(f"{table}: {key} = {props.get(key, 'default')} (< {required} days)")
    return problems
