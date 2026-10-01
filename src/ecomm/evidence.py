"""The evidence pack as a table.

The brief ends with an evidence pack: numbers that can only come from your own
workspace. Instead of copying them out of job logs, every notebook records what it
measured through `Evidence`, which prints each value and appends it to
`ops.measurements`. One query then reproduces the whole pack:

    SELECT step, metric, value, note
    FROM <catalog>.ops.measurements
    WHERE orchestration_run_id = '<run id>'
    ORDER BY recorded_at
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from ecomm.history import tblproperties_sql
from ecomm.project import Project

EVIDENCE_TABLE = ("ops", "measurements")

_COLUMNS = ("orchestration_run_id string, run_id string, step string, metric string, "
            "value string, value_num double, note string, recorded_at timestamp")


def evidence_ddl(project: Project) -> str:
    """CREATE TABLE statement for ops.measurements (idempotent)."""
    return f"""
        CREATE TABLE IF NOT EXISTS {project.table(*EVIDENCE_TABLE)} (
            orchestration_run_id STRING COMMENT 'ecomm_99_orchestrator run id; NULL for a standalone job run',
            run_id      STRING    COMMENT 'Run id of the job that ran the notebook, or "manual"',
            step        STRING    COMMENT 'Notebook that recorded the value',
            metric      STRING,
            value       STRING    COMMENT 'Value as displayed',
            value_num   DOUBLE    COMMENT 'Numeric value, when the metric is a number',
            note        STRING,
            recorded_at TIMESTAMP
        )
        COMMENT 'Evidence pack: one row per measurement, appended by every notebook'
        {tblproperties_sql("ops")}
    """


@dataclass
class Evidence:
    """Collects measurements for one notebook run and appends them in one write."""

    spark: object
    project: Project
    step: str
    run_id: str = "manual"
    orchestration_run_id: str | None = None
    _rows: list = field(default_factory=list)

    def record(self, metric: str, value, note: str | None = None):
        """Print a measurement and queue it for ops.measurements. Returns `value` unchanged."""
        is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
        shown = f"{value:,}" if isinstance(value, int) and is_number else str(value)
        self._rows.append((self.orchestration_run_id, self.run_id, self.step, metric, shown,
                           float(value) if is_number else None, note,
                           dt.datetime.now(dt.UTC).replace(tzinfo=None)))
        print(f"{metric:<44} {shown}" + (f"   ({note})" if note else ""))
        return value

    def flush(self) -> None:
        """Append everything recorded so far. Call once at the end of the notebook."""
        if not self._rows:
            return
        table = self.project.table(*EVIDENCE_TABLE)
        self.spark.createDataFrame(self._rows, _COLUMNS).write.mode("append").saveAsTable(table)
        print(f"\n{len(self._rows)} measurement(s) appended to {table}")
        self._rows.clear()
