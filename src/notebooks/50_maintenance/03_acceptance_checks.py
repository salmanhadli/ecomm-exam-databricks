# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance · Acceptance checks
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_50_maintenance` → `acceptance_checks` (the last task of every orchestrated run) |
# MAGIC | **Reads** | `ops.measurements` |
# MAGIC | **Brief** | the acceptance criteria table (A1–A12) |
# MAGIC
# MAGIC Each criterion is proved by one notebook, which records `acceptance_<id>` as PASS or FAIL
# MAGIC (`ecomm.acceptance` maps which). This task takes the latest recorded value of each criterion
# MAGIC and fails the run if a **required** one is missing or failed.
# MAGIC
# MAGIC Criteria proved by the manual jobs (evolution A6/A7, audit A11) are reported when
# MAGIC present, and never fail the orchestrated run.

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ecomm.acceptance import CRITERIA  # noqa: E402
from ecomm.evidence import EVIDENCE_TABLE  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="acceptance_checks")

# COMMAND ----------

newest_first = Window.partitionBy("metric").orderBy(F.desc("recorded_at"))
latest = {
    row["metric"]: row
    for row in (spark.read.table(project.table(*EVIDENCE_TABLE))
                .where(F.col("metric").startswith("acceptance_"))
                .withColumn("_rank", F.row_number().over(newest_first))
                .where("_rank = 1")
                .collect())
}

summary, failures = [], []
for criterion in CRITERIA:
    row = latest.get(f"acceptance_{criterion.id}")
    status = row["value"] if row else "NOT RUN"
    summary.append((criterion.id, criterion.title, status, criterion.proved_by,
                    "required" if criterion.required else "manual job",
                    str(row["recorded_at"]) if row else None))
    if criterion.required and status != "PASS":
        failures.append(f"{criterion.id} {criterion.title}: {status}")

display(spark.createDataFrame(summary, "id string, criterion string, status string, proved_by string, "
                                       "kind string, recorded_at string"))

passed = sum(1 for s in summary if s[2] == "PASS")
ev.record("acceptance_passed", passed, f"of {len(CRITERIA)} criteria")
ev.record("acceptance_required_failures", len(failures), "; ".join(failures) or None)
ev.flush()

if failures:
    raise AssertionError("required acceptance criteria not met:\n" + "\n".join(failures))
