# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance · History guard
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_50_maintenance` → `history_guard` |
# MAGIC | **Reads** | the properties of every table in `bronze`, `silver`, `gold`, `audit`, `ops` |
# MAGIC | **Brief** | Q3's retention requirement: *"a maintenance policy that will not delete the data underneath"* |
# MAGIC | **Databricks concepts** | `SHOW TBLPROPERTIES` · Delta log and file retention · predictive optimization's VACUUM |
# MAGIC
# MAGIC Predictive optimization runs `VACUUM` on managed tables by itself. It keeps as much history
# MAGIC as each table's `delta.deletedFileRetentionDuration` says, and at least 7 days. So a table's
# MAGIC time-travel window is exactly as long as its retention properties.
# MAGIC
# MAGIC This task checks every table against the policy in `ecomm.history` (decision D-09) and **fails
# MAGIC the job** if any table keeps less history than required, for example after someone shortened it
# MAGIC by hand. The source-level guard (`tests/unit/test_history_policy.py`) stops the same mistake
# MAGIC in code; this one catches it in the workspace.

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from ecomm import history  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="history_guard")

# COMMAND ----------

checked, skipped, problems = 0, [], []
for schema in history.RETENTION_DAYS:
    for row in spark.sql(f"SHOW TABLES IN {project.schema(schema)}").collect():
        if row["isTemporary"]:
            continue
        table = project.table(schema, row["tableName"])
        try:
            is_delta = spark.sql(f"DESCRIBE DETAIL {table}").first()["format"] == "delta"
        except Exception:                    # e.g. a materialized view, managed by its own pipeline
            is_delta = False
        if not is_delta:
            skipped.append(table)
            continue
        checked += 1
        problems += history.violations(spark, table, schema)

ev.record("history_tables_checked", checked)
ev.record("history_objects_skipped", len(skipped), ", ".join(skipped) or None)
ev.record("history_policy", "PASS" if not problems else "FAIL", "; ".join(problems) or None)
ev.flush()

if problems:
    raise AssertionError("tables keep less history than the policy requires:\n" + "\n".join(problems))
