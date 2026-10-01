# Databricks notebook source
# MAGIC %md
# MAGIC # Audit · Retention: clean-up must not delete what the audit needs (Q3)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_60_audit` → `retention_check` (after `immutability_proof`) |
# MAGIC | **Reads** | the live gold table, the frozen copy, the predictive optimization history |
# MAGIC | **Brief** | Step 7c (retention policy) · the "trap that fails this audit silently" · acceptance **A11** · evidence item 19 |
# MAGIC | **Databricks concepts** | `VACUUM … DRY RUN` · predictive optimization · time-travel horizon |
# MAGIC
# MAGIC **The trap, on Delta.** Iceberg's version is a tag without `RETAIN`, which `expire_snapshots`
# MAGIC removes. Delta's is quieter: predictive optimization runs `VACUUM` on managed tables by itself,
# MAGIC and with the default 7-day retention it deletes the files older versions need. Time travel to
# MAGIC the Black Friday version then fails, months later, in front of an auditor.
# MAGIC
# MAGIC **The proof here, without deleting anything (decision D-09):**
# MAGIC 1. Both tables carry the 365-day policy.
# MAGIC 2. `VACUUM … DRY RUN` lists what a clean-up is *allowed* to delete right now. The DML and
# MAGIC    RESTORE just replaced files, yet none of them may be deleted inside the retention window.
# MAGIC 3. The frozen copy and time travel to the frozen version still read correctly.
# MAGIC 4. Predictive optimization's own run history, where the system table is accessible.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ecomm import audit, history  # noqa: E402
from ecomm.evidence import EVIDENCE_TABLE  # noqa: E402
from ecomm.project import audit_table_name  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="retention_check")
dbutils.widgets.text("audit_name", "black_friday_2026_final")

live = GOLD_CONVERSION_HOURLY.fqn(project)
frozen = project.table("audit", audit_table_name(dbutils.widgets.get("audit_name")))
props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {frozen}").collect()}
frozen_version = int(props[audit.PROP_VERSION])

# COMMAND ----------

# MAGIC %md ## 2 · The policy is in place on both tables

# COMMAND ----------

problems = history.violations(spark, live, "gold") + history.violations(spark, frozen, "audit")
ev.record("retention_policy", "PASS" if not problems else "FAIL", "; ".join(problems) or "365 days on both")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · What a clean-up could delete right now
# MAGIC
# MAGIC `DRY RUN` lists the files `VACUUM` would remove and removes none. Only files unreferenced for
# MAGIC longer than `delta.deletedFileRetentionDuration` qualify.

# COMMAND ----------

deletable = spark.sql(f"VACUUM {live} DRY RUN").count()
ev.record("vacuum_dry_run_deletable_files", deletable, "files a VACUUM could remove now; 0 within 365 days")

oldest = spark.sql(f"DESCRIBE HISTORY {live}").agg(
    F.min("version").alias("v"), F.date_format(F.min("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("t")).first()
ev.record("time_travel_oldest_version", oldest["v"], f"committed {oldest['t']} UTC")

# COMMAND ----------

# MAGIC %md ## 4 · The audit still reads

# COMMAND ----------

category = audit.top_category(spark.read.table(frozen))
frozen_now = audit.reading(spark.read.table(frozen), category)
travel_now = audit.reading(spark.sql(f"SELECT * FROM {live} VERSION AS OF {frozen_version}"), category)
reads_ok = frozen_now == travel_now
ev.record("frozen_and_time_travel_agree", str(reads_ok))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Predictive optimization's own record
# MAGIC
# MAGIC `system.storage.predictive_optimization_operations_history` logs every OPTIMIZE, VACUUM and
# MAGIC ANALYZE it ran. Free Edition may not expose system tables; if so, that is recorded, not hidden.

# COMMAND ----------

try:
    operations = (spark.table("system.storage.predictive_optimization_operations_history")
                  .where(F.col("catalog_name") == project.catalog)
                  .groupBy("operation_type", "operation_status").count().collect())
    for row in operations:
        ev.record(f"predictive_optimization.{row['operation_type']}.{row['operation_status']}", row["count"])
    if not operations:
        ev.record("predictive_optimization_operations", 0, "none logged yet for this catalog")
except Exception as err:
    ev.record("predictive_optimization_operations", "unavailable", str(err)[:160])

# COMMAND ----------

# MAGIC %md ## 6 · A11

# COMMAND ----------

newest = Window.orderBy(F.desc("recorded_at"))
proof_row = (spark.read.table(project.table(*EVIDENCE_TABLE))
             .where(F.col("metric") == "q3_immutability_proof")
             .withColumn("_rank", F.row_number().over(newest)).where("_rank = 1").first())
proof_ok = proof_row is not None and proof_row["value"] == "PASS"

a11 = proof_ok and not problems and deletable == 0 and reads_ok
ev.record("acceptance_A11", "PASS" if a11 else "FAIL",
          "frozen copy + 4-step proof + 365-day retention + nothing deletable + audit still reads")
ev.flush()

if not a11:
    raise AssertionError("Q3 audit is not safe: see the evidence above")
