# Databricks notebook source
# MAGIC %md
# MAGIC # Audit · Freeze the Black Friday state (Q3)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_60_audit` → `freeze` (manual job) |
# MAGIC | **Reads** | `gold.product_conversion_hourly` at a pinned version |
# MAGIC | **Writes** | `audit.product_conversion_hourly_<audit_name>`: a **DEEP CLONE**, created once and never replaced |
# MAGIC | **Brief** | Q3 · step 7b · acceptance **A11** · evidence item 17 |
# MAGIC | **Databricks concepts** | Delta time travel (`VERSION AS OF`) · `DEEP CLONE` · table properties and comments |
# MAGIC
# MAGIC > *Can the executive team query exact metrics as they stood at midnight on Black Friday, months
# MAGIC > later, while the underlying tables keep receiving updates and deletes?*
# MAGIC
# MAGIC The brief: this needs **a named, immutable reference** *and* **a maintenance policy that won't
# MAGIC delete the data underneath it**. On Delta (decision D-01):
# MAGIC
# MAGIC | Brief (Iceberg) | Here (Delta) |
# MAGIC |---|---|
# MAGIC | `CREATE TAG black_friday_2026_final` | `audit.product_conversion_hourly_black_friday_2026_final`: a named table |
# MAGIC | the tag points at one snapshot | `DEEP CLONE … VERSION AS OF n`: a full, independent copy of version *n* |
# MAGIC | `RETAIN 365 DAYS` on the tag | 365-day retention on both the copy and the live table (`ecomm.history`) |
# MAGIC
# MAGIC **Why deep, not shallow.** A shallow clone copies only metadata and points at the live table's
# MAGIC files. A deep clone copies the files, so nothing done to the live table (DML, OPTIMIZE, VACUUM)
# MAGIC can change or remove what the copy reads.
# MAGIC
# MAGIC **Immutability.** An existing frozen copy is **never** replaced. Re-running with the same
# MAGIC `audit_name` reports the copy that already exists and leaves it untouched.
# MAGIC
# MAGIC **Parameters**
# MAGIC - `audit_name`: default `black_friday_2026_final`.
# MAGIC - `as_of_timestamp`: freeze the last version committed at or before this UTC time,
# MAGIC   e.g. `2026-11-27 00:00:00`. Empty means the current version.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm import audit, history  # noqa: E402
from ecomm.project import audit_table_name  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="freeze")
dbutils.widgets.text("audit_name", "black_friday_2026_final")
dbutils.widgets.text("as_of_timestamp", "")

audit_name = dbutils.widgets.get("audit_name")
as_of = dbutils.widgets.get("as_of_timestamp").strip()
source = GOLD_CONVERSION_HOURLY.fqn(project)
frozen = project.table("audit", audit_table_name(audit_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Pick the version to freeze
# MAGIC
# MAGIC `DESCRIBE HISTORY` lists every committed version with its timestamp. "As they stood at
# MAGIC midnight" means the last version committed at or before that instant.

# COMMAND ----------

versions = spark.sql(f"DESCRIBE HISTORY {source}")
if as_of:
    versions = versions.where(F.col("timestamp") <= F.lit(as_of).cast("timestamp"))
pinned = (versions.orderBy(F.desc("version"))
          .select("version", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss.SSS").alias("committed_at"),
                  "operation")
          .first())
if pinned is None:
    raise ValueError(f"{source} has no version committed at or before {as_of!r}")

# COMMAND ----------

# MAGIC %md ## 3 · Freeze, or report the copy that already exists

# COMMAND ----------

if spark.catalog.tableExists(frozen):
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {frozen}").collect()}
    ev.record("frozen_table", frozen, "already existed: left untouched")
    ev.record("frozen_source_version", props.get(audit.PROP_VERSION))
    ev.record("frozen_source_committed_at_utc", props.get(audit.PROP_COMMITTED))
else:
    spark.sql(f"CREATE TABLE {frozen} DEEP CLONE {source} VERSION AS OF {pinned['version']}")
    spark.sql(history.apply_sql(frozen, "audit"))
    spark.sql(audit.provenance_sql(frozen, audit_name, source, pinned["version"], pinned["committed_at"]))
    spark.sql(f"COMMENT ON TABLE {frozen} IS 'Q3 audit {audit_name}: frozen copy of {source} "
              f"at version {pinned['version']} (committed {pinned['committed_at']} UTC). Never replaced.'")
    ev.record("frozen_table", frozen, "created")
    ev.record("frozen_source_version", pinned["version"])
    ev.record("frozen_source_committed_at_utc", pinned["committed_at"])

frozen_rows = spark.read.table(frozen).count()
ev.record("audit_name", audit_name)
ev.record("frozen_rows", frozen_rows)
ev.record("frozen_retention_days", history.RETENTION_DAYS["audit"])
ev.flush()
