# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance · OPTIMIZE: compaction and clustering, measured
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_50_maintenance` → `optimize` |
# MAGIC | **Changes** | the silver and gold tables: file layout only, never the data |
# MAGIC | **Brief** | Step 7a (compaction) · acceptance **A8** · evidence item 14 |
# MAGIC | **Databricks concepts** | `OPTIMIZE` on liquid-clustered tables · data skipping · `DESCRIBE DETAIL` · predictive optimization |
# MAGIC
# MAGIC `OPTIMIZE` rewrites small files into larger ones and clusters rows by the table's
# MAGIC clustering keys, so a query filtering on those keys reads fewer files. On liquid-clustered
# MAGIC tables it is **incremental**: it only processes files not yet clustered.
# MAGIC
# MAGIC **What to expect honestly.** Unity Catalog managed tables already get automatic file-size
# MAGIC tuning and auto compaction, and predictive optimization may have run OPTIMIZE by itself.
# MAGIC So this can find little to do. The numbers below say how much it actually did.
# MAGIC
# MAGIC **Layout effect, measured.** A probe query reads one day of `silver.clickstream`. The number
# MAGIC of files holding its rows is the least any query for that day must open, and clustering by
# MAGIC `event_time` is what shrinks it.
# MAGIC
# MAGIC No `VACUUM` here, by design (decision D-09). Files replaced by OPTIMIZE stay readable for
# MAGIC time travel until the retention window passes, and predictive optimization removes them after.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm import perf  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.tables import (  # noqa: E402
    GOLD_CONVERSION_HOURLY,
    SILVER_CLICKSTREAM,
    SILVER_PRICE_INTERVALS,
    SILVER_SESSIONS,
)

project, ev = bootstrap(spark, dbutils, step="optimize")

TABLES = [SILVER_CLICKSTREAM, SILVER_SESSIONS, SILVER_PRICE_INTERVALS, GOLD_CONVERSION_HOURLY]
probe_table = SILVER_CLICKSTREAM.fqn(project)

# The probe: one full day (the third of the replay), the kind of time-bounded scan Q1's
# windows make. The day is computed inside Spark, so no timestamp passes through Python.
probe_day = spark.read.table(probe_table).agg(
    (F.date_trunc("day", F.min("event_time")) + F.expr("INTERVAL 2 DAYS")).alias("day_start"))


def layout(table: str) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    files = max(detail["numFiles"], 1)
    return {"files": detail["numFiles"], "avg_bytes": int(detail["sizeInBytes"] / files),
            "clustering": ", ".join(detail["clusteringColumns"] or [])}


def probe() -> dict:
    rows = (spark.read.table(probe_table).crossJoin(F.broadcast(probe_day))
            .where((F.col("event_time") >= F.col("day_start"))
                   & (F.col("event_time") < F.col("day_start") + F.expr("INTERVAL 1 DAY")))
            .drop("day_start"))
    return {"files_holding_rows": rows.select("_metadata.file_path").distinct().count(),
            "seconds": round(perf.time_full_compute(rows.groupBy("product_id", "event_type").count()), 2)}

# COMMAND ----------

# MAGIC %md ## 2 · Before

# COMMAND ----------

before = {spec.name: layout(spec.fqn(project)) for spec in TABLES}
probe_before = probe()

# COMMAND ----------

# MAGIC %md ## 3 · OPTIMIZE

# COMMAND ----------

for spec in TABLES:
    spark.sql(f"OPTIMIZE {spec.fqn(project)}")

# COMMAND ----------

# MAGIC %md ## 4 · After, and the evidence (A8)

# COMMAND ----------

after = {spec.name: layout(spec.fqn(project)) for spec in TABLES}
probe_after = probe()

for spec in TABLES:
    b, a = before[spec.name], after[spec.name]
    ev.record(f"{spec.layer}.{spec.name}.files", f"{b['files']:,} -> {a['files']:,}",
              f"avg {b['avg_bytes']:,} -> {a['avg_bytes']:,} bytes; clustered by {a['clustering']}")

ev.record("probe_day", probe_day.select(F.date_format("day_start", "yyyy-MM-dd")).first()[0])
ev.record("probe_files_holding_rows_before", probe_before["files_holding_rows"])
ev.record("probe_files_holding_rows_after", probe_after["files_holding_rows"])
ev.record("probe_seconds_before", probe_before["seconds"])
ev.record("probe_seconds_after", probe_after["seconds"], "serverless timings vary; compare files first")

silver_clustered = after[SILVER_CLICKSTREAM.name]["clustering"] != ""
ev.record("acceptance_A8", "PASS" if silver_clustered else "FAIL",
          "OPTIMIZE ran on clustered tables; files and probe measured before and after")
ev.flush()
