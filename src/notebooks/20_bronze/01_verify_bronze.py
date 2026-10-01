# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · Verify ingestion and measure the known dirt
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_20_bronze` → `verify_bronze` (runs after the pipeline update) |
# MAGIC | **Reads** | `bronze.clickstream` · `bronze.price_catalog` · the pipeline event log |
# MAGIC | **Brief** | Step 1 (bronze, small-file baseline) · known-dirt table · evidence items 1, 5 |
# MAGIC | **Databricks concepts** | Lakeflow expectations and the event log · `DESCRIBE DETAIL` · `_metadata` on Delta tables |
# MAGIC
# MAGIC The pipeline has already ingested the micro-batches. This notebook:
# MAGIC
# MAGIC 1. **Completeness:** every landed file and every event reached bronze.
# MAGIC 2. **Small files:** how ~4,000 small input files compare with the Delta files bronze wrote.
# MAGIC 3. **Known dirt:** measures the null rates and category skew the brief asks you to report.
# MAGIC 4. **Expectations:** reads each rule's pass/fail counts from the pipeline event log.
# MAGIC 5. **History:** confirms bronze keeps its time-travel history (retention policy, D-09).

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm import history  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="verify_bronze")

clicks_table = project.table("bronze", "clickstream")
prices_table = project.table("bronze", "price_catalog")
clicks = spark.read.table(clicks_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Completeness
# MAGIC
# MAGIC Each micro-batch file should appear exactly once as a `_src_file`, and the row count should
# MAGIC match what the harness landed (recorded by `harness_clickstream` as `events_landed`).

# COMMAND ----------

completeness = clicks.agg(
    F.count("*").alias("rows"),
    F.countDistinct("_src_file").alias("files"),
    F.countDistinct("_batch_id").alias("batches"),
).first()

ev.record("bronze_clickstream_rows", completeness["rows"])
ev.record("bronze_source_files_ingested", completeness["files"])
ev.record("bronze_micro_batches_ingested", completeness["batches"])
ev.record("bronze_price_catalog_rows", spark.read.table(prices_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Small files: input vs table
# MAGIC
# MAGIC The brief's step 1 baseline: file count and file sizes. `DESCRIBE DETAIL` reports the files
# MAGIC in the current version. Reading `_metadata.file_size` also gives the smallest file, the
# MAGIC equivalent of Iceberg's `.files` metadata table.

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {clicks_table}").first()
file_sizes = (clicks.select(F.col("_metadata.file_path").alias("path"),
                            F.col("_metadata.file_size").alias("bytes"))
              .distinct()
              .agg(F.count("*").alias("files"), F.avg("bytes").alias("avg"), F.min("bytes").alias("min"))
              .first())

ev.record("bronze_table_files", detail["numFiles"], f"vs {completeness['files']:,} input files")
ev.record("bronze_table_bytes", detail["sizeInBytes"])
ev.record("bronze_table_avg_file_bytes", int(file_sizes["avg"]))
ev.record("bronze_table_min_file_bytes", int(file_sizes["min"]))
ev.record("bronze_clustering_columns", ", ".join(detail["clusteringColumns"] or []) or "none")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Known dirt
# MAGIC
# MAGIC The brief's table of known problems, measured. Nothing is dropped in bronze (D-15). These
# MAGIC numbers drive the decisions silver and gold make.

# COMMAND ----------

def pct(condition) -> F.Column:
    return F.round(100 * F.avg(F.when(condition, 1).otherwise(0)), 2)


dirt = clicks.agg(
    pct(F.col("category_code").isNull()).alias("category_code_null_pct"),
    pct(F.col("brand").isNull()).alias("brand_null_pct"),
    pct(F.col("user_session").isNull()).alias("user_session_null_pct"),
    F.sum(F.when(F.col("price").isNull() | (F.col("price") <= 0), 1).otherwise(0)).alias("price_le_0_or_null"),
    F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_rows"),
).first()

for metric, value in dirt.asDict().items():
    ev.record(metric, value)

# Category skew: this is what becomes a shuffle problem in the gold aggregations.
total = completeness["rows"]
for column in ("category_code", "category_id"):
    top = (clicks.groupBy(column).count().orderBy(F.desc("count")).first())
    label = top[column] if top[column] is not None else "NULL"
    ev.record(f"largest_{column}_share_pct", round(100 * top["count"] / total, 2), f"{column} = {label}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Expectation results from the pipeline event log
# MAGIC
# MAGIC Every pipeline update logs a `flow_progress` event with the pass/fail counts of each
# MAGIC expectation. `event_log(TABLE(...))` exposes that log as a table. The counts add up over
# MAGIC updates, because each update processes only rows it has not seen before.

# COMMAND ----------

EXPECTATIONS = "array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>"

try:
    expectation_results = spark.sql(f"""
        SELECT e.dataset, e.name,
               sum(e.passed_records) AS passed,
               sum(e.failed_records) AS failed
        FROM (
            SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                                     '{EXPECTATIONS}')) AS e
            FROM event_log(TABLE({clicks_table}))
            WHERE event_type = 'flow_progress'
        )
        GROUP BY e.dataset, e.name
        ORDER BY e.dataset, e.name
    """).collect()
    for row in expectation_results:
        checked = row["passed"] + row["failed"]
        failed_pct = round(100 * row["failed"] / checked, 2) if checked else 0.0
        ev.record(f"expectation.{row['dataset']}.{row['name']}", failed_pct,
                  f"% failed: {row['failed']:,} of {checked:,}")
except Exception as err:  # the event log needs pipeline-owner access; report and carry on
    ev.record("expectation_results", "unavailable", str(err)[:200])

# COMMAND ----------

# MAGIC %md ## 6 · History retention and evidence

# COMMAND ----------

problems = [p for t in (clicks_table, prices_table) for p in history.violations(spark, t, "bronze")]
ev.record("bronze_history_policy", "PASS" if not problems else "FAIL", "; ".join(problems) or None)
ev.flush()

if problems:
    raise AssertionError("bronze tables keep less history than the policy requires:\n" + "\n".join(problems))
