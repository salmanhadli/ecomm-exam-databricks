# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · Hourly conversion funnel
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_40_gold` → `conversion_hourly` |
# MAGIC | **Reads** | `silver.clickstream` |
# MAGIC | **Writes** | `gold.product_conversion_hourly` (row tracking on, for the step 9 materialized view) |
# MAGIC | **Brief** | Step 6a · evidence item 5 (category share) · review question 11 |
# MAGIC | **Databricks concepts** | adaptive query execution (AQE) on serverless · salting a skewed key · Delta row tracking |
# MAGIC
# MAGIC One row per **product, hour and catalog price**, with views, carts, purchases, three
# MAGIC conversion rates and revenue. Every rate is `0.0` when its denominator is zero, never
# MAGIC `NULL`: a NULL rate silently drops out of an average.
# MAGIC
# MAGIC **Skew.** The brief warns that one `category_code` dominates, and a `groupBy` on it becomes one
# MAGIC enormous task. In this dataset the dominant "category" is `NULL` (98% of rows). The notebook
# MAGIC measures that skew, then compares a plain aggregation with a salted one.

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
from ecomm.settings import SETTINGS  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY, SILVER_CLICKSTREAM  # noqa: E402
from ecomm.transforms.gold import conversion_hourly  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="conversion_hourly")

# COMMAND ----------

# MAGIC %md ## 2 · Build and write

# COMMAND ----------

clicks = spark.read.table(SILVER_CLICKSTREAM.fqn(project))
funnel = conversion_hourly(clicks).withColumn("computed_at", F.current_timestamp())
GOLD_CONVERSION_HOURLY.overwrite(spark, project, funnel)

# COMMAND ----------

# MAGIC %md ## 3 · Measure

# COMMAND ----------

gold = spark.read.table(GOLD_CONVERSION_HOURLY.fqn(project))
totals = gold.agg(
    F.count("*").alias("rows"),
    F.sum("views").alias("views"), F.sum("carts").alias("carts"),
    F.sum("purchases").alias("purchases"), F.sum("revenue").alias("revenue"),
    F.sum(F.when(F.col("view_to_cart").isNull() | F.col("cart_to_purchase").isNull()
                 | F.col("overall_conv").isNull(), 1).otherwise(0)).alias("null_rates"),
).first()

for metric in ("rows", "views", "carts", "purchases"):
    ev.record(f"gold_conversion_{metric}", totals[metric])
ev.record("gold_conversion_revenue", str(totals["revenue"]))
ev.record("gold_conversion_null_rates", totals["null_rates"], "must be 0: rates are 0.0, never NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Skew: the category rollup
# MAGIC
# MAGIC Three measurements:
# MAGIC 1. **Share.** How much of the data the largest category holds.
# MAGIC 2. **Task skew.** Rows per shuffle partition when hashed on the key. A `groupBy` sends each
# MAGIC    partition to one task, and every NULL hashes to the same partition.
# MAGIC 3. **Plain vs salted.** The salted version spreads the hot key over N sub-keys
# MAGIC    (`product_id` hashed into 16 buckets), aggregates, then re-aggregates the partial results.
# MAGIC
# MAGIC AQE is always on for serverless and can split skewed *join* partitions. An aggregation
# MAGIC already pre-aggregates on the map side, so whether salting still pays off is measured here,
# MAGIC not assumed.

# COMMAND ----------

try:
    aqe = spark.conf.get("spark.sql.adaptive.enabled")
except Exception:
    aqe = "not readable (always on for serverless)"
ev.record("aqe_enabled", aqe)

total_views = totals["views"]
for key in ("category_code", "category_id"):
    top = gold.groupBy(key).agg(F.sum("views").alias("v")).orderBy(F.desc("v")).first()
    label = top[key] if top[key] is not None else "NULL"
    ev.record(f"largest_{key}_share_of_views_pct", round(100 * top["v"] / total_views, 2), f"{key} = {label}")

skew = perf.partition_skew(gold, "category_code")
ev.record("category_code_partition_skew_ratio", skew["ratio"],
          f"max {skew['max_rows']:,} vs median {skew['median_rows']:,} rows per task")

plain = gold.groupBy("category_code").agg(F.sum("views").alias("views"), F.sum("purchases").alias("purchases"))
salted = (gold
          .withColumn("_salt", F.pmod(F.hash("product_id"), F.lit(SETTINGS.salt_buckets)))
          .groupBy("category_code", "_salt").agg(F.sum("views").alias("v"), F.sum("purchases").alias("p"))
          .groupBy("category_code").agg(F.sum("v").alias("views"), F.sum("p").alias("purchases")))

ev.record("rollup_plain_seconds", round(perf.time_full_compute(plain), 2))
ev.record("rollup_salted_seconds", round(perf.time_full_compute(salted), 2),
          f"{SETTINGS.salt_buckets} salt buckets")
ev.record("rollup_results_identical",
          str(plain.exceptAll(salted).count() == 0 and salted.exceptAll(plain).count() == 0))
ev.flush()

if totals["null_rates"]:
    raise AssertionError(f"{totals['null_rates']:,} rows have a NULL rate; safe_rate must return 0.0")
