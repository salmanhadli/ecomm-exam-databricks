# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Price intervals
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_30_silver` → `price_intervals` |
# MAGIC | **Reads** | `bronze.price_catalog` |
# MAGIC | **Writes** | `silver.price_intervals` |
# MAGIC | **Brief** | Step 4 input (`silver_price_intervals`) · evidence item 4 |
# MAGIC | **Databricks concepts** | managed Delta table with liquid clustering · `DESCRIBE DETAIL` for table size |
# MAGIC
# MAGIC Silver's view of the price catalog:
# MAGIC - one row per `(product_id, effective_start)`, the latest ingested version
# MAGIC - money as `DECIMAL(12,2)`
# MAGIC - clustered by `product_id`, the key every temporal join uses
# MAGIC
# MAGIC Its size decides the join strategy in the next task: a small interval table can be broadcast.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.tables import SILVER_PRICE_INTERVALS  # noqa: E402
from ecomm.transforms.silver import latest_intervals  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="price_intervals")

# COMMAND ----------

# MAGIC %md ## 2 · Build and write

# COMMAND ----------

bronze = spark.read.table(project.table("bronze", "price_catalog"))
intervals = latest_intervals(bronze).withColumn("_updated_at", F.current_timestamp())
SILVER_PRICE_INTERVALS.overwrite(spark, project, intervals)

# COMMAND ----------

# MAGIC %md ## 3 · Measure

# COMMAND ----------

table = SILVER_PRICE_INTERVALS.fqn(project)
silver = spark.read.table(table)
detail = spark.sql(f"DESCRIBE DETAIL {table}").first()

ev.record("bronze_price_catalog_rows", bronze.count())
ev.record("price_intervals", silver.count())
ev.record("products", silver.select("product_id").distinct().count())
ev.record("open_ended_intervals", silver.where(F.col("effective_end").isNull()).count())
ev.record("price_intervals_bytes", detail["sizeInBytes"], "decides whether the join can broadcast")
ev.record("price_intervals_files", detail["numFiles"])
ev.flush()
