# Databricks notebook source
# MAGIC %md
# MAGIC # Harness · Stream B — price catalog feed
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_10_harness` → `harness_price_catalog` |
# MAGIC | **Reads** | `/Volumes/<catalog>/raw/landing/clickstream/` (Stream A, as landed) |
# MAGIC | **Writes** | `/Volumes/<catalog>/raw/landing/price_catalog/` — price validity intervals, Parquet |
# MAGIC | **Brief** | Stream B · acceptance A1, **A2** · evidence item 4 |
# MAGIC | **Databricks concepts** | window functions on serverless · `min_by` · DECIMAL money |
# MAGIC
# MAGIC The export carries a price *per event*, but a real platform has a price catalog that changes
# MAGIC on its own schedule. This reconstructs it — each product's event prices collapsed into
# MAGIC validity intervals:
# MAGIC
# MAGIC `product_id, price, effective_start, effective_end, change_reason, pct_change`
# MAGIC
# MAGIC It is derived from the **landed** clickstream, so it covers exactly the replayed window.
# MAGIC
# MAGIC | Rule | Decision |
# MAGIC |---|---|
# MAGIC | Noise | a one-cent step is rounding, not a repricing. Compared in **integer cents**: in DOUBLE, 43% of one-cent steps compare as `> 0.01` |
# MAGIC | Dirt | a price `<= 0` or NULL is not a catalog price: excluded and counted |
# MAGIC | Ties | both windows carry a secondary sort key, so equal timestamps resolve the same way on every run |
# MAGIC | Open end | the newest interval keeps `effective_end = NULL`; each consumer closes it for its own purpose |
# MAGIC | A2 | at least 200 products must change by more than 15%, or this task fails |
# MAGIC
# MAGIC The transformation itself is `ecomm.transforms.harness.derive_price_intervals`.

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
from ecomm.schemas import EVENT_SCHEMA  # noqa: E402
from ecomm.settings import SETTINGS  # noqa: E402
from ecomm.transforms.harness import (  # noqa: E402
    derive_price_intervals,
    priced_events,
    products_with_significant_change,
)

project, ev = bootstrap(spark, dbutils, step="harness_price_catalog")
dbutils.widgets.dropdown("replay_mode", "skip_if_present", ["skip_if_present", "overwrite"])

target = project.price_catalog_dir
already_landed = os.path.isdir(target) and any(n.endswith(".parquet") for n in os.listdir(target))

# COMMAND ----------

# MAGIC %md ## 2 · Derive the intervals

# COMMAND ----------

if already_landed and dbutils.widgets.get("replay_mode") == "skip_if_present":
    ev.record("derive", "skipped", f"{target} already populated; replay_mode=overwrite forces a rebuild")
else:
    clicks = spark.read.schema(EVENT_SCHEMA).json(project.clickstream_dir)

    events_read = ev.record("events_read", clicks.count())
    ev.record("events_excluded_price_le_0_or_null", events_read - priced_events(clicks).count())

    intervals = derive_price_intervals(clicks, SETTINGS.noise_threshold_cents,
                                       SETTINGS.decimal_precision, SETTINGS.decimal_scale)

    # ~40k rows: one file is the right size, and it keeps the feed easy to inspect.
    intervals.coalesce(1).write.mode("overwrite").parquet(target)
    ev.record("derive", "written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Measure the feed that landed (acceptance A2)
# MAGIC
# MAGIC Measured from the Parquet that actually landed, not from the in-memory plan, so the numbers
# MAGIC describe exactly what bronze will ingest.

# COMMAND ----------

feed = spark.read.parquet(target)

ev.record("price_intervals", feed.count())
ev.record("products", feed.select("product_id").distinct().count())
ev.record("open_ended_intervals", feed.where(F.col("effective_end").isNull()).count())
for row in feed.groupBy("change_reason").count().orderBy("change_reason").collect():
    ev.record(f"change_reason.{row['change_reason']}", row["count"])

significant = products_with_significant_change(feed, SETTINGS.significant_change_pct)
required = SETTINGS.min_products_with_significant_change
ev.record("products_with_change_gt_15pct", significant, f"A2 requires >= {required}")
ev.record("acceptance_A2", "PASS" if significant >= required else "FAIL")

# A1: two independent feeds, each landed in its own folder of the landing volume.
clickstream_files = (spark.read.schema(EVENT_SCHEMA).json(project.clickstream_dir)
                     .select("_metadata.file_path").distinct().count())
ev.record("acceptance_A1", "PASS" if clickstream_files and feed.count() else "FAIL",
          f"clickstream: {clickstream_files:,} micro-batch files; price catalog: its own Parquet feed")
ev.flush()

if significant < required:
    raise AssertionError(f"A2 failed: {significant} products changed by more than "
                         f"{SETTINGS.significant_change_pct:.0%}, need {required}")
