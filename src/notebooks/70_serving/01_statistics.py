# Databricks notebook source
# MAGIC %md
# MAGIC # Serving · Table statistics for the optimizer
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_70_serving` → `statistics` |
# MAGIC | **Changes** | column statistics on the three gold tables (`ANALYZE TABLE`) |
# MAGIC | **Brief** | Step 8 (column statistics, plan before and after) · evidence item 20 |
# MAGIC | **Databricks concepts** | `ANALYZE TABLE … COMPUTE STATISTICS FOR ALL COLUMNS` · `EXPLAIN COST` · predictive optimization |
# MAGIC
# MAGIC The brief's step 8 targets Redshift Spectrum and Glue statistics. Here the same idea sits on
# MAGIC Unity Catalog tables. Column statistics (row counts, distinct counts, min/max) let the
# MAGIC cost-based optimizer estimate how many rows each operator produces. That estimate decides join
# MAGIC order and whether a side is small enough to broadcast.
# MAGIC
# MAGIC `EXPLAIN COST` prints the optimizer's estimate for every operator. This notebook records the
# MAGIC estimate at the root of one representative query before and after `ANALYZE`. Predictive
# MAGIC optimization may already have collected statistics by itself, in which case "before" already
# MAGIC has a row count. That gets reported as found.

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import re
import sys

sys.path.insert(0, os.path.abspath("../.."))

from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.serving import category_daily_sql  # noqa: E402
from ecomm.tables import GOLD_CART_ABANDONMENT, GOLD_CONVERSION_HOURLY, GOLD_PRICE_ELASTICITY  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="statistics")
query = category_daily_sql(GOLD_CONVERSION_HOURLY.fqn(project))


def root_estimate() -> str:
    """The optimizer's estimate for the query's final result, from EXPLAIN COST."""
    plan = spark.sql(f"EXPLAIN COST {query}").first()[0]
    found = re.search(r"Statistics\(([^)]*)\)", plan)
    return found.group(1) if found else "no estimate"

# COMMAND ----------

before = ev.record("estimate_before_analyze", root_estimate())

for spec in (GOLD_CONVERSION_HOURLY, GOLD_PRICE_ELASTICITY, GOLD_CART_ABANDONMENT):
    spark.sql(f"ANALYZE TABLE {spec.fqn(project)} COMPUTE STATISTICS FOR ALL COLUMNS")

after = ev.record("estimate_after_analyze", root_estimate())
ev.record("estimate_gained_row_count", str("rowCount" in after and "rowCount" not in before),
          "True: statistics were added by ANALYZE; False: they already existed or are still missing")
ev.flush()
