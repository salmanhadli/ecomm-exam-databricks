# Databricks notebook source
# MAGIC %md
# MAGIC # Serving · One question, three ways
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_70_serving` → `three_way` |
# MAGIC | **Reads** | `gold.product_conversion_hourly` · `gold.mv_category_daily` |
# MAGIC | **Brief** | Step 9's three-way table · acceptance **A12** · evidence item 20 |
# MAGIC | **Databricks concepts** | serverless Spark vs the SQL warehouse vs a materialized view · query history metrics |
# MAGIC
# MAGIC The brief compares Athena, a Redshift external schema and a Redshift materialized view. The
# MAGIC Databricks counterparts answer the same category-by-day question:
# MAGIC
# MAGIC | Path | What runs |
# MAGIC |---|---|
# MAGIC | **Serverless Spark** | this notebook's compute aggregates the gold table |
# MAGIC | **Warehouse, direct** | the SQL warehouse aggregates the gold table |
# MAGIC | **Warehouse, MV** | the SQL warehouse reads the precomputed materialized view |
# MAGIC
# MAGIC For each path: wall-clock time seen here, and for the warehouse paths the warehouse's own
# MAGIC record (duration, bytes read) from its query history. A warm-up statement runs first, so
# MAGIC warehouse start-up is not timed.
# MAGIC
# MAGIC **Result cache.** The warehouse returns a cached result for a repeated query on unchanged
# MAGIC tables, which would time the cache instead of the query. The first workspace run did exactly
# MAGIC that: `system.query.history` showed `from_result_cache = true` and 0 bytes read. A unique SQL
# MAGIC comment does **not** help, because the cache ignores comments (measured). Each statement here
# MAGIC carries a filter on a run-unique literal, `WHERE '<run id>' IS NOT NULL`. That is always true
# MAGIC and folded away by the optimizer, but no earlier result can match it.
# MAGIC
# MAGIC The statement ids are recorded too. `system.query.history` holds the warehouse's full record
# MAGIC of each one (duration, bytes read, cache use), usually a few minutes after it runs; see
# MAGIC `docs/results.md` for the lookup query.
# MAGIC
# MAGIC **Which to put behind a 5-minute dashboard?** The one that reads least on every refresh: the
# MAGIC materialized view, which pays its cost once per refresh (`mv_refresh_seconds`) instead of on
# MAGIC every dashboard load.

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from ecomm import perf  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.serving import category_daily_sql, mv_fqn  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY  # noqa: E402
from ecomm.warehouse import Warehouse  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="serving")
dbutils.widgets.text("warehouse_id", "")
warehouse = Warehouse(dbutils.widgets.get("warehouse_id"))

# A run-unique literal keeps the warehouse's result cache from answering instead
# (the cache ignores comments, so a tag in a comment is not enough).
nonce = f"'ecomm three_way run {ev.run_id}' IS NOT NULL"
direct_sql = category_daily_sql(GOLD_CONVERSION_HOURLY.fqn(project), where=nonce)
mv_sql = f"SELECT * FROM {mv_fqn(project)} WHERE {nonce}"

# COMMAND ----------

# MAGIC %md ## 1 · Serverless Spark

# COMMAND ----------

spark_rows = spark.sql(direct_sql)
perf.time_full_compute(spark_rows)                        # warm-up
ev.record("three_way.serverless_spark.seconds", round(perf.time_full_compute(spark_rows), 2))
ev.record("three_way.serverless_spark.rows", spark_rows.count())

# COMMAND ----------

# MAGIC %md ## 2 · SQL warehouse: direct, and through the materialized view

# COMMAND ----------

warehouse.run("SELECT 1")                                 # warm-up: the warehouse is running

for path, statement in (("warehouse_direct", direct_sql), ("warehouse_mv", mv_sql)):
    result = warehouse.run(statement)
    measured = warehouse.metrics(result.statement_id)
    ev.record(f"three_way.{path}.statement_id", result.statement_id, "look up in system.query.history")
    ev.record(f"three_way.{path}.seconds", round(result.seconds, 2), "wall clock seen by the notebook")
    ev.record(f"three_way.{path}.duration_ms", measured["duration_ms"], "the warehouse's own record")
    ev.record(f"three_way.{path}.read_bytes", measured["read_bytes"])
    ev.record(f"three_way.{path}.rows", len(result.rows))

ev.record("acceptance_A12", "PASS", "statistics, materialized view and three-way comparison recorded")
ev.flush()
