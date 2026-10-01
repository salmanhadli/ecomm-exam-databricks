# Databricks notebook source
# MAGIC %md
# MAGIC # Evolution · Schema and clustering changes without rewriting data
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_35_evolution` → `evolve` (manual only, not in the orchestrator) |
# MAGIC | **Changes** | `silver.clickstream`: two new columns, a rename and revert, a new clustering key |
# MAGIC | **Brief** | Step 5 (5a schema evolution, 5b partition evolution) · acceptance **A6**, **A7** · evidence items 11–13 |
# MAGIC | **Databricks concepts** | Delta metadata-only changes · column mapping · liquid clustering key changes · `DESCRIBE HISTORY` / `DESCRIBE DETAIL` |
# MAGIC
# MAGIC The brief's claim to prove: these changes cost nothing, and no data file is rewritten.
# MAGIC On Delta:
# MAGIC
# MAGIC | Brief (Iceberg) | Here (Delta) | Proof |
# MAGIC |---|---|---|
# MAGIC | `ADD COLUMN` | `ADD COLUMNS … AFTER` | `numFiles` unchanged; the history entry adds no files |
# MAGIC | rename, resolved by column ID | rename with **column mapping** (`delta.columnMapping.mode = 'name'`) | old files still read under the new name |
# MAGIC | `ADD PARTITION FIELD bucket(16, product_id)` | `ALTER TABLE … CLUSTER BY (event_time, user_id, product_id)` | `numFiles` unchanged; new clustering columns reported |
# MAGIC
# MAGIC **One difference to state honestly.** Iceberg adds no snapshot for a schema change. Delta
# MAGIC records every change, metadata-only ones included, as a new table version. So the Delta
# MAGIC proof is *no data files added or removed*, not *no new version*.
# MAGIC
# MAGIC Safe to run more than once: each change is skipped if it is already in place.

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
from ecomm.tables import SILVER_CLICKSTREAM  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="evolution")
table = SILVER_CLICKSTREAM.fqn(project)


def state() -> dict:
    """What the proofs compare: version, data files, clustering, rows and columns."""
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    latest = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    return {
        "version": latest["version"],
        "files": detail["numFiles"],
        "bytes": detail["sizeInBytes"],
        "clustering": list(detail["clusteringColumns"] or []),
        "rows": spark.read.table(table).count(),
        "columns": spark.read.table(table).columns,
    }


def history_since(version: int):
    """History entries after `version`: the operation, and whether it touched data files."""
    return (spark.sql(f"DESCRIBE HISTORY {table}")
            .where(F.col("version") > version)
            .select("version", "operation", "operationParameters", "operationMetrics")
            .orderBy("version"))


start = state()
ev.record("evolution_start_version", start["version"])
ev.record("evolution_start_files", start["files"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · 5a Schema evolution: add two columns
# MAGIC
# MAGIC `ADD COLUMNS` writes only a new schema to the transaction log. Existing Parquet files don't
# MAGIC contain the new columns, so readers return NULL for them. That is correct, not an error.

# COMMAND ----------

before = state()
if "promotional_tag" not in before["columns"]:
    spark.sql(f"""
        ALTER TABLE {table} ADD COLUMNS (
            promotional_tag  STRING COMMENT 'Added in step 5a; NULL for rows written before it',
            price_change_pct DOUBLE COMMENT 'Added in step 5a; positioned after catalog_price' AFTER catalog_price
        )""")
after = state()

display(history_since(before["version"]))
nulls = spark.read.table(table).where(F.col("promotional_tag").isNull() & F.col("price_change_pct").isNull()).count()

ev.record("5a_files_before", before["files"])
ev.record("5a_files_after", after["files"])
ev.record("5a_rows_reading_null_in_new_columns", nulls, f"of {after['rows']:,} rows")
ev.record("5a_price_change_pct_position", after["columns"].index("price_change_pct"),
          "0-based; catalog_price is at " + str(after["columns"].index("catalog_price")))
schema_ok = after["files"] == before["files"] and nulls == after["rows"]
ev.record("acceptance_A6", "PASS" if schema_ok else "FAIL", "data files unchanged; existing rows read NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Why it works: rename a column, read old files, rename back
# MAGIC
# MAGIC With column mapping, Delta stores each column under a permanent physical name in the Parquet
# MAGIC files, and the table schema maps logical names to physical ones. A rename changes only that
# MAGIC mapping, so files written under the old name still resolve.

# COMMAND ----------

non_null_before = spark.read.table(table).where(F.col("category_code").isNotNull()).count()
spark.sql(f"ALTER TABLE {table} RENAME COLUMN category_code TO category_path")
renamed = state()
non_null_renamed = spark.read.table(table).where(F.col("category_path").isNotNull()).count()
spark.sql(f"ALTER TABLE {table} RENAME COLUMN category_path TO category_code")   # revert the demo

ev.record("rename_non_null_before", non_null_before)
ev.record("rename_non_null_under_new_name", non_null_renamed, "same values, read from the same old files")
ev.record("rename_files_unchanged", str(renamed["files"] == after["files"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · 5b Clustering evolution (the Delta counterpart of partition evolution)
# MAGIC
# MAGIC Silver is clustered by `(event_time, user_id)`, the brief's `hours(event_time)` +
# MAGIC `bucket(32, user_id)`. Q1 is product-centric, so `product_id` is added as a clustering key.
# MAGIC
# MAGIC **What the engine does.** Liquid clustering has no partition *spec* for queries to reconcile.
# MAGIC Every file keeps its own min/max column statistics, and a query skips files using those
# MAGIC statistics, however the file was laid out. Files written before the change keep the old
# MAGIC layout; new writes and `OPTIMIZE` use the new keys. A query spanning both is correct by
# MAGIC construction. Only its skipping efficiency differs between old and new files.
# MAGIC
# MAGIC **Interaction with maintenance (step 7).** Incremental `OPTIMIZE` clusters files that are not
# MAGIC yet clustered. `OPTIMIZE … FULL` would rewrite the history under the new keys. The Delta
# MAGIC counterpart of the brief's warning is: don't run `FULL` unless you mean to re-lay-out all data.

# COMMAND ----------

target_keys = ["event_time", "user_id", "product_id"]
before = state()
checksum_before = spark.read.table(table).agg(F.count("*"), F.sum("catalog_price")).first()

if before["clustering"] != target_keys:
    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(target_keys)})")
after = state()
checksum_after = spark.read.table(table).agg(F.count("*"), F.sum("catalog_price")).first()

display(history_since(before["version"]))

ev.record("5b_clustering_before", ", ".join(before["clustering"]))
ev.record("5b_clustering_after", ", ".join(after["clustering"]))
ev.record("5b_files_before", before["files"])
ev.record("5b_files_after", after["files"], "zero files rewritten by the key change")
ev.record("5b_rows_before", before["rows"])
ev.record("5b_rows_after", after["rows"])
ev.record("5b_query_result_unchanged", str(tuple(checksum_before) == tuple(checksum_after)),
          "count and sum(catalog_price) over old + new layout")
clustering_ok = (after["files"] == before["files"] and after["rows"] == before["rows"]
                 and after["clustering"] == target_keys
                 and tuple(checksum_before) == tuple(checksum_after))
ev.record("acceptance_A7", "PASS" if clustering_ok else "FAIL",
          "Delta: clustering keys changed with zero files rewritten")
ev.flush()

if not (schema_ok and clustering_ok):
    raise AssertionError("an evolution step rewrote data or changed results; see the evidence above")
