# Databricks notebook source
# MAGIC %md
# MAGIC # Audit · Prove immutability under live writes (Q3)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_60_audit` → `immutability_proof` (after `freeze`) |
# MAGIC | **Changes** | `gold.product_conversion_hourly`: heavy DML on purpose, then `RESTORE` |
# MAGIC | **Brief** | Q3 · step 7b's four-step proof · acceptance **A11** · evidence item 18 |
# MAGIC | **Databricks concepts** | Delta DML (`UPDATE`/`DELETE`/`INSERT`) · time travel · `RESTORE TABLE` |
# MAGIC
# MAGIC The brief: *"Prove immutability, do not assert it."*
# MAGIC
# MAGIC | Reading | What | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | the frozen copy, before any DML | the Black Friday numbers |
# MAGIC | 2 | *(heavy `UPDATE`, `DELETE` and `INSERT` against the live table)* | |
# MAGIC | 3 | the frozen copy again | **identical** to reading 1 |
# MAGIC | 4 | the live table | **different** from reading 1 |
# MAGIC
# MAGIC A second mechanism is checked alongside: **time travel** on the live table to the frozen version
# MAGIC must also equal reading 1. That works because the live table keeps 365 days of history (D-09).
# MAGIC
# MAGIC Finally, the live table is put back with `RESTORE TABLE … TO VERSION AS OF` the pre-DML version,
# MAGIC so the dashboards stay right. `RESTORE` is itself a new version, so even the damage stays in the
# MAGIC history for inspection.
# MAGIC
# MAGIC Each reading: row count, total purchases, total revenue, and the purchases of the top category.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm import audit  # noqa: E402
from ecomm.project import audit_table_name  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="immutability_proof")
dbutils.widgets.text("audit_name", "black_friday_2026_final")

live = GOLD_CONVERSION_HOURLY.fqn(project)
frozen = project.table("audit", audit_table_name(dbutils.widgets.get("audit_name")))
props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {frozen}").collect()}
frozen_version = int(props[audit.PROP_VERSION])


def current_version(table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def show(label: str, value: tuple) -> tuple:
    rows, purchases, revenue, category = value
    ev.record(label, f"rows={rows:,} purchases={purchases:,} revenue={revenue} top_category={category:,}")
    return value

# COMMAND ----------

# MAGIC %md ## 2 · Reading 1: the frozen copy

# COMMAND ----------

category = audit.top_category(spark.read.table(frozen))
ev.record("tracked_category_id", category)
reading_1 = show("reading_1_frozen", audit.reading(spark.read.table(frozen), category))
time_travel_before = audit.reading(spark.sql(f"SELECT * FROM {live} VERSION AS OF {frozen_version}"), category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Heavy DML against the live table
# MAGIC
# MAGIC Three kinds of change, each its own new version of the live table:
# MAGIC - **UPDATE:** inflate the tracked category's purchases.
# MAGIC - **DELETE:** drop the first day of data.
# MAGIC - **INSERT:** add a sentinel row no real product could produce.
# MAGIC
# MAGIC The cutoff for the DELETE is computed and formatted inside Spark, so it is exact in UTC.

# COMMAND ----------

pre_dml_version = current_version(live)
cutoff = (spark.read.table(live)
          .agg(F.date_format(F.min("event_hour") + F.expr("INTERVAL 1 DAY"), "yyyy-MM-dd HH:mm:ss"))
          .first()[0])

spark.sql(f"UPDATE {live} SET purchases = purchases + 1000 WHERE category_id = '{category}'")
spark.sql(f"DELETE FROM {live} WHERE event_hour < timestamp'{cutoff}'")
spark.sql(f"""
    INSERT INTO {live} (product_id, category_id, category_code, event_hour, catalog_price, views, carts,
                        purchases, view_to_cart, cart_to_purchase, overall_conv, revenue, computed_at)
    VALUES ('__audit_sentinel__', '{category}', NULL, timestamp'{cutoff}', 1.00, 1, 1,
            1000000, 1.0, 1.0, 1.0, 1000000.00, current_timestamp())""")

ev.record("live_version_before_dml", pre_dml_version)
ev.record("live_version_after_dml", current_version(live), "UPDATE + DELETE + INSERT")
ev.record("dml_delete_cutoff_utc", cutoff)

# COMMAND ----------

# MAGIC %md ## 4 · Readings 3 and 4, and time travel

# COMMAND ----------

reading_3 = show("reading_3_frozen_after_dml", audit.reading(spark.read.table(frozen), category))
reading_4 = show("reading_4_live_after_dml", audit.reading(spark.read.table(live), category))
time_travel = audit.reading(spark.sql(f"SELECT * FROM {live} VERSION AS OF {frozen_version}"), category)

frozen_identical = reading_3 == reading_1
live_moved = reading_4 != reading_1
time_travel_holds = time_travel == reading_1 == time_travel_before

ev.record("frozen_identical_after_dml", str(frozen_identical))
ev.record("live_differs_after_dml", str(live_moved))
ev.record("live_time_travel_to_frozen_version_matches", str(time_travel_holds),
          f"SELECT … VERSION AS OF {frozen_version}")

# COMMAND ----------

# MAGIC %md ## 5 · Put the live table back with RESTORE

# COMMAND ----------

spark.sql(f"RESTORE TABLE {live} TO VERSION AS OF {pre_dml_version}")
restored = audit.reading(spark.read.table(live), category)
pre_dml = audit.reading(spark.sql(f"SELECT * FROM {live} VERSION AS OF {pre_dml_version}"), category)
ev.record("live_restored_to_version", pre_dml_version, f"RESTORE committed as version {current_version(live)}")
ev.record("live_matches_pre_dml_after_restore", str(restored == pre_dml))

proof = frozen_identical and live_moved and time_travel_holds and restored == pre_dml
ev.record("q3_immutability_proof", "PASS" if proof else "FAIL")
ev.flush()

if not proof:
    raise AssertionError("the immutability proof failed; see the readings above")
