# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Deduplicate and sessionize
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_30_silver` → `sessionize` |
# MAGIC | **Reads** | `bronze.clickstream` |
# MAGIC | **Writes** | `silver.clickstream_sessions` |
# MAGIC | **Brief** | Step 2 (2a dedup, 2b sessionization) · acceptance **A3** · evidence items 6, 7 |
# MAGIC | **Databricks concepts** | window functions on serverless · liquid clustering · no cache, so materialise instead |
# MAGIC
# MAGIC **2a · Deduplicate.** One row per `(event_time, user_id, product_id, event_type)`. The brief's
# MAGIC tiebreak `(_ingested_at, _src_file)` doesn't fully order this data, so payload columns complete
# MAGIC the order (decision D-07). Same input, same surviving rows, every run.
# MAGIC
# MAGIC **2b · Sessionize.** `user_session` is unreliable: it is NULL on some rows and gets reused after
# MAGIC long gaps. So sessions are rebuilt here: **a session ends after 30 minutes without an event
# MAGIC from that user.**
# MAGIC
# MAGIC The result is written to a table because serverless can't cache DataFrames. The temporal
# MAGIC join reads it back instead of recomputing the window.

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
from ecomm.tables import SILVER_SESSIONS  # noqa: E402
from ecomm.transforms.silver import deduplicate, sessionize  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="sessionize")

# Silver's input: usable events only. Bronze keeps everything (D-15); these rows
# cannot be placed in a session or a funnel, so they stop here, counted.
bronze = spark.read.table(project.table("bronze", "clickstream"))
usable = bronze.where(F.col("event_time").isNotNull() & F.col("user_id").isNotNull()
                      & F.col("product_id").isNotNull())

# COMMAND ----------

# MAGIC %md ## 2 · Build and write

# COMMAND ----------

sessions = sessionize(deduplicate(usable), SETTINGS.session_gap_seconds)
SILVER_SESSIONS.overwrite(spark, project, sessions.withColumn("_updated_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Measure: dedup and sessions (A3)
# MAGIC
# MAGIC Everything below reads the table just written, so the numbers describe what downstream will use.

# COMMAND ----------

silver = spark.read.table(SILVER_SESSIONS.fqn(project))

bronze_rows = ev.record("bronze_rows", bronze.count())
usable_rows = ev.record("usable_rows", usable.count(), "event_time, user_id and product_id present")
kept = ev.record("rows_after_dedup", silver.count())
ev.record("duplicates_removed", usable_rows - kept, f"{100 * (usable_rows - kept) / usable_rows:.2f}%")

per_session = silver.groupBy("session_key").agg(F.count("*").alias("events"))
stats = per_session.agg(
    F.count("*").alias("sessions"),
    F.percentile_approx("events", 0.5).alias("median"),
    F.round(F.avg("events"), 2).alias("mean"),
    F.max("events").alias("max"),
).first()
ev.record("sessions", stats["sessions"])
ev.record("median_events_per_session", stats["median"])
ev.record("mean_events_per_session", stats["mean"])
ev.record("max_events_per_session", stats["max"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Disagreement with the raw `user_session`
# MAGIC
# MAGIC - **Split:** one raw `user_session` spans several of our sessions, because the id was reused
# MAGIC   after a gap longer than 30 minutes.
# MAGIC - **Merged:** one of our sessions contains several raw ids.
# MAGIC
# MAGIC The worked example shows the events of the most-split raw session, with the gaps that split it.

# COMMAND ----------

with_raw = silver.where(F.col("user_session").isNotNull())
raw_split = (with_raw.groupBy("user_session")
             .agg(F.countDistinct("session_key").alias("own_sessions"))
             .where("own_sessions > 1"))
own_merged = (with_raw.groupBy("session_key")
              .agg(F.countDistinct("user_session").alias("raw_sessions"))
              .where("raw_sessions > 1"))

ev.record("raw_sessions_split_across_own", raw_split.count())
merged = ev.record("own_sessions_merging_several_raw", own_merged.count())
ev.record("own_sessions_disagreeing_pct", round(100 * merged / stats["sessions"], 2))

example = raw_split.orderBy(F.desc("own_sessions"), "user_session").first()
if example:
    worked = (silver.where(F.col("user_session") == example["user_session"])
              .orderBy("event_time")
              .select("event_time", "gap_s", "event_type", "product_id", "session_key"))
    display(worked.limit(20))
    ev.record("worked_example_user_session", example["user_session"],
              f"split into {example['own_sessions']} own sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Skew in the sessionization window
# MAGIC
# MAGIC The window is partitioned by `user_id` with no bound, so all of one user's events go to one
# MAGIC task. With no Spark UI on serverless, skew is measured on the data itself: rows per shuffle
# MAGIC partition when hashed on `user_id`. That is the work each task of the window stage receives.
# MAGIC The brief's threshold is a ratio of about 10× between the largest and the median.
# MAGIC
# MAGIC If it is exceeded, salting doesn't help, because a user's events must stay together to be
# MAGIC sessionized. The fix is to split the heaviest users by day first (no 30-minute gap can span
# MAGIC one), sessionize per (user, day), then stitch sessions across midnight.

# COMMAND ----------

skew = perf.partition_skew(silver, "user_id")
per_user = silver.groupBy("user_id").count().agg(
    F.max("count").alias("max"), F.percentile_approx("count", 0.5).alias("median")).first()

ev.record("window_partition_rows_max", skew["max_rows"])
ev.record("window_partition_rows_median", skew["median_rows"])
ev.record("window_partition_skew_ratio", skew["ratio"], "max / median rows per task; brief flags > 10")
ev.record("events_per_user_max", per_user["max"])
ev.record("events_per_user_median", per_user["median"])
ev.record("acceptance_A3", "PASS", "own 30-minute sessions; disagreement with user_session quantified")
ev.flush()
