# Databricks notebook source
# MAGIC %md
# MAGIC # Harness · Stream A — clickstream replay
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_10_harness` → `harness_clickstream` |
# MAGIC | **Reads** | `/Volumes/<catalog>/raw/landing/source/cosmetics/*.csv` |
# MAGIC | **Writes** | `/Volumes/<catalog>/raw/landing/clickstream/dt=*/hh=*/min5=*/` — one JSON file per 5-minute interval |
# MAGIC | **Brief** | Stream A · acceptance A1 · evidence item 3 |
# MAGIC | **Databricks concepts** | serverless compute (Spark Connect) · volumes · the `_metadata` file column |
# MAGIC
# MAGIC **Why replay?** The Kaggle file is one flat export, but a real platform receives
# MAGIC clickstream as a steady stream of small files. This replays the first 14 days as one JSON
# MAGIC file per 5-minute interval — about 4,000 small files. The brief requires this input shape;
# MAGIC the bronze pipeline (Auto Loader) is what keeps it from becoming a small-file problem in
# MAGIC the tables.
# MAGIC
# MAGIC **Serverless notes**
# MAGIC - `.cache()` raises on serverless, so nothing is cached. The output is verified by reading
# MAGIC   back what landed, which is far smaller than the source.
# MAGIC - The replay bound is computed inside Spark (`replay_window`), never collected to Python,
# MAGIC   so it can't pick up the driver's time zone.
# MAGIC
# MAGIC **Re-runs.** With `replay_mode=skip_if_present` (the default) an existing landing is left
# MAGIC alone. Spark names output files with a fresh UUID on every write, and Auto Loader tracks
# MAGIC files by path, so a second replay would look like ~4,000 *new* files and double bronze.

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
from ecomm.transforms.harness import replay_window, stage_micro_batches  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="harness_clickstream")
dbutils.widgets.dropdown("replay_mode", "skip_if_present", ["skip_if_present", "overwrite"])

target = project.clickstream_dir
already_landed = os.path.isdir(target) and any(os.scandir(target))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Replay into micro-batches
# MAGIC
# MAGIC `repartition("dt", "hh", "min5")` sends every row of one interval to the same task, so each
# MAGIC `dt/hh/min5` folder receives exactly one file.

# COMMAND ----------

if already_landed and dbutils.widgets.get("replay_mode") == "skip_if_present":
    ev.record("replay", "skipped", f"{target} already populated; replay_mode=overwrite forces a new replay")
else:
    events = (spark.read.format("csv")
              .option("header", "true")
              .schema(EVENT_SCHEMA)
              .load(project.source_dir))

    staged = stage_micro_batches(replay_window(events, SETTINGS.simulated_days),
                                 SETTINGS.micro_batch_minutes)

    (staged.repartition("dt", "hh", "min5")
        .write.mode("overwrite")
        .partitionBy("dt", "hh", "min5")
        .format("json")
        .save(target))
    ev.record("replay", "written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Verify what landed and record evidence
# MAGIC
# MAGIC `_metadata.file_path` is a hidden column available on every file read; it replaces
# MAGIC `input_file_name()`, which Unity Catalog does not support. Counting distinct files against
# MAGIC distinct `dt/hh/min5` folders proves the one-file-per-interval shape.

# COMMAND ----------

landed = (spark.read.schema(EVENT_SCHEMA).json(target)
          .select("event_time", F.col("_metadata.file_path").alias("file_path")))

stats = landed.agg(
    F.date_format(F.min("event_time"), "yyyy-MM-dd HH:mm:ss").alias("first_event"),
    F.date_format(F.max("event_time"), "yyyy-MM-dd HH:mm:ss").alias("last_event"),
    F.count("*").alias("rows"),
    F.countDistinct("file_path").alias("files"),
    F.countDistinct(F.regexp_extract("file_path", r"(dt=[^/]+/hh=[^/]+/min5=[^/]+)", 1)).alias("intervals"),
).first()

ev.record("replay_window_start_utc", stats["first_event"])
ev.record("replay_window_end_utc", stats["last_event"])
ev.record("simulated_days", SETTINGS.simulated_days)
ev.record("micro_batch_minutes", SETTINGS.micro_batch_minutes)
ev.record("events_landed", stats["rows"])
ev.record("micro_batch_intervals", stats["intervals"])
ev.record("micro_batch_files", stats["files"])
ev.flush()

if stats["files"] != stats["intervals"]:
    raise AssertionError(f"expected one file per interval, got {stats['files']} files "
                         f"for {stats['intervals']} intervals")
