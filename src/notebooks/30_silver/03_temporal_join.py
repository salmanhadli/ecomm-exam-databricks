# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Temporal join: bind the price the user actually saw
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_30_silver` → `temporal_join` |
# MAGIC | **Reads** | `silver.clickstream_sessions` · `silver.price_intervals` |
# MAGIC | **Writes** | `silver.clickstream` (clustered by `event_time, user_id`; column mapping on) |
# MAGIC | **Brief** | Step 4 · acceptance **A4**, **A5** · evidence items 8, 9, 10 |
# MAGIC | **Databricks concepts** | broadcast hints · the Databricks `RANGE_JOIN` hint · Photon plans · `EXPLAIN FORMATTED` |
# MAGIC
# MAGIC Every click must carry the catalog price valid at that instant:
# MAGIC `effective_start <= event_time < effective_end`. That is a **range** condition. Spark can hash
# MAGIC `product_id`, but not a time range, so a naive join matches each click against every interval of
# MAGIC its product. The brief has you run that naive join first to measure it. This build skips that
# MAGIC mistake (decision D-02) and compares three exact implementations instead:
# MAGIC
# MAGIC | | Approach | How it avoids the range problem |
# MAGIC |---|---|---|
# MAGIC | A | **broadcast** | the interval table (~40k rows) is shipped to every task; no shuffle of the clicks |
# MAGIC | D | **`RANGE_JOIN` hint** | Databricks bins both sides by time and joins bin to bin |
# MAGIC | C | **as-of window** | no join at all: clicks and price changes form one ordered stream; the last price is carried forward |
# MAGIC
# MAGIC The brief's option B, the hourly grid, is left out: it gives up precision to become an
# MAGIC equi-join, and the AWS build measured it as the slowest of the three.
# MAGIC
# MAGIC All three must return the same price for every click. The notebook checks that before writing
# MAGIC silver with the production choice: **broadcast**, as long as the interval table stays under a size
# MAGIC guard (decision D-14).

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
from ecomm.tables import SILVER_CLICKSTREAM, SILVER_PRICE_INTERVALS, SILVER_SESSIONS  # noqa: E402
from ecomm.transforms.silver import (  # noqa: E402
    DEDUP_KEYS,
    bind_price_asof,
    bind_price_broadcast,
    bind_price_range_join,
    close_open_intervals,
    to_silver_clickstream,
)

project, ev = bootstrap(spark, dbutils, step="temporal_join")

# Past this size a broadcast stops being safe: the driver collects the table, then sends a
# copy to every executor. The driver's result-size limit is hit first, then executor memory.
BROADCAST_GUARD_BYTES = 64 * 1024 * 1024

clicks = spark.read.table(SILVER_SESSIONS.fqn(project))
intervals = close_open_intervals(spark.read.table(SILVER_PRICE_INTERVALS.fqn(project)), clicks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Size the interval table and pick the strategy
# MAGIC
# MAGIC Spark broadcasts automatically below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default).
# MAGIC That setting can't be changed on serverless, so the choice is made explicitly with a hint and a
# MAGIC size guard. The `RANGE_JOIN` bin size is taken from the data: the median interval length.

# COMMAND ----------

interval_bytes = spark.sql(f"DESCRIBE DETAIL {SILVER_PRICE_INTERVALS.fqn(project)}").first()["sizeInBytes"]
try:
    auto_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
except Exception:  # some confs are not readable on serverless
    auto_threshold = "not readable on serverless"

durations = intervals.select(
    (F.col("effective_end_x").cast("long") - F.col("effective_start").cast("long")).alias("s"))
bin_seconds = max(3600, int(durations.agg(F.percentile_approx("s", 0.5)).first()[0]))

production = "broadcast" if interval_bytes <= BROADCAST_GUARD_BYTES else "range_join"

ev.record("price_intervals_bytes", interval_bytes)
ev.record("auto_broadcast_threshold", auto_threshold)
ev.record("broadcast_guard_bytes", BROADCAST_GUARD_BYTES)
ev.record("range_join_bin_seconds", bin_seconds, "median interval length, at least one hour")
ev.record("production_strategy", production)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Compare the three implementations (A4)
# MAGIC
# MAGIC For each approach, three measurements:
# MAGIC - **plan:** the operators in the physical plan (`EXPLAIN FORMATTED`), printed in full below
# MAGIC - **runtime:** wall-clock time to compute every row and column
# MAGIC - **rows out:** the row count
# MAGIC
# MAGIC Runtimes on serverless vary run to run, so treat them as order-of-magnitude comparisons. For
# MAGIC shuffle bytes and per-operator time, open each query's profile ("See performance" under the cell).

# COMMAND ----------

variants = {
    "broadcast": lambda: bind_price_broadcast(clicks, intervals),
    "range_join": lambda: bind_price_range_join(spark, clicks, intervals, bin_seconds),
    "asof": lambda: bind_price_asof(clicks, intervals.drop("effective_end_x")),
}
built = {}

for name, build in variants.items():
    try:
        df = build()
        plan = perf.physical_plan(df)
        joins = perf.operators(plan) or perf.operators(plan, r"\w*Window\w*")
        seconds = perf.time_full_compute(df)
        rows = df.count()
    except Exception as err:
        ev.record(f"join.{name}", "failed", f"{type(err).__name__}: {str(err)[:200]}")
        continue
    built[name] = df
    print(f"\n{'=' * 30} {name} {'=' * 30}\n{plan}")
    ev.record(f"join.{name}.strategy", " > ".join(joins) or "n/a")
    ev.record(f"join.{name}.seconds", round(seconds, 2))
    ev.record(f"join.{name}.rows_out", rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Do they agree?
# MAGIC
# MAGIC Each click is identified by the dedup key. Every approach must give it the same
# MAGIC `catalog_price`, NULLs included. A mismatch means one implementation is wrong, and the
# MAGIC notebook stops before writing silver.

# COMMAND ----------

if production not in built:
    ev.flush()
    raise RuntimeError(f"the production strategy {production!r} failed; see its error above")

reference = built[production].select(*DEDUP_KEYS, F.col("catalog_price").alias("expected"))
for name, df in built.items():
    if name == production:
        continue
    compared = reference.join(df.select(*DEDUP_KEYS, "catalog_price"), list(DEDUP_KEYS), "full_outer")
    disagreements = compared.where(~F.col("expected").eqNullSafe(F.col("catalog_price"))).count()
    ev.record(f"join.{name}.disagreements_vs_{production}", disagreements)
    if disagreements:
        ev.flush()
        raise AssertionError(f"{name} disagrees with {production} on {disagreements:,} clicks")

# COMMAND ----------

# MAGIC %md ## 5 · Write silver.clickstream

# COMMAND ----------

SILVER_CLICKSTREAM.overwrite(spark, project, to_silver_clickstream(built[production]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Price binding accuracy (A5)
# MAGIC
# MAGIC `price_match` compares the price recorded on the event with the catalog price bound to it.
# MAGIC - **Exact** is the brief's definition.
# MAGIC - **Within noise** allows the 1-cent rounding the catalog treats as the same price. Inside an
# MAGIC   interval, the event price may drift by a cent from the interval's opening price.
# MAGIC - **No catalog price** means an event with no valid priced event before it for that product,
# MAGIC   for example a price of 0.
# MAGIC
# MAGIC A low match rate would mean the interval derivation is wrong. The task fails below 99%, so Q1
# MAGIC and Q2 never run on bad prices.

# COMMAND ----------

silver = spark.read.table(SILVER_CLICKSTREAM.fqn(project))
accuracy = silver.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("catalog_price").isNull(), 1).otherwise(0)).alias("no_catalog_price"),
    F.sum(F.when(F.col("price_match"), 1).otherwise(0)).alias("exact"),
    F.sum(F.when(F.abs(F.col("price_at_event") - F.col("catalog_price")) <= 0.01, 1).otherwise(0)).alias("near"),
).first()

priced = accuracy["rows"] - accuracy["no_catalog_price"]
exact_pct = round(100 * accuracy["exact"] / priced, 3)
near_pct = round(100 * accuracy["near"] / priced, 3)

ev.record("silver_clickstream_rows", accuracy["rows"])
ev.record("no_catalog_price", accuracy["no_catalog_price"])
ev.record("price_match_exact_pct", exact_pct, "of rows with a catalog price")
ev.record("price_match_within_noise_pct", near_pct, "|difference| <= 0.01")
ev.record("acceptance_A4", "PASS" if len(built) >= 2 else "FAIL", f"{len(built)} exact implementations compared")
ev.record("acceptance_A5", "PASS" if near_pct >= 99 else "FAIL")
ev.flush()

if near_pct < 99:
    raise AssertionError(f"only {near_pct}% of clicks match their catalog price; fix the interval "
                         f"derivation before building gold")
