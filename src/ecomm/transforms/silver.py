"""Silver transformations: deduplicate, sessionize, and bind the price the user actually saw.

Step 2a  `deduplicate`        one row per natural key, chosen by a total order
Step 2b  `sessionize`         own sessions: 30 minutes of inactivity ends a session
Step 4   `bind_price_*`       three exact implementations of the temporal join
         `latest_intervals`   silver's view of the price catalog feed
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

MONEY = DecimalType(12, 2)

# The brief's dedup key: the same user doing the same thing to the same product at the same instant.
DEDUP_KEYS = ("event_time", "user_id", "product_id", "event_type")
# The brief's tiebreakers, newest first...
DEDUP_TIEBREAK_DESC = ("_ingested_at", "_src_file")
# ...which are not enough on this data: duplicates sit in the same file with one ingestion
# time, and on AWS 267 groups still differed in their payload. These columns complete a total
# order, so the surviving row is the same on every run (decision D-07).
DEDUP_PAYLOAD = ("price", "user_session", "category_id", "category_code", "brand")


# --- Step 2a · deduplicate ------------------------------------------------------------

def deduplicate(events: DataFrame) -> DataFrame:
    """Keep one row per DEDUP_KEYS, chosen deterministically."""
    payload = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in DEDUP_PAYLOAD])
    order = [F.col(c).desc() for c in DEDUP_TIEBREAK_DESC] + [payload.asc()]
    ranked = Window.partitionBy(*DEDUP_KEYS).orderBy(*order)
    return (events
            .withColumn("_rank", F.row_number().over(ranked))
            .where(F.col("_rank") == 1)
            .drop("_rank"))


# --- Step 2b · sessionize ---------------------------------------------------------------

def sessionize(events: DataFrame, gap_seconds: int) -> DataFrame:
    """Assign each event to an own session: a new session starts after `gap_seconds` idle.

    Same shape as the price runs: flag each event that starts a session, then a running
    sum of the flags numbers the user's sessions. The window is unbounded per user, so
    every event of one user is processed by one task, which is the skew risk step 2 measures.
    """
    by_user = Window.partitionBy("user_id").orderBy("event_time", "event_type", "product_id")
    return (events
            .withColumn("_prev_time", F.lag("event_time").over(by_user))
            .withColumn("gap_s", F.col("event_time").cast("long") - F.col("_prev_time").cast("long"))
            .withColumn("_starts_session",
                        F.when(F.col("_prev_time").isNull() | (F.col("gap_s") > gap_seconds), 1).otherwise(0))
            .withColumn("session_seq", F.sum("_starts_session").over(
                by_user.rowsBetween(Window.unboundedPreceding, Window.currentRow)).cast("bigint"))
            .withColumn("session_key", F.concat_ws("#", "user_id", "session_seq"))
            .drop("_prev_time", "_starts_session"))


# --- silver price intervals ----------------------------------------------------------------

def latest_intervals(bronze_catalog: DataFrame) -> DataFrame:
    """One row per (product_id, effective_start): the most recently ingested version.

    Bronze is append-only, so a re-landed feed would appear twice; silver keeps the latest.
    """
    newest = Window.partitionBy("product_id", "effective_start").orderBy(
        F.col("_ingested_at").desc(), F.col("_src_file").desc())
    return (bronze_catalog
            .withColumn("_rank", F.row_number().over(newest))
            .where(F.col("_rank") == 1)
            .select("product_id", F.col("price").cast(MONEY).alias("price"), "effective_start",
                    "effective_end", "change_reason", "pct_change"))


def close_open_intervals(intervals: DataFrame, clicks: DataFrame) -> DataFrame:
    """Add `effective_end_x`: the interval end, with the open newest interval closed.

    The open end is replaced by a day past the last click. Any bound strictly after the
    last event works, and a near bound keeps range-join bins small; a year-2999 sentinel
    would not. Computed inside Spark, so no timestamp is collected to Python.
    """
    last_click = clicks.agg((F.max("event_time") + F.expr("INTERVAL 1 DAY")).alias("_data_end"))
    return (intervals.crossJoin(F.broadcast(last_click))
            .withColumn("effective_end_x", F.coalesce("effective_end", "_data_end"))
            .drop("_data_end"))


# --- Step 4 · temporal join: three exact implementations ---------------------------------
# Each returns the clicks with one extra column, `catalog_price`: the price of the interval
# where effective_start <= event_time < effective_end. They must agree row for row; the
# notebook checks that before choosing one to write.

def bind_price_broadcast(clicks: DataFrame, intervals: DataFrame) -> DataFrame:
    """A: ship the small interval table to every task. product_id is the hash key; the time
    range is a filter evaluated per matched pair. No shuffle of the large side."""
    p = intervals.select("product_id", "effective_start", "effective_end_x",
                         F.col("price").alias("catalog_price")).alias("p")
    c = clicks.alias("c")
    return (c.join(F.broadcast(p),
                   (F.col("c.product_id") == F.col("p.product_id"))
                   & (F.col("c.event_time") >= F.col("p.effective_start"))
                   & (F.col("c.event_time") < F.col("p.effective_end_x")),
                   "left")
            .select("c.*", "p.catalog_price"))


def bind_price_range_join(spark, clicks: DataFrame, intervals: DataFrame, bin_seconds: int) -> DataFrame:
    """D: Databricks range-join optimisation, requested with the RANGE_JOIN hint.

    Databricks buckets both sides into time bins of `bin_seconds` and joins bin to bin, so a
    point only meets the intervals that overlap its bin. Built for exactly this point-in-interval
    shape (a LEFT join with the point on the left side).
    """
    clicks.createOrReplaceTempView("_rj_clicks")
    intervals.createOrReplaceTempView("_rj_intervals")
    return spark.sql(f"""
        SELECT /*+ RANGE_JOIN(p, {int(bin_seconds)}) */ c.*, p.price AS catalog_price
        FROM _rj_clicks c
        LEFT JOIN _rj_intervals p
          ON c.product_id = p.product_id
         AND c.event_time >= p.effective_start
         AND c.event_time <  p.effective_end_x
    """)


def bind_price_asof(clicks: DataFrame, intervals: DataFrame) -> DataFrame:
    """C: as-of join. Put clicks and price changes in one stream per product, ordered by time,
    and carry the last known price forward onto each click. No join operator at all.

    A price change must sort before a click at the same instant: intervals were derived from
    the events, so an interval's first click shares its timestamp with the interval start.
    """
    click_rows = (clicks
                  .withColumn("_ts", F.col("event_time"))
                  .withColumn("_new_price", F.lit(None).cast(MONEY))
                  .withColumn("_kind", F.lit(1)))
    price_rows = intervals.select("product_id", F.col("effective_start").alias("_ts"),
                                  F.col("price").alias("_new_price"), F.lit(0).alias("_kind"))
    carried = Window.partitionBy("product_id").orderBy("_ts", "_kind").rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
    return (click_rows.unionByName(price_rows, allowMissingColumns=True)
            .withColumn("catalog_price", F.last("_new_price", ignorenulls=True).over(carried))
            .where(F.col("_kind") == 1)
            .drop("_ts", "_new_price", "_kind"))


def to_silver_clickstream(bound: DataFrame) -> DataFrame:
    """Shape a price-bound click stream into silver.clickstream's columns."""
    return (bound
            .withColumn("price_at_event", F.col("price").cast(MONEY))
            .withColumn("catalog_price", F.col("catalog_price").cast(MONEY))
            .withColumn("price_match", F.col("price_at_event") == F.col("catalog_price"))
            .withColumn("_updated_at", F.current_timestamp())
            .select("event_time", "event_type", "product_id", "category_id", "category_code",
                    "brand", "price_at_event", "catalog_price", "price_match",
                    "user_id", "session_key", "_updated_at"))
