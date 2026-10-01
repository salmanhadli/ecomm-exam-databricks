"""Harness transformations: turn the flat Kaggle export into the two feeds the platform receives.

Stream A  `replay_window` + `stage_micro_batches`: the first N days of events, each
          tagged with the 5-minute micro-batch it belongs to (dt / hh / min5).
Stream B  `derive_price_intervals`: per-event prices collapsed into validity intervals
          `product_id, price, effective_start, effective_end, change_reason, pct_change`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# --- Stream A: clickstream replay ---------------------------------------------------

def replay_window(events: DataFrame, days: int) -> DataFrame:
    """Keep the first `days` of the export: `event_time < min(event_time) + days`.

    The bound is computed inside Spark (a one-row aggregate, broadcast to the
    filter). Collecting it to Python would render it in the driver's time zone.
    Rows with a NULL event_time fail the comparison and are dropped here.
    """
    bound = events.agg(
        (F.min("event_time") + F.expr(f"INTERVAL {int(days)} DAYS")).alias("_replay_end"))
    return (events
            .crossJoin(F.broadcast(bound))
            .where(F.col("event_time") < F.col("_replay_end"))
            .drop("_replay_end"))


def stage_micro_batches(events: DataFrame, minutes: int) -> DataFrame:
    """Add dt / hh / min5: the path parts of the micro-batch each event falls in.

    An event at 10:07:59 with 5-minute batches belongs to the 10:05 batch:
    dt=<date>, hh=10, min5=05.
    """
    seconds = int(minutes) * 60
    batch_start = F.to_timestamp(F.floor(F.unix_timestamp("event_time") / seconds) * seconds)
    return (events
            .withColumn("_batch_start", batch_start)
            .withColumn("dt", F.date_format("_batch_start", "yyyy-MM-dd"))
            .withColumn("hh", F.date_format("_batch_start", "HH"))
            .withColumn("min5", F.date_format("_batch_start", "mm"))
            .drop("_batch_start"))


# --- Stream B: price catalog ------------------------------------------------------------

def priced_events(events: DataFrame) -> DataFrame:
    """Events that can carry a catalog price, with the price in integer cents.

    A price of 0 or NULL is known dirt, not a catalog price. Cents make every
    comparison exact: 100.00 - 99.99 is exactly 1, not 0.010000000000005116.
    """
    return (events
            .where(F.col("product_id").isNotNull() & F.col("event_time").isNotNull()
                   & F.col("price").isNotNull() & (F.col("price") > 0))
            .select("product_id", "event_time",
                    F.round(F.col("price") * 100).cast("long").alias("price_cents")))


def derive_price_intervals(events: DataFrame, noise_threshold_cents: int,
                           precision: int = 12, scale: int = 2) -> DataFrame:
    """Collapse per-event prices into closed validity intervals, one row per price run.

    1. Order each product's events by time and compare every price with the previous
       one. A difference above `noise_threshold_cents` starts a new run.
    2. A running sum of those "new run" flags numbers the runs (0, 1, 2, ...).
    3. Each run becomes one interval, priced at its opening price.
    4. `effective_end` is the next interval's start; the newest stays NULL (open).

    Determinism: both windows carry a secondary sort key, so two events of one
    product at the same instant always resolve the same way on every run.
    """
    by_time = Window.partitionBy("product_id").orderBy("event_time", "price_cents")

    runs = (priced_events(events)
            .withColumn("prev_cents", F.lag("price_cents").over(by_time))
            .withColumn("is_new_run",
                        F.when(F.col("prev_cents").isNull(), F.lit(1))
                         .when(F.abs(F.col("price_cents") - F.col("prev_cents")) > noise_threshold_cents,
                               F.lit(1))
                         .otherwise(F.lit(0)))
            .withColumn("run_id", F.sum("is_new_run").over(
                by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow))))

    # min_by returns the price of the run's earliest event, which is the price that
    # became effective at effective_start. (min("price") would return the run's
    # cheapest price instead, and prices can drift by one cent inside a run.)
    intervals = runs.groupBy("product_id", "run_id").agg(
        F.min("event_time").alias("effective_start"),
        F.min_by("price_cents", F.struct("event_time", "price_cents")).alias("price_cents"),
    )

    by_start = Window.partitionBy("product_id").orderBy("effective_start", "run_id")
    return (intervals
            .withColumn("effective_end", F.lead("effective_start").over(by_start))
            .withColumn("prev_cents", F.lag("price_cents").over(by_start))
            .withColumn("pct_change", F.try_divide(
                (F.col("price_cents") - F.col("prev_cents")).cast("double"),
                F.col("prev_cents").cast("double")))
            .withColumn("change_reason",
                        F.when(F.col("prev_cents").isNull(), F.lit("initial"))
                         .when(F.col("price_cents") > F.col("prev_cents"), F.lit("increase"))
                         .when(F.col("price_cents") < F.col("prev_cents"), F.lit("decrease"))
                         # possible when a run drifted in one-cent steps before a real
                         # change brought the price back to the previous run's opening price
                         .otherwise(F.lit("no_net_change")))
            .select("product_id",
                    (F.col("price_cents") / 100).cast(DecimalType(precision, scale)).alias("price"),
                    "effective_start", "effective_end", "change_reason", "pct_change"))


def products_with_significant_change(intervals: DataFrame, pct: float) -> int:
    """How many products have at least one change above `pct` in either direction (A2)."""
    return (intervals
            .where(F.abs(F.col("pct_change")) > pct)
            .select("product_id").distinct().count())
