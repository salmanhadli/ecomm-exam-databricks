"""Local parity check: harness -> silver -> gold on the full data, before anything is deployed.

Runs the exact functions the notebooks use (`src/ecomm/transforms/`) on a local pyspark
session against the five Kaggle CSVs, and compares each layer with the numbers measured by
the earlier AWS implementation of the same brief (EMR + Iceberg). It also re-runs the AWS
float-threshold logic on the same data, so the effect of the integer-cents fix (D-06) is
measured, not assumed.

    ECOMM_SOURCE_DIR=/path/to/ecommerce-events-history-in-cosmetics-shop \
        uv run python tests/parity/local_parity.py

ECOMM_SOURCE_DIR defaults to data/ecommerce-events-history-in-cosmetics-shop in this repo
(git-ignored). It takes about 30 seconds on a laptop.
"""

import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repository root
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from ecomm.schemas import EVENT_SCHEMA  # noqa: E402
from ecomm.settings import SETTINGS  # noqa: E402
from ecomm.transforms.gold import (  # noqa: E402
    abandoned_carts,
    carts_after_increase,
    conversion_hourly,
    elasticity,
    significant_changes,
)
from ecomm.transforms.harness import (  # noqa: E402
    derive_price_intervals,
    priced_events,
    products_with_significant_change,
    replay_window,
    stage_micro_batches,
)
from ecomm.transforms.silver import (  # noqa: E402
    bind_price_asof,
    bind_price_broadcast,
    bind_price_range_join,
    close_open_intervals,
    deduplicate,
    sessionize,
    to_silver_clickstream,
)

AWS_SILVER = {  # measured by the AWS implementation, steps 2 and 4
    "rows_after_dedup": 1_855_165,
    "sessions": 321_828,
    "median_events_per_session": 2,
    "no_catalog_price": 2_385,
}

AWS = {  # measured by the AWS implementation, harness
    "events_replayed": 1_960_971,
    "micro_batch_intervals": 4_032,
    "events_excluded": 2_390,
    "price_intervals": 39_801,
    "products": 38_343,
    "products_gt_15pct": 991,
}


def float_threshold_intervals(events, threshold=0.01):
    """The AWS logic: DOUBLE comparison, event_time-only ordering."""
    priced = events.where(F.col("product_id").isNotNull() & F.col("price").isNotNull()
                          & (F.col("price") > 0))
    by_time = Window.partitionBy("product_id").orderBy("event_time")
    changed = F.col("prev").isNull() | (F.abs(F.col("price") - F.col("prev")) > threshold)
    running = by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    runs = (priced.withColumn("prev", F.lag("price").over(by_time))
                  .withColumn("chg", F.when(changed, 1).otherwise(0))
                  .withColumn("run_id", F.sum("chg").over(running)))
    starts = runs.groupBy("product_id", "run_id").agg(
        F.min_by("price", "event_time").alias("price"),
        F.min("event_time").alias("effective_start"))
    by_start = Window.partitionBy("product_id").orderBy("effective_start")
    prev_price = F.lag("price").over(by_start)
    return starts.withColumn("pct_change", F.try_divide(F.col("price") - prev_price, prev_price))


def main():
    spark = (SparkSession.builder.master("local[*]").appName("ecomm-local-parity")
             .config("spark.driver.memory", "6g")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "64")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()

    source = Path(os.environ.get("ECOMM_SOURCE_DIR",
                                 ROOT / "data" / "ecommerce-events-history-in-cosmetics-shop"))
    if not list(source.glob("*.csv")):
        sys.exit(f"no CSVs in {source}: set ECOMM_SOURCE_DIR to the folder holding the five Kaggle files")
    events = spark.read.option("header", "true").schema(EVENT_SCHEMA).csv(str(source / "*.csv"))
    replay = replay_window(events, SETTINGS.simulated_days)

    staged = stage_micro_batches(replay, SETTINGS.micro_batch_minutes)
    tmp = Path(tempfile.mkdtemp(prefix="ecomm-parity-")) / "replay.parquet"
    staged.write.mode("overwrite").parquet(str(tmp))  # materialise once, reuse below
    staged = spark.read.parquet(str(tmp))

    got = {
        "events_replayed": staged.count(),
        "micro_batch_intervals": staged.select("dt", "hh", "min5").distinct().count(),
    }
    got["events_excluded"] = got["events_replayed"] - priced_events(staged).count()

    intervals = derive_price_intervals(staged, SETTINGS.noise_threshold_cents).localCheckpoint()
    got["price_intervals"] = intervals.count()
    got["products"] = intervals.select("product_id").distinct().count()
    got["products_gt_15pct"] = products_with_significant_change(intervals, SETTINGS.significant_change_pct)

    legacy = float_threshold_intervals(staged).localCheckpoint()
    legacy_counts = {
        "price_intervals": legacy.count(),
        "products_gt_15pct": products_with_significant_change(legacy, SETTINGS.significant_change_pct),
    }

    print(f"\n{'metric':<24}{'local (cents)':>16}{'AWS':>12}{'diff':>10}")
    for k, aws in AWS.items():
        print(f"{k:<24}{got[k]:>16,}{aws:>12,}{got[k] - aws:>+10,}")
    print("\nfloat-threshold logic re-run locally on the same data:")
    for k, v in legacy_counts.items():
        print(f"  {k:<22}{v:>12,}   cents fix changes this by {got[k] - v:+,}")
    reasons = intervals.groupBy("change_reason").count().collect()
    print("\nchange_reason:", {r["change_reason"]: r["count"] for r in reasons})
    print(f"open-ended intervals: {intervals.where(F.col('effective_end').isNull()).count():,}")
    print(f"replay bounds: {staged.agg(F.min('event_time'), F.max('event_time')).first()}")
    # --- silver: dedup, sessions, temporal join -----------------------------------------
    # Bronze stand-in: the replayed events with the audit columns dedup orders by. Every
    # duplicate sits in one micro-batch file, as it does on the platform.
    bronze = (staged
              .withColumn("_ingested_at", F.lit("2026-10-01 00:00:00").cast("timestamp"))
              .withColumn("_src_file", F.concat_ws("/", "dt", "hh", "min5")))
    sessions = sessionize(deduplicate(bronze), SETTINGS.session_gap_seconds).localCheckpoint()
    per_session = sessions.groupBy("session_key").count()
    got_silver = {
        "rows_after_dedup": sessions.count(),
        "sessions": per_session.count(),
        "median_events_per_session": per_session.agg(F.percentile_approx("count", 0.5)).first()[0],
    }

    closed = close_open_intervals(intervals, sessions)
    broadcast = bind_price_broadcast(sessions, closed).localCheckpoint()
    keys = ["event_time", "user_id", "product_id", "event_type"]
    for name, other in (("asof", bind_price_asof(sessions, intervals)),
                        ("range_join", bind_price_range_join(spark, sessions, closed, 86_400))):
        diff = (broadcast.select(*keys, F.col("catalog_price").alias("a"))
                .join(other.select(*keys, F.col("catalog_price").alias("b")), keys, "full_outer")
                .where(~F.col("a").eqNullSafe(F.col("b"))).count())
        print(f"join {name:<11} disagreements vs broadcast: {diff:,}")

    silver = to_silver_clickstream(broadcast)
    acc = silver.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("catalog_price").isNull().cast("int")).alias("no_catalog_price"),
        F.sum(F.col("price_match").cast("int")).alias("exact"),
        F.sum((F.abs(F.col("price_at_event") - F.col("catalog_price")) <= 0.01).cast("int")).alias("near"),
    ).first()
    got_silver["no_catalog_price"] = acc["no_catalog_price"]
    priced = acc["rows"] - acc["no_catalog_price"]

    print(f"\n{'silver metric':<28}{'local':>12}{'AWS':>12}{'diff':>10}")
    for k, aws in AWS_SILVER.items():
        print(f"{k:<28}{got_silver[k]:>12,}{aws:>12,}{got_silver[k] - aws:>+10,}")
    print(f"price_match exact        {100 * acc['exact'] / priced:.3f}%   (AWS 99.871% of all rows)")
    print(f"price_match within noise {100 * acc['near'] / priced:.3f}%")

    # --- gold: funnel, Q1, Q2 --------------------------------------------------------------
    silver = silver.localCheckpoint()
    funnel = conversion_hourly(silver)
    print(f"\ngold conversion rows        {funnel.count():>10,}   (AWS 952,335)")

    changes = significant_changes(intervals, SETTINGS.significant_change_pct).localCheckpoint()
    measured = elasticity(silver, changes, SETTINGS.post_window_hours, SETTINGS.baseline_days,
                          SETTINGS.q1_min_post_hours).localCheckpoint()
    print(f"Q1 changes > 15%           {changes.count():>10,}   (AWS 1,059)")
    print(f"Q1 changes measurable      {measured.count():>10,}")
    for floor in SETTINGS.q1_floors:
        drops = measured.where((F.col("base_purchases") >= floor) & (F.col("pct_price_change") < 0)
                               & F.col("elasticity").isNotNull())
        s = drops.agg(F.count("*").alias("n"), F.countDistinct("product_id").alias("prod"),
                      F.percentile_approx("elasticity", 0.5).alias("med"),
                      F.avg((F.col("elasticity") < 0).cast("double")).alias("neg")).first()
        med = f"{s['med']:+.3f}" if s["med"] is not None else "n/a"
        neg = f"{100 * s['neg']:.0f}%" if s["n"] else "n/a"
        print(f"Q1 floor {floor:>2}: drops {s['n']:>4} ({s['prod']:>4} products)  "
              f"median {med}  theory sign {neg}")

    increases = changes.where(F.col("pct_price_change") > 0)
    carts = carts_after_increase(silver, increases, SETTINGS.abandonment_window_minutes)
    lost = abandoned_carts(silver, increases, SETTINGS.abandonment_window_minutes).localCheckpoint()
    totals = lost.agg(F.sum("lost_revenue_at_cart"), F.sum("lost_revenue_at_old"),
                      F.countDistinct("session_key"), F.countDistinct("category_id")).first()
    print(f"Q2 increases {increases.count():,} (AWS 227) | carts {carts.count():,} (AWS 93) | "
          f"abandoned {lost.count():,} (AWS 82) | sessions {totals[2]:,} (AWS 58) | categories {totals[3]:,}")
    print(f"Q2 lost revenue at cart {totals[0]} (AWS 495.53) | at old price {totals[1]} (AWS 357.67)")

    print(f"\nelapsed {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
