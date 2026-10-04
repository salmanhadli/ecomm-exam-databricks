"""Performance helpers (ecomm.perf): they must work on real data sizes under ANSI mode."""

from __future__ import annotations

from pyspark.sql import functions as F

from ecomm import perf


def test_full_compute_survives_millions_of_hashes_under_ansi(spark):
    # Regression: summing 64-bit hashes overflowed BIGINT on the 1.85M-row silver join
    # (ARITHMETIC_OVERFLOW on the first workspace run). 200k rows overflow a sum reliably.
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    df = spark.range(200_000).withColumn("x", F.col("id") * 7)
    assert perf.time_full_compute(df) >= 0


def test_operators_reads_join_names_from_a_formatted_plan(spark):
    left = spark.range(100).withColumnRenamed("id", "k")
    right = spark.range(10).withColumnRenamed("id", "k")
    plan = perf.physical_plan(left.join(F.broadcast(right), "k"))
    assert any("BroadcastHashJoin" in name for name in perf.operators(plan))


def test_partition_skew_spots_one_hot_key(spark):
    df = spark.range(10_000).withColumn("key", F.when(F.col("id") < 9_000, F.lit("hot"))
                                         .otherwise(F.col("id").cast("string")))
    skew = perf.partition_skew(df, "key", partitions=8)
    assert skew["max_rows"] >= 9_000 and skew["ratio"] > 10
