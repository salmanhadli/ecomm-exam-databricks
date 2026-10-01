"""Behaviour of the harness transformations (ecomm.transforms.harness), on tiny hand-built inputs.

Each test pins one rule the brief asks us to decide and defend.
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import functions as F

from ecomm.schemas import EVENT_SCHEMA
from ecomm.transforms.harness import (
    derive_price_intervals,
    priced_events,
    products_with_significant_change,
    replay_window,
    stage_micro_batches,
)

from conftest import ts


def events(spark, rows):
    """rows: (event_time, product_id, price) -> a DataFrame with the full event schema."""
    full = [(ts(t) if t else None, "view", p, "c1", None, None, price, "u1", "s1") for t, p, price in rows]
    return spark.createDataFrame(full, EVENT_SCHEMA)


def intervals_of(spark, rows, noise_cents=1):
    df = derive_price_intervals(events(spark, rows), noise_cents)
    return [r.asDict() for r in df.orderBy("product_id", "effective_start").collect()]


# --- Stream A --------------------------------------------------------------------------

def test_replay_window_keeps_first_n_days_exclusive_of_the_bound(spark):
    df = events(spark, [("2019-10-01 04:00:00", "p", 1.0),
                        ("2019-10-02 03:59:59", "p", 1.0),
                        ("2019-10-02 04:00:00", "p", 1.0),   # exactly min + 1 day: excluded
                        (None, "p", 1.0)])                   # NULL event_time: dropped
    kept = replay_window(df, days=1).select(F.date_format("event_time", "yyyy-MM-dd HH:mm:ss")).collect()
    assert [r[0] for r in kept] == ["2019-10-01 04:00:00", "2019-10-02 03:59:59"]


def test_micro_batch_folder_is_the_floor_of_the_five_minute_interval(spark):
    df = stage_micro_batches(events(spark, [("2019-10-01 10:07:59", "p", 1.0),
                                            ("2019-10-01 10:10:00", "p", 1.0)]), minutes=5)
    assert [(r.dt, r.hh, r.min5) for r in df.orderBy("event_time").collect()] == [
        ("2019-10-01", "10", "05"),
        ("2019-10-01", "10", "10"),
    ]


# --- Stream B --------------------------------------------------------------------------

def test_one_cent_step_is_noise_for_prices_where_double_arithmetic_disagrees(spark):
    # In DOUBLE, 100.00 - 99.99 > 0.01 but 10.00 - 9.99 < 0.01; in cents both are exactly 1.
    rows = [("2019-10-01 10:00:00", "a", 100.00), ("2019-10-01 11:00:00", "a", 99.99),
            ("2019-10-01 10:00:00", "b", 10.00), ("2019-10-01 11:00:00", "b", 9.99)]
    assert [(r["product_id"], r["price"]) for r in intervals_of(spark, rows)] == [
        ("a", Decimal("100.00")), ("b", Decimal("10.00"))]


def test_two_cent_step_is_a_new_interval_closing_the_previous_one(spark):
    rows = [("2019-10-01 10:00:00", "a", 10.00), ("2019-10-01 12:00:00", "a", 9.98)]
    first, second = intervals_of(spark, rows)
    assert first["effective_end"] == second["effective_start"] == ts("2019-10-01 12:00:00")
    assert (first["change_reason"], second["change_reason"]) == ("initial", "decrease")
    assert second["effective_end"] is None          # newest interval stays open


def test_interval_is_priced_at_its_opening_price_not_its_minimum(spark):
    # one-cent drift inside a run: 10.00 -> 9.99 -> 9.98 is one run priced 10.00
    rows = [("2019-10-01 10:00:00", "a", 10.00), ("2019-10-01 11:00:00", "a", 9.99),
            ("2019-10-01 12:00:00", "a", 9.98)]
    (only,) = intervals_of(spark, rows)
    assert only["price"] == Decimal("10.00")


def test_equal_timestamps_resolve_the_same_way_whatever_the_input_order(spark):
    rows = [("2019-10-01 10:00:00", "a", 5.00), ("2019-10-01 10:00:00", "a", 9.00),
            ("2019-10-01 11:00:00", "a", 9.00)]
    assert intervals_of(spark, rows) == intervals_of(spark, list(reversed(rows)))


def test_zero_and_null_prices_are_not_catalog_prices(spark):
    df = events(spark, [("2019-10-01 10:00:00", "a", 0.0), ("2019-10-01 10:00:00", "a", None),
                        ("2019-10-01 10:00:00", "a", 3.5)])
    assert priced_events(df).count() == 1


def test_significant_change_counts_products_not_intervals(spark):
    rows = [("2019-10-01 10:00:00", "a", 10.00), ("2019-10-01 11:00:00", "a", 8.00),   # -20%
            ("2019-10-01 12:00:00", "a", 10.00),                                       # +25%
            ("2019-10-01 10:00:00", "b", 10.00), ("2019-10-01 11:00:00", "b", 9.50)]   # -5%
    df = derive_price_intervals(events(spark, rows), noise_threshold_cents=1)
    assert products_with_significant_change(df, pct=0.15) == 1
