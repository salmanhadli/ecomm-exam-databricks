"""Silver rules (ecomm.transforms.silver): dedup, sessions, and the three temporal joins.

The join tests use the edge cases that break naive implementations: a click before the
first price, a click at the exact instant a price changes, and a click in the open interval.
Locally, OSS Spark ignores the Databricks-only RANGE_JOIN hint, so the range-join test checks
the query's *result*; the optimisation itself only shows on Databricks.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecomm.transforms.silver import (
    bind_price_asof,
    bind_price_broadcast,
    bind_price_range_join,
    close_open_intervals,
    deduplicate,
    latest_intervals,
    sessionize,
)

from conftest import ts

EVENT_COLS = ("event_time timestamp, event_type string, product_id string, user_id string, "
              "price double, user_session string, category_id string, category_code string, "
              "brand string, _ingested_at timestamp, _src_file string")


def events(spark, rows):
    """rows: (time, type, product, user, price, user_session)."""
    return spark.createDataFrame(
        [(ts(t), e, p, u, price, s, "c1", None, None, ts("2026-10-01 00:00:00"), "f1.json")
         for t, e, p, u, price, s in rows], EVENT_COLS)


# --- dedup -------------------------------------------------------------------------------

def test_dedup_keeps_one_row_and_the_same_one_whatever_the_order(spark):
    rows = [("2019-10-01 10:00:00", "view", "p", "u", 5.0, "s-b"),
            ("2019-10-01 10:00:00", "view", "p", "u", 5.0, "s-a")]   # same key, payload differs
    first = deduplicate(events(spark, rows)).collect()
    second = deduplicate(events(spark, list(reversed(rows)))).collect()
    assert len(first) == 1 and first == second


# --- sessions ------------------------------------------------------------------------------

def test_thirty_minutes_idle_keeps_the_session_one_second_more_starts_a_new_one(spark):
    rows = [("2019-10-01 10:00:00", "view", "p", "u", 1.0, None),
            ("2019-10-01 10:30:00", "view", "p", "u", 1.0, None),    # gap 1800 s: same session
            ("2019-10-01 11:00:01", "view", "p", "u", 1.0, None)]    # gap 1801 s: new session
    keys = [r.session_key for r in sessionize(events(spark, rows), 1800).orderBy("event_time").collect()]
    assert keys == ["u#1", "u#1", "u#2"]


def test_sessions_are_per_user(spark):
    rows = [("2019-10-01 10:00:00", "view", "p", "a", 1.0, None),
            ("2019-10-01 10:00:00", "view", "p", "b", 1.0, None)]
    keys = sorted(r.session_key for r in sessionize(events(spark, rows), 1800).collect())
    assert keys == ["a#1", "b#1"]


# --- price intervals ---------------------------------------------------------------------

def test_latest_ingested_version_of_an_interval_wins(spark):
    catalog = spark.createDataFrame(
        [("p", 5.0, ts("2019-10-01 00:00:00"), None, "initial", None, ts("2026-10-01 01:00:00"), "old"),
         ("p", 6.0, ts("2019-10-01 00:00:00"), None, "initial", None, ts("2026-10-01 02:00:00"), "new")],
        "product_id string, price double, effective_start timestamp, effective_end timestamp, "
        "change_reason string, pct_change double, _ingested_at timestamp, _src_file string")
    (row,) = latest_intervals(catalog).collect()
    assert row.price == Decimal("6.00")


# --- temporal join -----------------------------------------------------------------------

@pytest.fixture
def join_inputs(spark):
    intervals = spark.createDataFrame(
        [("p", Decimal("10.00"), ts("2019-10-01 10:00:00"), ts("2019-10-01 12:00:00")),
         ("p", Decimal("8.00"), ts("2019-10-01 12:00:00"), None)],          # open, newest
        "product_id string, price decimal(12,2), effective_start timestamp, effective_end timestamp")
    clicks = events(spark, [
        ("2019-10-01 09:00:00", "view", "p", "u", 10.0, None),    # before the first price -> NULL
        ("2019-10-01 11:59:59", "view", "p", "u", 10.0, None),    # last second of 10.00
        ("2019-10-01 12:00:00", "cart", "p", "u", 8.0, None),     # the instant the price changes
        ("2019-10-03 00:00:00", "purchase", "p", "u", 8.0, None),  # inside the open interval
    ])
    return clicks, intervals


EXPECTED = [None, Decimal("10.00"), Decimal("8.00"), Decimal("8.00")]


def prices(df):
    return [r.catalog_price for r in df.orderBy("event_time").collect()]


def test_broadcast_join_binds_the_valid_price(spark, join_inputs):
    clicks, intervals = join_inputs
    assert prices(bind_price_broadcast(clicks, close_open_intervals(intervals, clicks))) == EXPECTED


def test_range_join_query_binds_the_valid_price(spark, join_inputs):
    clicks, intervals = join_inputs
    closed = close_open_intervals(intervals, clicks)
    assert prices(bind_price_range_join(spark, clicks, closed, bin_seconds=3600)) == EXPECTED


def test_asof_join_binds_the_valid_price(spark, join_inputs):
    clicks, intervals = join_inputs
    assert prices(bind_price_asof(clicks, intervals)) == EXPECTED


def test_open_interval_is_closed_after_the_last_click(spark, join_inputs):
    clicks, intervals = join_inputs
    closed = close_open_intervals(intervals, clicks).where(F.col("effective_end").isNull()).first()
    assert closed.effective_end_x == ts("2019-10-04 00:00:00")      # last click + 1 day
