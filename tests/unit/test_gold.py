"""Gold rules (ecomm.transforms.gold): safe rates, the hourly funnel, Q1 windows and Q2 abandonment."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecomm.transforms.gold import (
    abandoned_carts,
    conversion_hourly,
    elasticity,
    safe_rate,
    significant_changes,
)

from conftest import ts

CLICK_COLS = ("event_time timestamp, event_type string, product_id string, category_id string, "
              "category_code string, price_at_event decimal(12,2), catalog_price decimal(12,2), "
              "user_id string, session_key string")
INTERVAL_COLS = ("product_id string, price decimal(12,2), effective_start timestamp, "
                 "effective_end timestamp")


def clicks(spark, rows):
    """rows: (time, event_type, price, session_key) for product 'p' in category 'c1'."""
    return spark.createDataFrame(
        [(ts(t), e, "p", "c1", None, Decimal(str(price)), Decimal(str(price)), "u", s)
         for t, e, price, s in rows], CLICK_COLS)


def intervals(spark, rows):
    """rows: (price, start, end|None) for product 'p'."""
    return spark.createDataFrame(
        [("p", Decimal(str(price)), ts(start), ts(end) if end else None) for price, start, end in rows],
        INTERVAL_COLS)


# --- 6a ------------------------------------------------------------------------------------

@pytest.mark.parametrize("num, den, expected", [(1, 4, 0.25), (3, 0, 0.0), (3, None, 0.0)])
def test_safe_rate_is_zero_not_null_without_a_denominator(spark, num, den, expected):
    df = spark.createDataFrame([(num, den)], "n int, d int")
    assert df.select(safe_rate(F.col("n"), F.col("d"))).first()[0] == expected


def test_hourly_funnel_counts_and_splits_an_hour_by_price(spark):
    df = conversion_hourly(clicks(spark, [
        ("2019-10-01 10:05:00", "view", 10.00, "s"),
        ("2019-10-01 10:10:00", "cart", 10.00, "s"),
        ("2019-10-01 10:20:00", "purchase", 10.00, "s"),
        ("2019-10-01 10:40:00", "view", 8.00, "s"),        # price changed mid-hour
    ])).orderBy(F.desc("catalog_price")).collect()
    assert [(r.catalog_price, r.views, r.carts, r.purchases) for r in df] == [
        (Decimal("10.00"), 1, 1, 1), (Decimal("8.00"), 1, 0, 0)]
    assert df[0].revenue == Decimal("10.00") and df[1].view_to_cart == 0.0


# --- 6b · Q1 -------------------------------------------------------------------------------

def test_only_changes_above_the_threshold_count_with_their_original_price(spark):
    changes = significant_changes(intervals(spark, [
        (10.00, "2019-10-01 00:00:00", "2019-10-03 00:00:00"),
        (8.00, "2019-10-03 00:00:00", "2019-10-05 00:00:00"),     # -20%: counts
        (7.80, "2019-10-05 00:00:00", None),                     # -2.5%: noise
    ]), pct=0.15).collect()
    assert [(c.original_price, c.new_price, round(c.pct_price_change, 2)) for c in changes] == [
        (Decimal("10.00"), Decimal("8.00"), -0.2)]
    assert changes[0].original_since == ts("2019-10-01 00:00:00")


def test_baseline_uses_only_the_original_price_and_post_stops_at_the_next_change(spark):
    price_history = intervals(spark, [
        (12.00, "2019-10-01 00:00:00", "2019-10-03 00:00:00"),   # an earlier price...
        (10.00, "2019-10-03 00:00:00", "2019-10-06 00:00:00"),   # ...the original price, 3 days
        (8.00, "2019-10-06 00:00:00", "2019-10-06 01:30:00"),    # the change; next one after 1.5 h
        (9.00, "2019-10-06 01:30:00", None),
    ])
    events = clicks(spark, [
        ("2019-10-02 12:00:00", "purchase", 12.00, "a"),   # before the original price: excluded
        ("2019-10-04 12:00:00", "purchase", 10.00, "b"),   # baseline
        ("2019-10-05 12:00:00", "purchase", 10.00, "c"),   # baseline
        ("2019-10-06 00:30:00", "purchase", 8.00, "d"),    # post
        ("2019-10-06 01:45:00", "purchase", 9.00, "e"),    # after the next change: excluded
        ("2019-10-07 00:00:00", "view", 9.00, "f"),        # data continues past the post window
    ])
    changes = significant_changes(price_history, pct=0.15).where(F.col("new_price") == 8)
    (row,) = elasticity(events, changes, post_hours=2, baseline_days=7, min_post_hours=1.0).collect()

    assert (row.base_hours, row.base_purchases) == (72.0, 2)        # 3 days, 2 purchases
    assert (row.post_hours, row.post_purchases) == (1.5, 1)         # cut at the next change
    assert row.base_purchases_per_2h == pytest.approx(2 / 36)       # 72 h = 36 two-hour blocks
    assert row.post_purchases_per_2h == pytest.approx(1 / 0.75)     # 1.5 h = 0.75 blocks


def test_a_post_window_shorter_than_the_minimum_is_not_measured(spark):
    price_history = intervals(spark, [
        (10.00, "2019-10-01 00:00:00", "2019-10-02 00:00:00"),
        (8.00, "2019-10-02 00:00:00", "2019-10-02 00:20:00"),    # only 20 minutes at the new price
        (10.00, "2019-10-02 00:20:00", None),
    ])
    events = clicks(spark, [("2019-10-01 12:00:00", "purchase", 10.00, "a"),
                            ("2019-10-03 00:00:00", "view", 10.00, "b")])
    changes = significant_changes(price_history, pct=0.15).where(F.col("new_price") == 8)
    assert elasticity(events, changes, 2, 7, min_post_hours=1.0).count() == 0


# --- 6c · Q2 -------------------------------------------------------------------------------

def test_abandoned_cart_rules_and_both_revenue_definitions(spark):
    increase = significant_changes(intervals(spark, [
        (10.00, "2019-10-01 00:00:00", "2019-10-01 12:00:00"),
        (12.00, "2019-10-01 12:00:00", None),                    # +20%
    ]), pct=0.15)
    events = clicks(spark, [
        ("2019-10-01 12:10:00", "cart", 12.00, "s1"),            # abandoned
        ("2019-10-01 12:20:00", "cart", 12.00, "s2"),            # bought later: not abandoned
        ("2019-10-01 13:00:00", "purchase", 12.00, "s2"),
        ("2019-10-01 12:31:00", "cart", 12.00, "s3"),            # 31 min after: outside the window
    ])
    rows = abandoned_carts(events, increase, window_minutes=30).collect()
    assert [(r.session_key, r.lost_revenue_at_cart, r.lost_revenue_at_old) for r in rows] == [
        ("s1", Decimal("12.00"), Decimal("10.00"))]
