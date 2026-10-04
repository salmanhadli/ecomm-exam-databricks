"""Serving SQL and Q3 audit helpers (ecomm.serving, ecomm.audit)."""

from __future__ import annotations

from decimal import Decimal

from ecomm import audit
from ecomm.project import Project, audit_table_name
from ecomm.serving import category_daily_sql, mv_ddl

from conftest import ts

GOLD_COLS = ("product_id string, category_id string, category_code string, event_hour timestamp, "
             "views bigint, carts bigint, purchases bigint, revenue decimal(18,2)")


def gold(spark, rows):
    return spark.createDataFrame(rows, GOLD_COLS)


def test_category_daily_query_runs_and_weights_price_by_units(spark):
    gold(spark, [
        ("a", "c1", None, ts("2019-10-01 10:00:00"), 10, 2, 1, Decimal("2000.00")),   # one laptop
        ("b", "c1", None, ts("2019-10-01 11:00:00"), 10, 5, 9, Decimal("81.00")),     # nine cables
    ]).createOrReplaceTempView("gold_hourly")
    (row,) = spark.sql(category_daily_sql("gold_hourly")).collect()
    assert (row.views, row.carts, row.purchases) == (20, 7, 10)
    assert row.revenue_per_purchase == Decimal("208.1")                   # 2081 / 10 units


def test_a_cache_busting_filter_keeps_the_answer_identical(spark):
    gold(spark, [("a", "c1", None, ts("2019-10-01 10:00:00"), 10, 2, 1, Decimal("5.00"))]
         ).createOrReplaceTempView("gold_hourly")
    plain = spark.sql(category_daily_sql("gold_hourly")).collect()
    busted = spark.sql(category_daily_sql("gold_hourly", where="'run 42' IS NOT NULL")).collect()
    assert plain == busted


def test_the_view_stays_eligible_for_incremental_refresh():
    ddl = mv_ddl(Project("exam_ecommerce"), "exam_ecommerce.gold.product_conversion_hourly")
    assert "DISTINCT" not in ddl.upper()
    assert ddl.startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS exam_ecommerce.gold.mv_category_daily")


def test_readings_compare_exactly_and_track_the_top_category(spark):
    df = gold(spark, [("a", "c1", None, ts("2019-10-01 10:00:00"), 5, 2, 1, Decimal("10.00")),
                      ("b", "c2", None, ts("2019-10-01 10:00:00"), 5, 2, 3, Decimal("30.00"))])
    assert audit.top_category(df) == "c2"
    assert audit.reading(df, "c2") == (2, 4, "40.00", 3)
    assert audit.reading(df, "c2") == audit.reading(df, "c2")


def test_frozen_copy_names_are_plain_identifiers():
    assert audit_table_name("black_friday_2026_final") == "product_conversion_hourly_black_friday_2026_final"
    try:
        audit_table_name("black-friday")
    except ValueError:
        pass
    else:
        raise AssertionError("a hyphenated audit name must be rejected")
