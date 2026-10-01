"""Gold transformations: the hourly funnel, Q1 elasticity and Q2 cart abandonment.

Step 6a  `conversion_hourly`     funnel per product, hour and catalog price
Step 6b  `significant_changes`   price changes above the threshold, with the original price
         `elasticity`            conversion before vs after each change, normalised
Step 6c  `carts_after_increase`  carts added just after a price increase
         `abandoned_carts`       ...whose session never bought the product

Categories: `category_code` is NULL on 98% of rows in this dataset, so every category-level
answer groups by `category_id` (always present), with `category_code` as a label where known
(decision D-13).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

MONEY = DecimalType(12, 2)
REVENUE = DecimalType(18, 2)


def safe_rate(numerator: Column, denominator: Column) -> Column:
    """numerator / denominator, or 0.0 when the denominator is 0 or NULL. Never NULL.

    A product with views and no carts must give 0.0, not NULL: averages silently drop
    NULL rows, and ANSI mode (on by default) raises on division by zero.
    """
    return (F.when(F.coalesce(denominator, F.lit(0)) > 0,
                   numerator.cast("double") / denominator.cast("double"))
             .otherwise(F.lit(0.0)))


def _count(event_type: str) -> Column:
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).cast("bigint")


def _hours_between(start: str, end: str) -> Column:
    return (F.col(end).cast("long") - F.col(start).cast("long")) / 3600


def _within(time: str, start: str, end: str) -> Column:
    """start <= time < end: the half-open interval every window in this project uses."""
    return (F.col(time) >= F.col(start)) & (F.col(time) < F.col(end))


# --- Step 6a · hourly funnel -----------------------------------------------------------

def conversion_hourly(clicks: DataFrame) -> DataFrame:
    """Views, carts, purchases, rates and revenue per product, hour and catalog price.

    `catalog_price` is part of the grain: an hour in which the price changed produces one row
    per price, so a query can always tell which price the counts were observed at.
    Category columns are product attributes, carried along with max() (non-NULL wins).
    """
    return (clicks
            .groupBy("product_id", F.date_trunc("hour", "event_time").alias("event_hour"), "catalog_price")
            .agg(F.max("category_id").alias("category_id"),
                 F.max("category_code").alias("category_code"),
                 _count("view").alias("views"),
                 _count("cart").alias("carts"),
                 _count("purchase").alias("purchases"),
                 F.coalesce(F.sum(F.when(F.col("event_type") == "purchase", F.col("price_at_event"))),
                            F.lit(0)).cast(REVENUE).alias("revenue"))
            .withColumn("view_to_cart", safe_rate(F.col("carts"), F.col("views")))
            .withColumn("cart_to_purchase", safe_rate(F.col("purchases"), F.col("carts")))
            .withColumn("overall_conv", safe_rate(F.col("purchases"), F.col("views")))
            .select("product_id", "category_id", "category_code", "event_hour", "catalog_price",
                    "views", "carts", "purchases", "view_to_cart", "cart_to_purchase",
                    "overall_conv", "revenue"))


# --- Step 6b · Q1 elasticity -----------------------------------------------------------

def significant_changes(intervals: DataFrame, pct: float) -> DataFrame:
    """Price changes of more than `pct` in either direction.

    Each row carries the *original price* (the previous interval's) and when that original
    price began: the rule that defines the baseline when a product changed price more than once.
    """
    by_start = Window.partitionBy("product_id").orderBy("effective_start")
    return (intervals
            .withColumn("original_price", F.lag("price").over(by_start))
            .withColumn("original_since", F.lag("effective_start").over(by_start))
            .where(F.col("original_price").isNotNull())
            .withColumn("pct_price_change", F.try_divide(
                (F.col("price") - F.col("original_price")).cast("double"),
                F.col("original_price").cast("double")))
            .where(F.abs(F.col("pct_price_change")) > pct)
            .select("product_id", F.col("effective_start").alias("changed_at"), "effective_end",
                    "original_since", "original_price", F.col("price").alias("new_price"),
                    "pct_price_change"))


def elasticity(clicks: DataFrame, changes: DataFrame, post_hours: int, baseline_days: int,
               min_post_hours: float) -> DataFrame:
    """Purchases and conversion before vs after each change, both normalised to 2-hour blocks.

    Windows, at event precision:
      baseline  [max(original_since, changed_at - baseline_days), changed_at)
                only the original price's own interval, so a second, earlier change inside
                the 7 days cannot contaminate it (the brief's "original price is undefined" trap)
      post      [changed_at, min(changed_at + post_hours, next change, end of data))
                only while the new price holds
    Each window's purchases are divided by its real length in 2-hour blocks: 7 full days is
    84 blocks, but a price that began 3 days before the change gives a 36-block baseline.
    """
    data_end = clicks.agg(F.max("event_time").alias("_data_end"))
    windows = (changes.crossJoin(F.broadcast(data_end))
               .withColumn("base_start", F.greatest(
                   "original_since", F.col("changed_at") - F.expr(f"INTERVAL {baseline_days} DAYS")))
               .withColumn("post_end", F.least(
                   F.col("changed_at") + F.expr(f"INTERVAL {post_hours} HOURS"),
                   F.coalesce("effective_end", "_data_end"), "_data_end"))
               .withColumn("base_hours", _hours_between("base_start", "changed_at"))
               .withColumn("post_hours", _hours_between("changed_at", "post_end"))
               .where(F.col("post_hours") >= min_post_hours)
               .drop("_data_end"))

    c = clicks.alias("c")
    w = windows.alias("w")
    in_base = _within("c.event_time", "w.base_start", "w.changed_at")
    in_post = _within("c.event_time", "w.changed_at", "w.post_end")

    def counted(window: Column, event_type: str) -> Column:
        return F.sum(F.when(window & (F.col("c.event_type") == event_type), 1).otherwise(0)).cast("bigint")

    per_change = (w.join(c, (F.col("c.product_id") == F.col("w.product_id")) & (in_base | in_post), "left")
                  .groupBy("w.product_id", "w.changed_at")
                  .agg(F.first("w.original_price").alias("original_price"),
                       F.first("w.new_price").alias("new_price"),
                       F.first("w.pct_price_change").alias("pct_price_change"),
                       F.first("w.base_hours").alias("base_hours"),
                       F.first("w.post_hours").alias("post_hours"),
                       F.max("c.category_id").alias("category_id"),
                       F.max("c.category_code").alias("category_code"),
                       counted(in_base, "view").alias("base_views"),
                       counted(in_base, "purchase").alias("base_purchases"),
                       counted(in_post, "view").alias("post_views"),
                       counted(in_post, "purchase").alias("post_purchases")))

    # try_divide: a zero-length baseline (two prices at the same instant) yields NULL, not an error.
    blocks = float(post_hours)
    return (per_change
            .withColumn("base_purchases_per_2h",
                        F.try_divide(F.col("base_purchases").cast("double"), F.col("base_hours") / blocks))
            .withColumn("post_purchases_per_2h",
                        F.try_divide(F.col("post_purchases").cast("double"), F.col("post_hours") / blocks))
            .withColumn("base_conv", safe_rate(F.col("base_purchases"), F.col("base_views")))
            .withColumn("post_conv", safe_rate(F.col("post_purchases"), F.col("post_views")))
            .withColumn("pct_change_purchases", F.try_divide(
                F.col("post_purchases_per_2h") - F.col("base_purchases_per_2h"),
                F.col("base_purchases_per_2h")))
            .withColumn("elasticity", F.try_divide(F.col("pct_change_purchases"), F.col("pct_price_change")))
            .select("product_id", "category_id", "category_code", "changed_at",
                    F.col("original_price").cast(MONEY).alias("original_price"),
                    F.col("new_price").cast(MONEY).alias("new_price"),
                    "pct_price_change", "base_hours", "post_hours",
                    "base_views", "base_purchases", "post_views", "post_purchases",
                    "base_purchases_per_2h", "post_purchases_per_2h",
                    "base_conv", "post_conv", "pct_change_purchases", "elasticity"))


# --- Step 6c · Q2 cart abandonment -----------------------------------------------------

def carts_after_increase(clicks: DataFrame, increases: DataFrame, window_minutes: int) -> DataFrame:
    """Cart events within `window_minutes` after a price increase of the same product."""
    windows = increases.withColumn(
        "window_end", F.col("changed_at") + F.expr(f"INTERVAL {window_minutes} MINUTES"))
    return (clicks.where(F.col("event_type") == "cart").alias("s")
            .join(windows.alias("i"),
                  (F.col("s.product_id") == F.col("i.product_id"))
                  & _within("s.event_time", "i.changed_at", "i.window_end"))
            .select("s.product_id", "s.category_id", "s.category_code", "s.session_key",
                    F.col("s.event_time").alias("carted_at"), "i.changed_at",
                    F.col("s.catalog_price").alias("price_at_cart"),
                    F.col("i.original_price").alias("price_before_increase"),
                    "i.pct_price_change"))


def abandoned_carts(clicks: DataFrame, increases: DataFrame, window_minutes: int) -> DataFrame:
    """Carts added within `window_minutes` after a price increase, whose session never bought
    that product. One row per abandoned cart event, priced both ways:

      lost_revenue_at_cart  the price shown when the item was carted (the new, higher price)
      lost_revenue_at_old   the price before the increase: what the sale was worth at the old price
    """
    purchased = (clicks.where(F.col("event_type") == "purchase")
                 .select("session_key", "product_id").distinct())
    return (carts_after_increase(clicks, increases, window_minutes)
            .join(purchased, ["session_key", "product_id"], "left_anti")
            .withColumn("lost_revenue_at_cart", F.col("price_at_cart").cast(MONEY))
            .withColumn("lost_revenue_at_old", F.col("price_before_increase").cast(MONEY)))
