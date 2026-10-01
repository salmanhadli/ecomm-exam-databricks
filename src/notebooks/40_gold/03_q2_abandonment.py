# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · Q2: cart abandonment after a price increase
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_40_gold` → `q2_abandonment` |
# MAGIC | **Reads** | `silver.price_intervals` · `silver.clickstream` |
# MAGIC | **Writes** | `gold.cart_abandonment`: one row per abandoned cart, priced both ways |
# MAGIC | **Brief** | Q2 · step 6c · acceptance **A10** · evidence item 16 |
# MAGIC
# MAGIC > *Which sessions added an item to cart within 30 minutes of a price increase and then ended
# MAGIC > without purchasing, and what is the total lost revenue per category?*
# MAGIC
# MAGIC **The rules**
# MAGIC - **Increase:** a change above +15%, from `silver.price_intervals`.
# MAGIC - **Cart:** a `cart` event in `[changed_at, changed_at + 30 min)` for that product.
# MAGIC - **Abandoned:** the same session (our own 30-minute sessions) never purchased that product.
# MAGIC - **Category:** `category_id`, labelled with `category_code` where known. `category_code` is
# MAGIC   NULL on 98% of rows, and grouping by it would collapse the answer into one row (decision D-13).
# MAGIC
# MAGIC **Two definitions of lost revenue** (the brief: pick one, justify it, report the other)
# MAGIC
# MAGIC | | Definition | Reading |
# MAGIC |---|---|---|
# MAGIC | **Headline** | `lost_revenue_at_cart`: the price shown when the item was carted, i.e. the new, higher price | the revenue the shopper walked away from |
# MAGIC | Sensitivity | `lost_revenue_at_old`: the same sale at the pre-increase price | what those carts were worth before the increase |
# MAGIC
# MAGIC The gap between them is the extra revenue per sale the increase was meant to earn. If
# MAGIC abandonment after increases is high, that gap is the case *against* the price rise.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark.sql import functions as F  # noqa: E402

from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.settings import SETTINGS  # noqa: E402
from ecomm.tables import GOLD_CART_ABANDONMENT, SILVER_CLICKSTREAM, SILVER_PRICE_INTERVALS  # noqa: E402
from ecomm.transforms.gold import abandoned_carts, carts_after_increase, significant_changes  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="q2_abandonment")

# COMMAND ----------

# MAGIC %md ## 2 · Build and write

# COMMAND ----------

clicks = spark.read.table(SILVER_CLICKSTREAM.fqn(project))
increases = (significant_changes(spark.read.table(SILVER_PRICE_INTERVALS.fqn(project)),
                                 SETTINGS.significant_change_pct)
             .where(F.col("pct_price_change") > 0))
window = SETTINGS.abandonment_window_minutes

abandoned = abandoned_carts(clicks, increases, window).withColumn("computed_at", F.current_timestamp())
GOLD_CART_ABANDONMENT.overwrite(spark, project, abandoned)

# COMMAND ----------

# MAGIC %md ## 3 · Measure: the funnel from increase to abandonment

# COMMAND ----------

gold = spark.read.table(GOLD_CART_ABANDONMENT.fqn(project))

ev.record("q2_price_increases_gt_15pct", increases.count())
ev.record("q2_carts_within_window", carts_after_increase(clicks, increases, window).count(),
          f"{window} minutes after an increase")
ev.record("q2_abandoned_carts", gold.count())
ev.record("q2_abandoned_sessions", gold.select("session_key").distinct().count())

totals = gold.agg(F.sum("lost_revenue_at_cart").alias("at_cart"),
                  F.sum("lost_revenue_at_old").alias("at_old")).first()
ev.record("q2_lost_revenue_at_cart_price", str(totals["at_cart"] or 0), "headline")
ev.record("q2_lost_revenue_at_old_price", str(totals["at_old"] or 0), "sensitivity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Lost revenue per category, both definitions

# COMMAND ----------

by_category = (gold
               .groupBy("category_id")
               .agg(F.max("category_code").alias("category_code"),
                    F.countDistinct("session_key").alias("abandoned_sessions"),
                    F.sum("lost_revenue_at_cart").alias("lost_at_cart_price"),
                    F.sum("lost_revenue_at_old").alias("lost_at_old_price"))
               .withColumn("price_rise_gap", F.col("lost_at_cart_price") - F.col("lost_at_old_price"))
               .orderBy(F.desc("lost_at_cart_price"), "category_id"))
display(by_category)

ev.record("q2_categories_affected", by_category.count())
for rank, row in enumerate(by_category.limit(3).collect(), 1):
    label = row["category_code"] or "no category_code"
    ev.record(f"q2_top{rank}_category", row["category_id"],
              f"{label}: {row['abandoned_sessions']} sessions, "
              f"{row['lost_at_cart_price']} at cart price vs {row['lost_at_old_price']} at old price")

ev.record("acceptance_A10", "PASS", "both definitions reported per category; headline = at cart price")
ev.flush()
