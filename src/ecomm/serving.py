"""The serving layer's SQL: one business question, and the materialized view that precomputes it.

The question, from the brief's step 9: conversion and revenue per category per day. Price is
never averaged across products ("AVG(price) … averages a $2,000 laptop with a $9 cable"):
revenue per purchase weights every price by the units sold.

The view avoids COUNT(DISTINCT …) and groups by the same expressions it selects, so it stays
eligible for incremental refresh (a lesson from the AWS build's Redshift MV).
"""

from __future__ import annotations

from ecomm.project import Project

MV_NAME = "mv_category_daily"


def category_daily_sql(source: str) -> str:
    """Conversion and revenue per category and day, computed from the hourly funnel table."""
    return f"""
        SELECT category_id,
               date_trunc('DAY', event_hour)              AS business_date,
               max(category_code)                         AS category_code,
               count(*)                                   AS product_hours,
               sum(views)                                 AS views,
               sum(carts)                                 AS carts,
               sum(purchases)                             AS purchases,
               sum(revenue)                               AS revenue,
               try_divide(sum(carts), sum(views))         AS view_to_cart,
               try_divide(sum(purchases), sum(carts))     AS cart_to_purchase,
               try_divide(sum(revenue), sum(purchases))   AS revenue_per_purchase
        FROM {source}
        GROUP BY category_id, date_trunc('DAY', event_hour)"""


def mv_fqn(project: Project) -> str:
    return project.table("gold", MV_NAME)


def mv_ddl(project: Project, source: str) -> str:
    """Create the materialized view once; later runs only REFRESH it."""
    return (f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_fqn(project)}\n"
            f"COMMENT 'Conversion and revenue per category per day, precomputed from {source}'\n"
            f"AS {category_daily_sql(source)}")
