-- Q3 · The frozen Black Friday numbers next to the live table
--
-- Parameters
--   :catalog     the project catalog, e.g. exam_ecommerce
--   :audit_name  the frozen copy's name, e.g. black_friday_2026_final
--
-- The frozen copy is a DEEP CLONE of gold.product_conversion_hourly at one pinned version
-- (notebooks/60_audit). Run this, change the live table, run it again: the frozen columns
-- never move.

WITH frozen AS (
  SELECT category_id, sum(purchases) AS purchases, sum(revenue) AS revenue
  FROM IDENTIFIER(:catalog || '.audit.product_conversion_hourly_' || :audit_name)
  GROUP BY category_id
),
live AS (
  SELECT category_id, sum(purchases) AS purchases, sum(revenue) AS revenue
  FROM IDENTIFIER(:catalog || '.gold.product_conversion_hourly')
  GROUP BY category_id
)
SELECT coalesce(f.category_id, l.category_id)   AS category_id,
       f.purchases                               AS frozen_purchases,
       f.revenue                                 AS frozen_revenue,
       l.purchases                               AS live_purchases,
       l.revenue                                 AS live_revenue,
       l.purchases - f.purchases                 AS purchases_drift
FROM frozen f
FULL OUTER JOIN live l ON f.category_id = l.category_id
ORDER BY frozen_revenue DESC NULLS LAST
LIMIT 50;
