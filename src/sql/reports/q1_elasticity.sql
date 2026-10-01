-- Q1 · Price elasticity after a price DROP of more than 15%
--
-- Open in the Databricks SQL editor and fill in the parameters:
--   :catalog  the project catalog, e.g. exam_ecommerce
--   :floor    minimum baseline purchases (the brief suggests 30; the headline is 5)
--
-- Windows are normalised to 2-hour blocks by their real length (decision D-19). Read the
-- result with the confounders in notebooks/40_gold/02_q1_elasticity: most post windows hold
-- no purchase, and then elasticity is exactly -1 / price change.

SELECT product_id,
       category_id,
       category_code,
       changed_at,
       original_price,
       new_price,
       round(pct_price_change * 100, 1)          AS price_change_pct,
       base_purchases,
       round(base_purchases_per_2h, 3)           AS base_purchases_per_2h,
       post_purchases,
       round(post_purchases_per_2h, 3)           AS post_purchases_per_2h,
       round(base_conv, 4)                       AS base_conv,
       round(post_conv, 4)                       AS post_conv,
       round(elasticity, 3)                      AS elasticity
FROM IDENTIFIER(:catalog || '.gold.price_elasticity')
WHERE pct_price_change < -0.15
  AND base_purchases >= :floor
ORDER BY elasticity ASC
LIMIT 50;
