-- Q2 · Lost revenue per category from carts abandoned within 30 minutes of a price increase
--
-- Parameter  :catalog  the project catalog, e.g. exam_ecommerce
--
-- Both definitions (decision D-13 groups by category_id; category_code is 98% NULL):
--   lost_at_cart_price  headline: the price the shopper walked away from
--   lost_at_old_price   sensitivity: the same sales at the pre-increase price
-- The gap is the extra revenue per sale the increase was meant to earn.

SELECT category_id,
       max(category_code)                                       AS category_code,
       count(DISTINCT session_key)                              AS abandoned_sessions,
       count(*)                                                 AS abandoned_carts,
       sum(lost_revenue_at_cart)                                AS lost_at_cart_price,
       sum(lost_revenue_at_old)                                 AS lost_at_old_price,
       sum(lost_revenue_at_cart) - sum(lost_revenue_at_old)     AS price_rise_gap
FROM IDENTIFIER(:catalog || '.gold.cart_abandonment')
GROUP BY category_id
ORDER BY lost_at_cart_price DESC, category_id;
