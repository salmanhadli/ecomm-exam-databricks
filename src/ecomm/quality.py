"""Data-quality rules, expressed as Lakeflow pipeline expectations.

An expectation is a named SQL condition evaluated on every row. What happens on a
violation is chosen per rule:

    warn   the row is kept; pass/fail counts are logged in the pipeline event log
    drop   the row is removed before it is written
    fail   the whole update stops

**Bronze is lossless** (decision D-15): the brief's "known dirt" is measured with warn rules,
never dropped, because bronze is the replayable record of what arrived. Silver decides what to
filter. Only a broken *contract* with the harness fails an update: a file outside the
dt=/hh=/min5= layout, or a price-catalog row without a product.

Keeping the rules here, not inline in the pipeline, puts them in one reviewable place and
lets tests/unit check that every condition is valid SQL.
"""

from __future__ import annotations

# --- bronze.clickstream -------------------------------------------------------------

CLICKSTREAM_WARN: dict[str, str] = {
    # Rows silver cannot use; counted here, filtered in silver.
    "event_time_present": "event_time IS NOT NULL",
    "product_present": "product_id IS NOT NULL",
    "user_present": "user_id IS NOT NULL",
    "known_event_type": "event_type IN ('view', 'cart', 'remove_from_cart', 'purchase')",
    "positive_price": "price > 0",
    # The brief's "known dirt": measured, reported, handled downstream.
    "category_code_present": "category_code IS NOT NULL",
    "brand_present": "brand IS NOT NULL",
    "user_session_present": "user_session IS NOT NULL",
    # Anything Auto Loader could not parse into the declared schema.
    "nothing_rescued": "_rescued_data IS NULL",
}

CLICKSTREAM_FAIL: dict[str, str] = {
    # Every clickstream file must come from the harness's dt=/hh=/min5= layout.
    "file_in_micro_batch_layout": "_batch_id IS NOT NULL",
}

# --- bronze.price_catalog -------------------------------------------------------------

PRICE_CATALOG_WARN: dict[str, str] = {
    "positive_price": "price > 0",
    "interval_not_inverted": "effective_end IS NULL OR effective_end >= effective_start",
    "nothing_rescued": "_rescued_data IS NULL",
}

PRICE_CATALOG_FAIL: dict[str, str] = {
    "product_present": "product_id IS NOT NULL",
    "start_present": "effective_start IS NOT NULL",
}
