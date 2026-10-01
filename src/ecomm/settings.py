"""Defended thresholds and constants for the whole project.

Every value here is a decision the brief asks you to make and defend. They are
carried over from the earlier AWS implementation of the same brief (EMR + Iceberg),
so both answer it with the same rules; the reasoning for each is in docs/decisions.md.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # --- Harness (brief: "Dataset & ingestion harness") ----------------------
    simulated_days: int = 14          # brief minimum: 14 simulated days
    micro_batch_minutes: int = 5      # brief: one JSON file per 5-minute interval

    # --- Price catalog (Stream B) ------------------------------------------------
    # A one-cent step is rounding, not a repricing. Compared in integer cents,
    # because in DOUBLE 43% of one-cent steps compare as greater than 0.01.
    noise_threshold_cents: int = 1
    significant_change_pct: float = 0.15            # brief: "> 15% in either direction"
    min_products_with_significant_change: int = 200  # acceptance A2

    # --- Sessionization (step 2) -------------------------------------------------
    session_gap_seconds: int = 1800   # brief: a session ends after 30 minutes idle

    # --- Money --------------------------------------------------------------------
    decimal_precision: int = 12       # DECIMAL(12,2): exact cents, up to 9,999,999,999.99
    decimal_scale: int = 2
    revenue_precision: int = 18       # sums of money: DECIMAL(18,2)

    # --- Q1 · elasticity (step 6b) ----------------------------------------------
    post_window_hours: int = 2        # brief: "the 2 hours after the change"
    baseline_days: int = 7            # brief: "the 7-day baseline at the original price"
    # A post window cut short (the next change came quickly, or the data ends) is still
    # normalised to 2 hours, but below this length that extrapolation is noise.
    q1_min_post_hours: float = 1.0
    # Minimum baseline purchases for a change to count. The brief suggests 30; on this
    # data that leaves no product, so every floor is reported and 5 is the headline
    # (as on AWS), stated as too thin for a pricing decision.
    q1_floors: tuple[int, ...] = (1, 5, 10, 30)
    q1_headline_floor: int = 5

    # --- Q2 · cart abandonment (step 6c) ------------------------------------------
    abandonment_window_minutes: int = 30   # brief: "within 30 minutes of a price increase"

    # --- Skew (step 6a) --------------------------------------------------------------
    salt_buckets: int = 16


SETTINGS = Settings()

# Kaggle "eCommerce Events History in Cosmetics Shop". The cosmetics variant is used
# because the electronics one has no product with more than one price, which leaves
# Q1 and Q2 nothing to measure.
KAGGLE_SLUG = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
SOURCE_FILES = ("2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv", "2020-Jan.csv", "2020-Feb.csv")
