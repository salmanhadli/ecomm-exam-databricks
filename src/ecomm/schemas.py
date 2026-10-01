"""Spark schemas shared across layers.

Declaring schemas explicitly (instead of inferring them) makes every read
deterministic and avoids an extra pass over the files to infer types.
"""

from pyspark.sql.types import DecimalType, DoubleType, StringType, StructField, StructType, TimestampType

# Auto Loader puts any field it cannot parse into the schema here instead of dropping it,
# so malformed input is visible in bronze rather than silently lost.
RESCUED_COLUMN = "_rescued_data"

# The raw event as exported by Kaggle. Ids stay strings to keep raw fidelity;
# only event_time and price are typed. Money becomes DECIMAL(12,2) from silver on.
EVENT_SCHEMA = StructType([
    StructField("event_time", TimestampType()),
    StructField("event_type", StringType()),     # view | cart | remove_from_cart | purchase
    StructField("product_id", StringType()),
    StructField("category_id", StringType()),
    StructField("category_code", StringType()),  # NULL on 98% of cosmetics rows
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", StringType()),
    StructField("user_session", StringType()),   # unreliable: NULLs and reuse after long gaps
])

# Stream B, as written by the harness (ecomm.transforms.harness.derive_price_intervals).
PRICE_CATALOG_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("price", DecimalType(12, 2)),
    StructField("effective_start", TimestampType()),
    StructField("effective_end", TimestampType()),   # NULL = current (open) interval
    StructField("change_reason", StringType()),      # initial | increase | decrease | no_net_change
    StructField("pct_change", DoubleType()),
])


def with_rescued(schema: StructType) -> StructType:
    """`schema` plus the rescued-data column, as Auto Loader expects when a schema is given."""
    return StructType(schema.fields + [StructField(RESCUED_COLUMN, StringType())])
