"""Q3 audit helpers: what a frozen copy records about itself, and the reading every proof compares.

A frozen copy is a DEEP CLONE of gold.product_conversion_hourly at one pinned version.
It carries its provenance as table properties, so it can be traced without any other record:

    ecomm.audit.name                 e.g. black_friday_2026_final
    ecomm.audit.source_table         the live table it was cloned from
    ecomm.audit.source_version       the live table's version it froze
    ecomm.audit.source_committed_at  when that version was committed (UTC)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROP_NAME = "ecomm.audit.name"
PROP_SOURCE = "ecomm.audit.source_table"
PROP_VERSION = "ecomm.audit.source_version"
PROP_COMMITTED = "ecomm.audit.source_committed_at"


def provenance_sql(table: str, name: str, source: str, version: int, committed_at: str) -> str:
    """ALTER TABLE statement that stamps a frozen copy with its provenance."""
    return (f"ALTER TABLE {table} SET TBLPROPERTIES ("
            f"'{PROP_NAME}' = '{name}', '{PROP_SOURCE}' = '{source}', "
            f"'{PROP_VERSION}' = '{version}', '{PROP_COMMITTED}' = '{committed_at}')")


def top_category(df: DataFrame) -> str:
    """The category_id with the most purchases: the Black Friday headline the proof tracks."""
    return (df.groupBy("category_id").agg(F.sum("purchases").alias("p"))
            .orderBy(F.desc("p"), "category_id").first()["category_id"])


def reading(df: DataFrame, category_id: str) -> tuple:
    """The numbers an executive would read: rows, purchases, revenue, and one category's purchases.

    Returned as a plain tuple so two readings compare with ==, exactly.
    """
    row = df.agg(
        F.count("*").alias("rows"),
        F.sum("purchases").alias("purchases"),
        F.sum("revenue").alias("revenue"),
        F.sum(F.when(F.col("category_id") == category_id, F.col("purchases")).otherwise(0)).alias("category"),
    ).first()
    return (row["rows"], row["purchases"], str(row["revenue"]), row["category"])
