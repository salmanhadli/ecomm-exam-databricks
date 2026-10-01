"""Bronze transformations: audit columns added to every ingested row.

    _src_file     full path of the file the row came from (`_metadata.file_path`)
    _batch_id     the micro-batch the file belongs to, from its dt=/hh=/min5= folders,
                  e.g. `2019-10-01T10:05`
    _ingested_at  when bronze ingested the row
    ingest_date   the date part of `_ingested_at`; bronze clusters on it, like the
                  brief's `ingest_date` partition

`_batch_id` identifies the *source micro-batch* rather than the ingestion run: two runs can
ingest the same file only if it was re-landed, and then the tie is visible in silver's dedup.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

_BATCH_PATH = r"dt=(\d{4}-\d{2}-\d{2})/hh=(\d{2})/min5=(\d{2})/"


def batch_id_from_path(path: Column) -> Column:
    """'…/dt=2019-10-01/hh=10/min5=05/part-….json' -> '2019-10-01T10:05'; NULL if the layout is wrong."""
    date = F.regexp_extract(path, _BATCH_PATH, 1)
    return F.when(date == "", F.lit(None)).otherwise(
        F.concat(date, F.lit("T"), F.regexp_extract(path, _BATCH_PATH, 2),
                 F.lit(":"), F.regexp_extract(path, _BATCH_PATH, 3)))


def with_audit_columns(df: DataFrame, micro_batch_layout: bool) -> DataFrame:
    """Add the bronze audit columns. `df` must be a file read (it uses `_metadata.file_path`)."""
    path = F.col("_metadata.file_path")
    out = df.withColumn("_src_file", path)
    if micro_batch_layout:
        out = out.withColumn("_batch_id", batch_id_from_path(path))
    return (out
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("ingest_date", F.current_date()))
