"""Performance evidence without a Spark UI.

Serverless compute has no Spark UI and no access to the JVM (`_jdf`), so the brief's
"paste the physical plan" and "max vs median task duration" are measured differently:

    physical_plan(df)          the formatted plan, captured from df.explain()
    operators(plan, pattern)   operator names in that plan, e.g. PhotonBroadcastHashJoin
    time_full_compute(df)      wall-clock seconds to compute every row and column
    partition_skew(df, key)    rows per shuffle partition when hashed on `key`: a data-level
                               stand-in for task skew, since each partition becomes one task

For the full operator-level picture, open the query profile from a notebook cell's
"See performance" link.
"""

from __future__ import annotations

import contextlib
import io
import re
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def physical_plan(df: DataFrame) -> str:
    """The plan df.explain(mode="formatted") prints, as a string."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode="formatted")
    return buffer.getvalue()


def operators(plan: str, pattern: str = r"\w*Join\w*") -> list[str]:
    """Distinct operator names matching `pattern`, in plan order.

    The formatted plan lists operators as numbered lines, e.g. `(7) PhotonBroadcastHashJoin`.
    """
    found = re.findall(rf"^\(\d+\)\s+({pattern})", plan, flags=re.MULTILINE)
    return list(dict.fromkeys(found))


def time_full_compute(df: DataFrame) -> float:
    """Seconds to compute `df` completely.

    `count()` is not enough: Spark prunes columns a count does not need, so the join
    output might never be built. Hashing every column forces the full result and still
    returns a single row.
    """
    started = time.perf_counter()
    df.select(F.sum(F.xxhash64(*df.columns))).collect()
    return time.perf_counter() - started


def partition_skew(df: DataFrame, key: str, partitions: int = 200) -> dict[str, float]:
    """Rows per partition after hashing on `key`: max, median and their ratio.

    A window partitioned by `key` puts each partition into one task, so this is what
    the Spark UI's max-vs-median task duration would show, measured in rows.
    """
    sizes = (df.repartition(partitions, key)
             .groupBy(F.spark_partition_id().alias("partition")).count()
             .agg(F.max("count").alias("max"),
                  F.percentile_approx("count", 0.5).alias("median"))
             .first())
    return {"max_rows": sizes["max"], "median_rows": sizes["median"],
            "ratio": round(sizes["max"] / sizes["median"], 2) if sizes["median"] else float("inf")}
