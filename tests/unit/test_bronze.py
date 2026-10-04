"""Bronze audit columns and data-quality rules (ecomm.transforms.bronze, ecomm.quality)."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ecomm import quality
from ecomm.schemas import EVENT_SCHEMA, PRICE_CATALOG_SCHEMA, RESCUED_COLUMN, with_rescued
from ecomm.transforms.bronze import batch_id_from_path

ROOT = "/Volumes/exam_ecommerce/raw/landing/clickstream"


@pytest.mark.parametrize("path, expected", [
    (f"{ROOT}/dt=2019-10-01/hh=10/min5=05/part-00000-abc.c000.json", "2019-10-01T10:05"),
    # _metadata.file_path may report a volume file with a URI scheme; the folders still parse
    (f"dbfs:{ROOT}/dt=2019-10-01/hh=10/min5=05/part-00000-abc.c000.json", "2019-10-01T10:05"),
    (f"{ROOT}/dt=2019-10-14/hh=23/min5=55/part-00007-def.c000.json", "2019-10-14T23:55"),
    (f"{ROOT}/stray-file.json", None),                     # outside the layout -> NULL -> fail rule
])
def test_batch_id_is_the_micro_batch_folder(spark, path, expected):
    got = spark.createDataFrame([(path,)], "p string").select(batch_id_from_path(F.col("p"))).first()[0]
    assert got == expected


def test_rescued_column_is_appended_once():
    schema = with_rescued(EVENT_SCHEMA)
    assert schema.fieldNames()[-1] == RESCUED_COLUMN
    assert schema.fieldNames().count(RESCUED_COLUMN) == 1


@pytest.mark.parametrize("rules, schema", [
    ({**quality.CLICKSTREAM_WARN, **quality.CLICKSTREAM_FAIL},
     StructType(with_rescued(EVENT_SCHEMA).fields + [StructField("_batch_id", StringType())])),
    ({**quality.PRICE_CATALOG_WARN, **quality.PRICE_CATALOG_FAIL},
     with_rescued(PRICE_CATALOG_SCHEMA)),
])
def test_every_expectation_is_valid_sql_over_the_bronze_columns(spark, rules, schema):
    empty = spark.createDataFrame([], schema)
    for name, condition in rules.items():
        empty.select(F.expr(condition).alias(name)).collect()   # raises if the SQL is invalid
