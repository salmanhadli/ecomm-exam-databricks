"""bronze.price_catalog: Stream B, ingested incrementally with Auto Loader.

Lakeflow pipeline `ecomm_bronze` (resources/pipelines/ecomm_bronze.pipeline.yml).

Each row is one price validity interval, as derived by the harness. Like
bronze.clickstream it is append-only and lossless; silver keeps the latest version of
each (product_id, effective_start).
"""

import sys

from pyspark import pipelines as dp

# The pipeline passes the bundle's src/ path in its configuration, which makes the project
# library importable (see `configuration` in the pipeline YAML).
sys.path.append(spark.conf.get("ecomm.src_path"))  # noqa: F821 - `spark` is a pipeline global

from ecomm.history import table_properties  # noqa: E402
from ecomm.project import Project  # noqa: E402
from ecomm.quality import PRICE_CATALOG_FAIL, PRICE_CATALOG_WARN  # noqa: E402
from ecomm.schemas import PRICE_CATALOG_SCHEMA, RESCUED_COLUMN, with_rescued  # noqa: E402
from ecomm.transforms.bronze import with_audit_columns  # noqa: E402

project = Project(spark.conf.get("ecomm.catalog"))  # noqa: F821


@dp.table(
    name="price_catalog",
    comment="Stream B as ingested: price validity intervals per product, plus audit columns. "
            "Append-only; silver keeps the latest version of each interval.",
    table_properties=table_properties("bronze"),
    cluster_by=["ingest_date"],
)
@dp.expect_all(PRICE_CATALOG_WARN)
@dp.expect_all_or_fail(PRICE_CATALOG_FAIL)
def price_catalog():
    intervals = (spark.readStream.format("cloudFiles")  # noqa: F821
                 .option("cloudFiles.format", "parquet")
                 .option("rescuedDataColumn", RESCUED_COLUMN)
                 .option("pathGlobFilter", "*.parquet")
                 .schema(with_rescued(PRICE_CATALOG_SCHEMA))
                 .load(project.price_catalog_dir))
    return with_audit_columns(intervals, micro_batch_layout=False).select(
        *PRICE_CATALOG_SCHEMA.fieldNames(), RESCUED_COLUMN,
        "_src_file", "_ingested_at", "ingest_date",
    )
