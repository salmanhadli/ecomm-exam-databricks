"""bronze.clickstream: Stream A, ingested incrementally with Auto Loader.

Lakeflow pipeline `ecomm_bronze` (resources/pipelines/ecomm_bronze.pipeline.yml).

What Auto Loader does here
--------------------------
- Lists the landing folder and remembers every file it has ingested, in a checkpoint
  the pipeline manages. Each update reads only files it has not seen: exactly-once
  ingestion, with no bookkeeping in our code.
- ~4,000 small JSON input files become a handful of well-sized Delta files, because the
  pipeline writes each update as one commit. This is where the brief's small-file problem
  is solved, instead of being carried into the tables.
- Fields that don't fit the declared schema go to `_rescued_data` instead of being dropped.

The table is lossless (decision D-15): expectations measure the known dirt, and silver filters it.
"""

import sys

from pyspark import pipelines as dp

# The pipeline passes the bundle's src/ path in its configuration, which makes the project
# library importable (see `configuration` in the pipeline YAML).
sys.path.append(spark.conf.get("ecomm.src_path"))  # noqa: F821 - `spark` is a pipeline global

from ecomm.history import table_properties  # noqa: E402
from ecomm.project import Project  # noqa: E402
from ecomm.quality import CLICKSTREAM_FAIL, CLICKSTREAM_WARN  # noqa: E402
from ecomm.schemas import EVENT_SCHEMA, RESCUED_COLUMN, with_rescued  # noqa: E402
from ecomm.transforms.bronze import with_audit_columns  # noqa: E402

project = Project(spark.conf.get("ecomm.catalog"))  # noqa: F821


@dp.table(
    name="clickstream",
    comment="Stream A as ingested: one row per event from the 5-minute JSON micro-batches, "
            "plus audit columns. Append-only and lossless; silver deduplicates and filters.",
    table_properties=table_properties("bronze"),
    cluster_by=["ingest_date"],
)
@dp.expect_all(CLICKSTREAM_WARN)
@dp.expect_all_or_fail(CLICKSTREAM_FAIL)
def clickstream():
    events = (spark.readStream.format("cloudFiles")  # noqa: F821
              .option("cloudFiles.format", "json")
              .option("rescuedDataColumn", RESCUED_COLUMN)
              # Only the data files. Spark also writes commit markers into each folder.
              .option("pathGlobFilter", "*.json")
              .schema(with_rescued(EVENT_SCHEMA))
              .load(project.clickstream_dir))
    return with_audit_columns(events, micro_batch_layout=True).select(
        *EVENT_SCHEMA.fieldNames(), RESCUED_COLUMN,
        "_src_file", "_batch_id", "_ingested_at", "ingest_date",
    )
