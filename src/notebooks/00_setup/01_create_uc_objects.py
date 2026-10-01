# Databricks notebook source
# MAGIC %md
# MAGIC # Setup · Unity Catalog objects
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_00_setup` → `setup` |
# MAGIC | **Creates** | catalog `<catalog>` · schemas `raw` `bronze` `silver` `gold` `audit` `ops` · volume `raw.landing` · table `ops.measurements` |
# MAGIC | **Databricks concepts** | Unity Catalog three-level namespace (`catalog.schema.object`) · managed volumes · managed Delta tables · table properties |
# MAGIC
# MAGIC **Idempotent** — every statement is `CREATE … IF NOT EXISTS`, so this runs first on every
# MAGIC job run at no cost.
# MAGIC
# MAGIC **Why here and not in the bundle?** Bundles can create schemas and volumes, but then
# MAGIC `databricks bundle destroy` would drop them — with every table inside. Jobs and pipelines
# MAGIC are deployment artefacts; the data is not. Keeping storage objects out of the bundle means
# MAGIC no deployment command can delete data.

# COMMAND ----------

# MAGIC %md ## 1 · Setup

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from ecomm.evidence import evidence_ddl  # noqa: E402
from ecomm.project import SCHEMAS  # noqa: E402
from ecomm.runtime import bootstrap  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="setup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Catalog
# MAGIC
# MAGIC A catalog is Unity Catalog's top-level container; everything this project creates lives
# MAGIC inside one. Free Edition's documentation doesn't say whether users may create catalogs.
# MAGIC If this cell fails, redeploy with `--var catalog=workspace` to use the default catalog.

# COMMAND ----------

existing_catalogs = {row[0] for row in spark.sql("SHOW CATALOGS").collect()}

if project.catalog in existing_catalogs:
    ev.record("catalog", project.catalog, "already existed")
else:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {project.catalog} "
                  f"COMMENT 'ECOMM clickstream exam: Delta on Databricks Free Edition'")
    except Exception as err:
        raise RuntimeError(
            f"Could not create catalog {project.catalog}: {err}\n"
            f"Create it in Catalog Explorer, or redeploy the bundle with "
            f"--var catalog=workspace to use the default catalog.") from err
    ev.record("catalog", project.catalog, "created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Schemas, landing volume, evidence table
# MAGIC
# MAGIC - One **schema per medallion layer** — permissions and retention can then differ by layer.
# MAGIC - A **volume** holds *files* under Unity Catalog governance (`/Volumes/<catalog>/<schema>/<volume>/…`).
# MAGIC   The source CSVs and both harness feeds are files, so they live in `raw.landing`.
# MAGIC - `ops.measurements` is created with the project's **history retention policy**
# MAGIC   (`ecomm.history`): previous versions are kept, never vacuumed away early.

# COMMAND ----------

for schema, comment in SCHEMAS.items():
    escaped = comment.replace("'", "\\'")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {project.schema(schema)} COMMENT '{escaped}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {project.schema('raw')}.landing "
          f"COMMENT 'Source CSVs, clickstream micro-batches and the price catalog feed'")

spark.sql(evidence_ddl(project))

# COMMAND ----------

# MAGIC %md ## 4 · Record evidence

# COMMAND ----------

created = {row[0] for row in spark.sql(f"SHOW SCHEMAS IN {project.catalog}").collect()}
ev.record("schemas", ", ".join(s for s in SCHEMAS if s in created))
ev.record("landing_volume", project.landing)
ev.flush()
