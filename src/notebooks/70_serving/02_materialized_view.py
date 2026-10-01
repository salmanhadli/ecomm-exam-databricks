# Databricks notebook source
# MAGIC %md
# MAGIC # Serving · Materialized view on the SQL warehouse
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Job → task** | `ecomm_70_serving` → `materialized_view` |
# MAGIC | **Writes** | `gold.mv_category_daily`: conversion and revenue per category per day |
# MAGIC | **Brief** | Step 9 (materialized views, refresh cadence) · acceptance **A12** |
# MAGIC | **Databricks concepts** | Databricks SQL materialized views · refresh (incremental vs full) · the SDK's statement execution API |
# MAGIC
# MAGIC A materialized view stores a query's result and keeps it current with `REFRESH`. Where it can,
# MAGIC Databricks refreshes **incrementally**, processing only what changed in the source, which is
# MAGIC why the source table has row tracking on (`ecomm.tables.ROW_TRACKING`).
# MAGIC
# MAGIC **Refresh cadence.** The view is refreshed by this task, once per orchestrated run, right after
# MAGIC gold is rebuilt. It is never staler than the run that built gold. An MV can also carry its own
# MAGIC `SCHEDULE` or refresh `TRIGGER ON UPDATE` of its source. Both start a background pipeline, and
# MAGIC Free Edition allows one active pipeline per type, so this project refreshes explicitly.
# MAGIC
# MAGIC **What to expect (measured below).** Gold is rewritten with `INSERT OVERWRITE` every run
# MAGIC (decision D-18). A full rewrite of the source leaves nothing incremental to exploit, so the
# MAGIC refresh is likely a full recompute. Appending or merging into gold would let it go incremental.
# MAGIC The pipeline event log says which technique each refresh actually used.
# MAGIC
# MAGIC The statements run on the SQL warehouse through the Databricks SDK (`ecomm.warehouse`),
# MAGIC because materialized views are created by a SQL warehouse, not by serverless notebooks.

# COMMAND ----------

# Make the bundle's src/ folder importable. This notebook lives in src/notebooks/<layer>/,
# so src/ is two levels up; `ecomm` is the project library.
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from ecomm.runtime import bootstrap  # noqa: E402
from ecomm.serving import mv_ddl, mv_fqn  # noqa: E402
from ecomm.tables import GOLD_CONVERSION_HOURLY  # noqa: E402
from ecomm.warehouse import Warehouse  # noqa: E402

project, ev = bootstrap(spark, dbutils, step="materialized_view")
dbutils.widgets.text("warehouse_id", "")
warehouse = Warehouse(dbutils.widgets.get("warehouse_id"))
mv = mv_fqn(project)

# COMMAND ----------

# MAGIC %md ## 1 · Create once, then refresh

# COMMAND ----------

created = warehouse.run(mv_ddl(project, GOLD_CONVERSION_HOURLY.fqn(project)))
refreshed = warehouse.run(f"REFRESH MATERIALIZED VIEW {mv}")
rows = warehouse.run(f"SELECT count(*) FROM {mv}").rows[0][0]

ev.record("mv", mv)
ev.record("mv_create_seconds", round(created.seconds, 1), "no-op after the first run (IF NOT EXISTS)")
ev.record("mv_refresh_seconds", round(refreshed.seconds, 1))
ev.record("mv_rows", int(rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Which refresh technique ran
# MAGIC
# MAGIC Each refresh logs a `planning_information` event naming the technique it chose: incremental,
# MAGIC or a full recompute and the reason.

# COMMAND ----------

try:
    # The event's message names the technique, e.g. "... executed as ROW_BASED" (incremental)
    # or "... executed as COMPLETE_RECOMPUTE".
    technique = warehouse.run(f"""
        SELECT message
        FROM event_log(TABLE({mv}))
        WHERE event_type = 'planning_information'
        ORDER BY timestamp DESC LIMIT 1""").rows
    ev.record("mv_last_refresh_plan", technique[0][0][:300] if technique else "not logged yet")
except Exception as err:
    ev.record("mv_last_refresh_technique", "unavailable", str(err)[:160])

ev.flush()
