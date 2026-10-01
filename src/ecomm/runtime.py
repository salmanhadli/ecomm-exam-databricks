"""Notebook bootstrap: the first thing every notebook calls.

Job parameters arrive in a notebook as widgets. Every job in this bundle declares:

    catalog               which Unity Catalog catalog to use
    run_id                the job's own run id ({{job.run_id}})
    orchestration_run_id  the orchestrator's run id, empty when the job ran on its own

The widget defaults below only apply when a notebook is opened and run by hand.
"""

from __future__ import annotations

from ecomm.evidence import Evidence
from ecomm.project import Project


def bootstrap(spark, dbutils, step: str) -> tuple[Project, Evidence]:
    """Read the common job parameters, pin the session time zone, open the evidence log."""
    dbutils.widgets.text("catalog", "exam_ecommerce")
    dbutils.widgets.text("run_id", "manual")
    dbutils.widgets.text("orchestration_run_id", "")

    project = Project(dbutils.widgets.get("catalog"))

    # Timestamps are stored as UTC instants; the session zone decides how they are
    # rendered and parsed (dt=/hh= folder names, date_format, string casts). Serverless
    # defaults to UTC already; setting it makes the notebook independent of that default.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    evidence = Evidence(spark, project, step,
                        run_id=dbutils.widgets.get("run_id"),
                        orchestration_run_id=dbutils.widgets.get("orchestration_run_id") or None)
    return project, evidence
