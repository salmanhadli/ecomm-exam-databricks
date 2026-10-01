"""Shared fixtures for the unit tests: one small local Spark session for the whole run."""

from __future__ import annotations

import datetime as dt
import os
import time

import pytest
from pyspark.sql import SparkSession

# PySpark converts naive Python datetimes with the *Python process's* local time zone,
# not the Spark session's. On a laptop outside UTC, '10:07' would land as a different
# instant. Serverless runs in UTC, so the tests do too.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("ecomm-unit-tests")
               .config("spark.sql.session.timeZone", "UTC")   # same as serverless
               .config("spark.sql.shuffle.partitions", "2")   # tiny inputs, fast tests
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def ts(text: str) -> dt.datetime:
    """'2019-10-01 10:07:59' -> naive datetime, read by Spark as UTC in this session."""
    return dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
