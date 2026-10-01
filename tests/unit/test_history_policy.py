"""Guard against shortening table history — the "vacuum it all away" habit.

The AWS build expired every table down to its latest snapshot after each write.
These tests make that impossible to reintroduce silently: they read the source of
every notebook, pipeline and library module and fail on any statement that deletes
table history early or overrides the retention policy outside ecomm/history.py.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ecomm.history import RETENTION_DAYS, _days, table_properties, tblproperties_sql

SRC = Path(__file__).resolve().parents[2] / "src"
POLICY_MODULE = SRC / "ecomm" / "history.py"

# Each pattern matches code that *does* something (an assignment, a call, a VACUUM with a
# number of hours), not prose that mentions it, so notebooks can still explain the policy.
FORBIDDEN = {
    r"VACUUM\b[^\n]*\bRETAIN\s+\d": "VACUUM with an explicit RETAIN clause",
    r"retentionDurationCheck": "disabling Delta's retention safety check",
    r"expire_snapshots\s*\(|retain_last\s*=": "Iceberg-style snapshot expiry",
    r"(logRetentionDuration|deletedFileRetentionDuration)['\"]?\s*[=:]":
        "retention set outside ecomm/history.py",
}


def source_files():
    return sorted(p for p in SRC.rglob("*.py") if p != POLICY_MODULE)


@pytest.mark.parametrize("pattern, meaning", FORBIDDEN.items())
def test_no_source_file_shortens_history(pattern, meaning):
    offenders = [f"{p.relative_to(SRC)}:{n}"
                 for p in source_files()
                 for n, line in enumerate(p.read_text().splitlines(), 1)
                 if re.search(pattern, line, re.IGNORECASE)]
    assert not offenders, f"{meaning} found in: {offenders}"


def test_every_layer_keeps_at_least_thirty_days():
    assert min(RETENTION_DAYS.values()) >= 30


def test_audit_layers_cover_a_full_year():
    # Q3 asks for Black Friday numbers "months later"
    assert RETENTION_DAYS["gold"] >= 365 and RETENTION_DAYS["audit"] >= 365


def test_log_and_file_retention_are_equal_so_the_whole_window_is_readable():
    for schema in RETENTION_DAYS:
        log, files = table_properties(schema).values()
        assert log == files


def test_properties_render_as_a_tblproperties_clause():
    assert tblproperties_sql("gold") == (
        "TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 365 days', "
        "'delta.deletedFileRetentionDuration' = 'interval 365 days')")


@pytest.mark.parametrize("value, days", [("interval 30 days", 30), ("interval 2 weeks", 14),
                                          ("interval 36 hours", 1.5), (None, 0)])
def test_interval_parsing(value, days):
    assert _days(value) == days


@pytest.mark.parametrize("code", [
    "spark.sql(f'VACUUM {table} RETAIN 0 HOURS')",
    "ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 1 hours')",
    'properties = {"delta.logRetentionDuration": "interval 1 days"}',
    "spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')",
    "CALL glue_catalog.system.expire_snapshots(table => 't', retain_last => 1)",
])
def test_the_guard_catches_real_violations(code):
    assert any(re.search(pattern, code, re.IGNORECASE) for pattern in FORBIDDEN)


@pytest.mark.parametrize("prose", [
    "# MAGIC longer than `delta.deletedFileRetentionDuration` qualify.",
    "# MAGIC Iceberg's version is a tag without `RETAIN`, which `expire_snapshots` removes.",
    "deletable = spark.sql(f'VACUUM {live} DRY RUN').count()",
])
def test_the_guard_allows_explanations_and_dry_runs(prose):
    assert not any(re.search(pattern, prose, re.IGNORECASE) for pattern in FORBIDDEN)
