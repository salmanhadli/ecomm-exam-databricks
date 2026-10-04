"""Warehouse helpers (ecomm.warehouse) that don't need a workspace."""

from __future__ import annotations

from types import SimpleNamespace

from ecomm.warehouse import _queries

QUERY = SimpleNamespace(duration=120, metrics=SimpleNamespace(read_bytes=10, pruned_bytes=0))


def test_query_history_as_a_response_object():
    # The SDK on Databricks serverless returns ListQueriesResponse(res=[...]); iterating it
    # raised "'ListQueriesResponse' object is not iterable" on the first workspace run.
    assert _queries(SimpleNamespace(res=[QUERY], has_next_page=False)) == [QUERY]
    assert _queries(SimpleNamespace(res=None)) == []


def test_query_history_as_an_iterator():
    assert _queries(iter([QUERY])) == [QUERY]
