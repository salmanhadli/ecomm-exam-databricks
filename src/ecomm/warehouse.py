"""Run SQL on the Databricks SQL warehouse from a notebook, and read back what it cost.

Notebooks run on serverless Spark. Some serving work belongs on the SQL warehouse instead:
materialized views are created there, and the three-way benchmark must time queries there.
The Databricks SDK is preinstalled on serverless and authenticates as the job's identity,
so no token is involved:

    run(statement)          execute on the warehouse, wait, return rows + statement id + seconds
    metrics(statement_id)   the warehouse's own record of the query: duration, bytes read

Imports of `databricks.sdk` are local to the functions, so this module can be imported
where the SDK is not installed (local unit tests).
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Result:
    rows: list
    statement_id: str
    seconds: float          # wall clock seen by the notebook, including queueing


class Warehouse:
    def __init__(self, warehouse_id: str, catalog: str | None = None):
        from databricks.sdk import WorkspaceClient

        self.client = WorkspaceClient()
        self.warehouse_id = warehouse_id
        self.catalog = catalog

    def run(self, statement: str, schema: str | None = None, timeout_s: int = 900) -> Result:
        """Execute `statement` and wait for it. Raises with the warehouse's error message on failure."""
        from databricks.sdk.service.sql import StatementState

        started = time.perf_counter()
        response = self.client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.warehouse_id,
            catalog=self.catalog, schema=schema, wait_timeout="30s")
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.perf_counter() - started > timeout_s:
                self.client.statement_execution.cancel_execution(response.statement_id)
                raise TimeoutError(f"statement {response.statement_id} ran longer than {timeout_s}s")
            time.sleep(2)
            response = self.client.statement_execution.get_statement(response.statement_id)
        if response.status.state != StatementState.SUCCEEDED:
            error = response.status.error.message if response.status.error else response.status.state
            raise RuntimeError(f"warehouse statement failed: {error}\n{statement}")
        rows = (response.result.data_array or []) if response.result else []
        return Result(rows, response.statement_id, time.perf_counter() - started)

    def metrics(self, statement_id: str, attempts: int = 10) -> dict:
        """Duration and bytes read, from the warehouse's query history (it can lag a few seconds)."""
        from databricks.sdk.service.sql import QueryFilter

        for _ in range(attempts):
            found = _queries(self.client.query_history.list(
                filter_by=QueryFilter(statement_ids=[statement_id]), include_metrics=True))
            if found and found[0].metrics is not None:
                info = found[0]
                return {"duration_ms": info.duration,
                        "read_bytes": info.metrics.read_bytes,
                        "pruned_bytes": info.metrics.pruned_bytes}
            time.sleep(3)
        return {"duration_ms": None, "read_bytes": None, "pruned_bytes": None}


def _queries(response) -> list:
    """The query records in a query-history response.

    SDK versions differ: some return an iterator of QueryInfo, others a ListQueriesResponse
    whose `res` holds the records (the serverless runtime's SDK does the latter).
    """
    if hasattr(response, "res"):
        return list(response.res or [])
    return list(response)
