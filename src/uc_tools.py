"""Unity Catalog + SQL execution helpers (v2).

All work runs as SQL against a serverless SQL Warehouse via the Statement
Execution API (Databricks-Apps-native). Adds: transient-error backoff, EXPLAIN
cost/perf pre-check, artifact upload for SQL versioning.
"""
from __future__ import annotations

import io
import time
from typing import Any, Dict, List, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from .config import config
from . import errors


class UCTools:
    def __init__(self) -> None:
        if config.host and config.token:
            self.w = WorkspaceClient(host=config.host, token=config.token)
        else:
            self.w = WorkspaceClient()
        self.warehouse_id = config.warehouse_id

    # ---------------------------------------------------------------- SQL
    def _execute_once(self, statement: str, timeout_s: int) -> Tuple[List[Dict[str, Any]], str]:
        try:
            resp = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id, statement=statement,
                wait_timeout="30s", on_wait_timeout="CONTINUE",
            )
            sid = resp.statement_id
            deadline = time.time() + timeout_s
            state = resp.status.state
            while state in (StatementState.PENDING, StatementState.RUNNING):
                if time.time() > deadline:
                    return [], f"Timed out after {timeout_s}s"
                time.sleep(2)
                resp = self.w.statement_execution.get_statement(sid)
                state = resp.status.state
            if state != StatementState.SUCCEEDED:
                err = resp.status.error
                return [], (err.message if err else f"Statement ended in state {state}")
            return self._rows(resp), ""
        except Exception as e:
            return [], f"{type(e).__name__}: {e}"

    def run_sql(self, statement: str, timeout_s: int = 300, retries: int = 2) -> Tuple[List[Dict[str, Any]], str]:
        """Execute SQL with automatic backoff on transient/infra-transient errors."""
        delay = 3
        for attempt in range(retries + 1):
            rows, err = self._execute_once(statement, timeout_s)
            if not err:
                return rows, ""
            cls = errors.classify(err)
            if cls.should_retry and attempt < retries:
                time.sleep(delay)
                delay *= 2
                continue
            return [], err
        return [], "exhausted retries"

    @staticmethod
    def _rows(resp) -> List[Dict[str, Any]]:
        result, manifest = resp.result, resp.manifest
        if result is None or result.data_array is None or manifest is None:
            return []
        cols = [c.name for c in manifest.schema.columns]
        return [dict(zip(cols, row)) for row in result.data_array]

    # ---------------------------------------------------- EXPLAIN (cost/perf)
    def explain(self, statement: str) -> Tuple[bool, str]:
        """Validate a statement's plan without running it. Returns (ok, plan_text).
        Used as a cheap cost/perf + correctness pre-check before execution."""
        # EXPLAIN works on the SELECT; strip a leading CREATE ... AS if present.
        sel = statement
        low = statement.lower()
        idx = low.find(" as select")
        if idx != -1:
            sel = statement[idx + 4:]
        rows, err = self.run_sql(f"EXPLAIN {sel}", timeout_s=60, retries=1)
        if err:
            return False, err
        plan = "\n".join(str(list(r.values())[0]) for r in rows) if rows else ""
        # DBSQL reports plan errors inside the EXPLAIN text
        if "error" in plan.lower() and "== physical plan ==" not in plan.lower():
            return False, plan
        return True, plan

    # ------------------------------------------------- UC object creation
    def ensure_namespace(self, catalog: str, schema: str, *volumes: str) -> None:
        self.run_sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        self.run_sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        for v in volumes:
            if v:
                self.run_sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{v}")

    def volume_path(self, catalog: str, schema: str, volume: str, filename: str = "") -> str:
        base = f"/Volumes/{catalog}/{schema}/{volume}"
        return f"{base}/{filename}" if filename else base

    # ---------------------------------------------------- files / artifacts
    def upload_to_volume(self, data: bytes, dest_path: str) -> str:
        self.w.files.upload(dest_path, io.BytesIO(data), overwrite=True)
        return dest_path

    def write_artifact(self, catalog: str, schema: str, filename: str, content: str) -> str:
        dest = self.volume_path(catalog, schema, config.artifacts_volume, filename)
        return self.upload_to_volume(content.encode("utf-8"), dest)

    # ------------------------------------------------------ introspection
    def infer_columns(self, source: str, fmt: str) -> List[str]:
        opts = ", header => true, inferSchema => true" if fmt == "csv" else ""
        rows, err = self.run_sql(
            f"SELECT * FROM read_files('{source}', format => '{fmt}'{opts}) LIMIT 5")
        if err or not rows:
            return []
        return list(rows[0].keys())

    def count_rows(self, table: str) -> int:
        rows, err = self.run_sql(f"SELECT count(*) AS n FROM {table}")
        if err or not rows:
            return -1
        return int(list(rows[0].values())[0])

    def table_columns(self, table: str) -> List[str]:
        rows, err = self.run_sql(f"DESCRIBE {table}")
        if err:
            return []
        cols = []
        for r in rows:
            name = r.get("col_name") or r.get("name")
            if name and not str(name).startswith("#") and name.strip():
                cols.append(name)
        return cols


_tools: UCTools | None = None


def get_uc() -> UCTools:
    global _tools
    if _tools is None:
        _tools = UCTools()
    return _tools
