"""Databricks Jobs helpers (items: no scheduling; SQL-only transformations).

- run_notebook_now: write PySpark code to a workspace notebook and submit a
  one-time Job run on a cluster (enables engine=pyspark execution).
- schedule_pipeline: create a recurring Job that runs a notebook on a cron.
"""
from __future__ import annotations

import base64
import time
from typing import Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service import jobs

from .config import config


def _client() -> WorkspaceClient:
    if config.host and config.token:
        return WorkspaceClient(host=config.host, token=config.token)
    return WorkspaceClient()


def write_notebook(path: str, code: str) -> str:
    w = _client()
    src = "# Databricks notebook source\n" + code
    w.workspace.import_(
        path=path, format=ImportFormat.SOURCE, language=Language.PYTHON,
        content=base64.b64encode(src.encode()).decode(), overwrite=True,
    )
    return path


def run_notebook_now(path: str, cluster_id: str, timeout_s: int = 900) -> Tuple[bool, str]:
    """Submit a one-time run of a notebook and wait. Returns (ok, message)."""
    w = _client()
    run = w.jobs.submit(
        run_name=f"de-agent-{int(time.time())}",
        tasks=[jobs.SubmitTask(
            task_key="run",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(notebook_path=path),
        )],
    ).result(timeout=timeout_s)
    state = run.state
    ok = state and state.result_state and state.result_state.value == "SUCCESS"
    return bool(ok), (state.state_message if state else "no state")


def schedule_pipeline(name: str, notebook_path: str, cron: str,
                      cluster_id: Optional[str] = None, timezone: str = "UTC") -> int:
    """Create a recurring Databricks Job for the pipeline notebook."""
    w = _client()
    task = jobs.Task(
        task_key="medallion",
        notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
        existing_cluster_id=cluster_id,
    )
    created = w.jobs.create(
        name=name,
        tasks=[task],
        schedule=jobs.CronSchedule(quartz_cron_expression=cron, timezone_id=timezone),
    )
    return created.job_id
