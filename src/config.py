"""Configuration for the medallion data-engineering agent (v2).

Adds: write modes (incremental/MERGE), engine choice (SQL/PySpark), streaming,
dynamic layer definitions, human approval gate, checkpointing, and semantics.
In a Databricks App auth is automatic; locally set DATABRICKS_HOST/TOKEN.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name, "")
    return [x.strip() for x in raw.split(",") if x.strip()] or default


@dataclass
class Config:
    # ---- Databricks ----
    host: str = os.getenv("DATABRICKS_HOST", "")
    token: str = os.getenv("DATABRICKS_TOKEN", "")
    warehouse_id: str = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    # ---- Unity Catalog ----
    catalog: str = os.getenv("UC_CATALOG", "main")
    schema: str = os.getenv("UC_SCHEMA", "de_agent")
    volume: str = os.getenv("UC_VOLUME", "raw")
    # Where generated SQL / run artifacts are versioned (UC volume subfolder).
    artifacts_volume: str = os.getenv("UC_ARTIFACTS_VOLUME", "agent_artifacts")

    # ---- LLM ----
    llm_provider: str = os.getenv("LLM_PROVIDER", "databricks")
    llm_model: str = os.getenv("LLM_MODEL", "databricks-meta-llama-3-3-70b-instruct")

    # ---- Pipeline behavior ----
    # Layers processed in order. Fully configurable (item: fixed layers).
    layers: List[str] = field(default_factory=lambda: _list("PIPELINE_LAYERS",
                                                             ["bronze", "silver", "gold"]))
    # full_refresh | incremental  (item: full-refresh only)
    write_mode: str = os.getenv("WRITE_MODE", "full_refresh")
    # batch | streaming (Auto Loader)  (item: no streaming)
    ingest_mode: str = os.getenv("INGEST_MODE", "batch")
    # sql | pyspark  (item: SQL-only transformations)
    engine: str = os.getenv("ENGINE", "sql")

    # ---- Quality / safety ----
    max_iterations: int = int(os.getenv("MAX_ITERATIONS", "4"))
    require_approval: bool = _bool("REQUIRE_APPROVAL", False)   # human-in-the-loop gate
    run_explain: bool = _bool("RUN_EXPLAIN", True)             # cost/perf pre-check
    enable_profiling: bool = _bool("ENABLE_PROFILING", True)   # data profiling before build
    min_rows: int = int(os.getenv("MIN_ROWS", "1"))
    max_null_pct: float = float(os.getenv("MAX_NULL_PCT", "50.0"))  # silver DQ threshold

    # ---- Checkpointing (item: stateless runs) ----
    checkpoint_path: str = os.getenv("CHECKPOINT_PATH", "/home/user/.agent_checkpoints.sqlite")

    def validate(self) -> None:
        if not self.warehouse_id:
            raise ValueError("DATABRICKS_WAREHOUSE_ID is required.")
        if self.write_mode not in ("full_refresh", "incremental"):
            raise ValueError("WRITE_MODE must be full_refresh or incremental.")
        if self.engine not in ("sql", "pyspark"):
            raise ValueError("ENGINE must be sql or pyspark.")
        if self.ingest_mode not in ("batch", "streaming"):
            raise ValueError("INGEST_MODE must be batch or streaming.")


config = Config()
