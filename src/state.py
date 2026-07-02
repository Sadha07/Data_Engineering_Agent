"""Shared LangGraph state for the medallion pipeline (v2)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class MedallionState(TypedDict, total=False):
    # ---- Inputs ----
    run_id: str
    instructions: str
    catalog: str
    schema: str
    raw_source: str
    raw_format: str
    dataset_name: str
    semantics: Dict[str, Any]        # business glossary / metrics
    key_columns: Dict[str, List[str]]   # per-layer natural keys (for MERGE + DQ)

    # ---- Modes (from config, overridable per run) ----
    write_mode: str                  # full_refresh | incremental
    ingest_mode: str                 # batch | streaming
    engine: str                      # sql | pyspark
    layers: List[str]                # e.g. [bronze, silver, gold]

    # ---- Clarification (item: prompt dependency) ----
    needs_clarification: bool
    clarification_question: str

    # ---- Discovery ----
    plan: str
    raw_schema: str
    profile: Dict[str, Any]          # profiling summary

    # ---- Layer artifacts ----
    layer_tables: Dict[str, str]     # layer -> fully-qualified table (multi-gold: gold -> list handled in results)
    gold_tables: List[str]           # multiple gold marts

    # ---- Current layer execution ----
    current_layer: str
    generated_code: str              # SQL or PySpark
    explain_plan: str
    execution_output: str
    execution_error: str
    error_kind: str                  # from errors.classify

    # ---- Validation ----
    layer_results: Dict[str, Any]
    dq_report: Dict[str, Any]
    is_valid: bool
    validation_feedback: str

    # ---- Approval (human-in-the-loop) ----
    approved: bool

    # ---- Control ----
    iteration: int
    max_iterations: int
    log: List[str]
    aborted: bool
    abort_reason: str
    manifest_path: str
    done: bool
