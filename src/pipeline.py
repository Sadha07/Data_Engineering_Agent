"""Programmatic entrypoint for the medallion pipeline (v2)."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from .config import config
from .graph import build_graph, get_checkpointer
from .state import MedallionState


def run_pipeline(
    instructions: str,
    raw_source: str,
    raw_format: str,
    dataset_name: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    semantics: Optional[Dict[str, Any]] = None,
    write_mode: Optional[str] = None,
    ingest_mode: Optional[str] = None,
    engine: Optional[str] = None,
    layers: Optional[List[str]] = None,
    thread_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the pipeline. Returns the final (or paused) state.

    When REQUIRE_APPROVAL is set the graph pauses before executing SQL; call
    resume_pipeline(thread_id, approved=True) to continue.
    """
    config.validate()
    checkpointer = get_checkpointer()
    app = build_graph(checkpointer=checkpointer)

    run_id = uuid.uuid4().hex[:10]
    initial: MedallionState = {
        "run_id": run_id,
        "instructions": instructions,
        "raw_source": raw_source,
        "raw_format": raw_format,
        "dataset_name": dataset_name,
        "catalog": catalog or config.catalog,
        "schema": schema or config.schema,
        "semantics": semantics or {},
        "write_mode": write_mode or config.write_mode,
        "ingest_mode": ingest_mode or config.ingest_mode,
        "engine": engine or config.engine,
        "layers": layers or config.layers,
        "log": [],
    }
    run_cfg = {"recursion_limit": 80,
               "configurable": {"thread_id": thread_id or run_id}}
    final = app.invoke(initial, config=run_cfg)
    final["thread_id"] = thread_id or run_id
    return final


def resume_pipeline(thread_id: str, approved: bool = True) -> Dict[str, Any]:
    """Resume a paused (approval-gated) run."""
    checkpointer = get_checkpointer()
    app = build_graph(checkpointer=checkpointer)
    run_cfg = {"configurable": {"thread_id": thread_id}}
    app.update_state(run_cfg, {"approved": approved})
    final = app.invoke(None, config=run_cfg)
    final["thread_id"] = thread_id
    return final


if __name__ == "__main__":
    result = run_pipeline(
        instructions="Aggregate total sales amount by region and month.",
        raw_source=f"/Volumes/{config.catalog}/{config.schema}/{config.volume}/sales.csv",
        raw_format="csv", dataset_name="sales",
    )
    print("PLAN:\n", result.get("plan"))
    for layer, info in result.get("layer_results", {}).items():
        print(f"[{layer}] {info.get('table')} -> {info.get('rows')} rows")
    print("\nLOG:")
    for line in result.get("log", []):
        print(" -", line)
