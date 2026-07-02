"""Versioning of generated SQL / run artifacts (item: no version control).

Every run writes its plan and per-layer SQL to a UC Volume as timestamped,
content-hashed files, so there is an auditable history of what the agent
generated and ran. Also returns a local copy for the UI/export.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_manifest(state: Dict[str, Any]) -> Dict[str, Any]:
    """Assemble a JSON manifest describing the run."""
    layers = state.get("layer_results", {})
    manifest = {
        "run_id": state.get("run_id"),
        "timestamp": _now(),
        "instructions": state.get("instructions"),
        "raw_source": state.get("raw_source"),
        "write_mode": state.get("write_mode"),
        "engine": state.get("engine"),
        "plan": state.get("plan"),
        "layers": {
            layer: {
                "table": info.get("table"),
                "rows": info.get("rows"),
                "sql": info.get("sql"),
                "sql_sha256": hashlib.sha256((info.get("sql") or "").encode()).hexdigest()[:12],
                "dq": info.get("dq"),
            }
            for layer, info in layers.items()
        },
    }
    return manifest


def manifest_json(state: Dict[str, Any]) -> str:
    return json.dumps(build_manifest(state), indent=2, default=str)


def artifact_filename(run_id: str) -> str:
    return f"run_{run_id}_{_now()}.json"
