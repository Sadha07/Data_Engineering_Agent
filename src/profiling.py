"""Data profiling (item: no automatic data profiling).

Generates lightweight profiling SQL and summarises the result so the planner /
builder gets real signal about nulls, cardinality, and ranges before writing
transformations.
"""
from __future__ import annotations

from typing import Any, Dict, List


def profile_sql(table_or_source: str, columns: List[str], is_source: bool = False,
                fmt: str = "csv") -> str:
    """Build a single profiling query returning row count + per-column null counts
    and approx distinct counts."""
    frm = (f"read_files('{table_or_source}', format => '{fmt}')" if is_source
           else table_or_source)
    parts = ["count(*) AS _row_count"]
    for c in columns:
        cq = f"`{c}`"
        parts.append(f"count({cq}) AS `{c}__non_null`")
        parts.append(f"approx_count_distinct({cq}) AS `{c}__distinct`")
    return f"SELECT {', '.join(parts)} FROM {frm}"


def summarise(profile_row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    """Turn the flat profile row into a per-column summary + a compact text blob
    suitable for injecting into an LLM prompt."""
    total = int(profile_row.get("_row_count", 0) or 0)
    cols: Dict[str, Any] = {}
    lines = [f"rows={total}"]
    for c in columns:
        non_null = int(profile_row.get(f"{c}__non_null", 0) or 0)
        distinct = int(profile_row.get(f"{c}__distinct", 0) or 0)
        null_pct = round(100 * (total - non_null) / total, 1) if total else 0.0
        cols[c] = {"null_pct": null_pct, "distinct": distinct}
        flag = "  <-- high nulls" if null_pct > 40 else ""
        uniq = "  <-- likely key" if total and distinct >= 0.98 * total else ""
        lines.append(f"{c}: null%={null_pct} distinct={distinct}{flag}{uniq}")
    return {"total": total, "columns": cols, "text": "\n".join(lines)}
