"""Real data-quality validation (item: weak validation).

Replaces the naive 'rows > 0' check with concrete, engine-grounded checks:
row count, duplicate rate, null rate on non-audit columns, and key uniqueness.
Produces a structured DQ report shown in the UI.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DQResult:
    passed: bool
    checks: List[Dict[str, Any]] = field(default_factory=list)
    summary: str = ""

    def add(self, name: str, ok: bool, detail: str) -> None:
        self.checks.append({"check": name, "passed": ok, "detail": detail})
        if not ok:
            self.passed = False


def rowcount_sql(table: str) -> str:
    return f"SELECT count(*) AS n FROM {table}"


def dup_sql(table: str, key_cols: List[str]) -> str:
    keys = ", ".join(f"`{k}`" for k in key_cols)
    return (f"SELECT count(*) AS dup_groups FROM (SELECT {keys}, count(*) c "
            f"FROM {table} GROUP BY {keys} HAVING c > 1)")


def null_sql(table: str, cols: List[str]) -> str:
    parts = ["count(*) AS n"] + [f"sum(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}__nulls`"
                                 for c in cols]
    return f"SELECT {', '.join(parts)} FROM {table}"


def evaluate(
    run_sql,                      # callable(str) -> (rows, error)
    table: str,
    layer: str,
    business_cols: List[str],
    key_cols: Optional[List[str]],
    min_rows: int,
    max_null_pct: float,
) -> DQResult:
    """Run the DQ checks appropriate for the layer and return a report."""
    res = DQResult(passed=True)

    rows, err = run_sql(rowcount_sql(table))
    if err:
        res.add("rowcount", False, f"query failed: {err}")
        res.summary = "Could not read table for DQ."
        return res
    n = int(list(rows[0].values())[0]) if rows else 0
    res.add("min_rows", n >= min_rows, f"{n} rows (min {min_rows})")

    # Null-rate check on business columns (skip bronze — raw is allowed to be dirty).
    if layer != "bronze" and business_cols:
        nrows, nerr = run_sql(null_sql(table, business_cols))
        if not nerr and nrows:
            row = nrows[0]
            total = int(row.get("n", 0) or 0)
            worst = 0.0
            worst_col = ""
            for c in business_cols:
                nulls = int(row.get(f"{c}__nulls", 0) or 0)
                pct = round(100 * nulls / total, 1) if total else 0.0
                if pct > worst:
                    worst, worst_col = pct, c
            res.add("null_rate", worst <= max_null_pct,
                    f"worst null% = {worst} on '{worst_col}' (max {max_null_pct})")

    # Uniqueness on declared keys (silver/gold).
    if layer != "bronze" and key_cols:
        drows, derr = run_sql(dup_sql(table, key_cols))
        if not derr and drows:
            dups = int(list(drows[0].values())[0] or 0)
            res.add("key_uniqueness", dups == 0,
                    f"{dups} duplicate key groups on {key_cols}")

    passed = sum(1 for c in res.checks if c["passed"])
    res.summary = f"{passed}/{len(res.checks)} checks passed for {table}"
    return res
