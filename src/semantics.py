"""Semantic / metrics layer (items: limited business understanding, no semantic
metadata). Lets the user supply a business glossary and metric definitions that
are injected into every layer prompt so the LLM uses the organisation's
definitions instead of guessing.

A semantics spec is a simple dict, e.g.:
{
  "glossary": {"active_customer": "purchased in the last 90 days"},
  "metrics": {"churn_rate": "1 - (retained_customers / customers_at_start)"},
  "grain": "one row per customer per month",
  "conventions": ["amounts are in USD", "dates are UTC"]
}
"""
from __future__ import annotations

import json
from typing import Any, Dict


def render(semantics: Dict[str, Any] | None) -> str:
    """Render the semantics spec into a prompt block. Empty string if none."""
    if not semantics:
        return ""
    out = ["BUSINESS SEMANTICS (authoritative — use these exact definitions):"]
    gl = semantics.get("glossary") or {}
    if gl:
        out.append("Glossary:")
        out += [f"  - {k}: {v}" for k, v in gl.items()]
    me = semantics.get("metrics") or {}
    if me:
        out.append("Metric definitions:")
        out += [f"  - {k} = {v}" for k, v in me.items()]
    if semantics.get("grain"):
        out.append(f"Target grain: {semantics['grain']}")
    conv = semantics.get("conventions") or []
    if conv:
        out.append("Conventions:")
        out += [f"  - {c}" for c in conv]
    return "\n".join(out)


def parse(text: str | None) -> Dict[str, Any]:
    """Parse a JSON semantics spec from the UI; tolerate empty/invalid input."""
    if not text or not text.strip():
        return {}
    try:
        return json.loads(text)
    except Exception:
        # allow plain-text notes as a single convention
        return {"conventions": [text.strip()]}
