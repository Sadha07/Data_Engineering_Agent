"""Error classification for smarter recovery.

Distinguishes transient/infra errors (retry with backoff, or abort cleanly)
from logical errors (regenerate the SQL). Addresses: 'retries cannot fix all
failures' and 'limited infrastructure-level error recovery'.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

TRANSIENT = "transient"     # network / timeout / warehouse waking — retry same SQL
INFRA = "infra"             # permissions / warehouse down / UC outage — abort, tell user
LOGICAL = "logical"         # SQL syntax / semantic — regenerate SQL
MISSING_DATA = "missing"    # source path/table not found — abort, tell user
UNKNOWN = "unknown"


@dataclass
class Classified:
    kind: str
    should_regenerate: bool   # ask the LLM to rewrite the SQL
    should_retry: bool        # retry the same statement (transient)
    should_abort: bool        # stop the run and surface to the user
    hint: str                 # human-readable guidance


_PATTERNS = [
    (INFRA, r"(permission|not authorized|access denied|forbidden|privilege|unauthorized|"
            r"insufficient privileges|does not have)"),
    (INFRA, r"(warehouse.*(stopped|not running|deleted)|cluster.*terminat)"),
    (MISSING_DATA, r"(path does not exist|no such file|file not found|table or view not found|"
                   r"cannot find|does not exist)"),
    (TRANSIENT, r"(timed out|timeout|temporarily unavailable|rate limit|429|503|502|"
                r"connection reset|deadline exceeded)"),
    (LOGICAL, r"(syntax error|parse|unresolved|cannot resolve|type mismatch|"
              r"ambiguous|invalid|analysisexception|datatype)"),
]


def classify(error: str) -> Classified:
    e = (error or "").lower()
    for kind, pat in _PATTERNS:
        if re.search(pat, e):
            if kind == TRANSIENT:
                return Classified(kind, False, True, False,
                                  "Transient issue — retrying the same statement with backoff.")
            if kind == INFRA:
                return Classified(kind, False, False, True,
                                  "Infrastructure/permission problem — grant the service "
                                  "principal access or start the warehouse, then re-run.")
            if kind == MISSING_DATA:
                return Classified(kind, False, False, True,
                                  "Source data not found — check the raw path/table.")
            if kind == LOGICAL:
                return Classified(kind, True, False, False,
                                  "Logical/SQL error — regenerating the statement.")
    # default: treat as logical so the LLM gets one chance to fix it
    return Classified(UNKNOWN, True, False, False,
                      "Unclassified error — attempting to regenerate the statement.")
