"""LangGraph nodes for the medallion pipeline (v2).

Covers: ambiguity clarification, data profiling, multi-gold planning, dynamic
layers, SQL/PySpark engines, full-refresh/incremental (MERGE) & streaming
(Auto Loader) writes, EXPLAIN cost/perf pre-check, human approval gate,
error-classified recovery, real data-quality validation, and run versioning.
"""
from __future__ import annotations

import json
import re
import uuid

from langchain_core.messages import HumanMessage, SystemMessage

from . import dq, errors, profiling, semantics as sem, versioning
from .config import config
from .llm import get_llm
from .state import MedallionState
from .uc_tools import get_uc


# ------------------------------------------------------------- helpers
def _log(state: MedallionState, msg: str) -> list:
    return state.get("log", []) + [msg]


def _extract_code(text: str, lang: str = "sql") -> str:
    m = re.search(rf"```(?:{lang}|python)?\s*(.*?)```", text, re.DOTALL)
    return (m.group(1) if m else text).strip().rstrip(";")


def _extract_json(text: str):
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    raw = m.group(1) if m else text
    try:
        return json.loads(raw)
    except Exception:
        s, e = raw.find("["), raw.rfind("]")
        if s != -1 and e != -1:
            try:
                return json.loads(raw[s:e + 1])
            except Exception:
                return None
    return None


def _targets_for_layer(state: MedallionState, layer: str):
    """Return list of (table, description) targets for the layer.
    Gold may expand into multiple marts."""
    c, s, name = state["catalog"], state["schema"], state["dataset_name"]
    if layer == "gold" and state.get("gold_tables"):
        return [(t, "") for t in state["gold_tables"]]
    return [(f"{c}.{s}.{layer}_{name}", "")]


# --------------------------------------------------- 1. clarify (ambiguity)
def clarify_node(state: MedallionState) -> MedallionState:
    llm = get_llm()
    resp = llm.invoke([
        SystemMessage(content=(
            "You gate a data-engineering agent. Decide if the user's instruction is "
            "specific enough to build a bronze/silver/gold pipeline (are the target "
            "metrics/grain clear?). Reply strictly as JSON: "
            '{"clear": true|false, "question": "single clarifying question or empty"}')),
        HumanMessage(content=(
            f"Instruction:\n{state['instructions']}\n\n"
            f"Raw columns: {state.get('raw_schema','(unknown)')}")),
    ])
    data = _extract_json(resp.content) or {"clear": True, "question": ""}
    if not data.get("clear", True) and data.get("question"):
        return {"needs_clarification": True,
                "clarification_question": data["question"],
                "log": _log(state, f"Needs clarification: {data['question']}")}
    return {"needs_clarification": False,
            "log": _log(state, "Instruction is specific enough; proceeding.")}


# --------------------------------------------- 2. setup + profile + schema
def setup_node(state: MedallionState) -> MedallionState:
    uc = get_uc()
    uc.ensure_namespace(state["catalog"], state["schema"],
                        config.volume, config.artifacts_volume)
    cols = uc.infer_columns(state["raw_source"], state["raw_format"])
    raw_schema = ", ".join(cols) if cols else "(schema inference failed)"

    profile = {}
    if config.enable_profiling and cols:
        psql = profiling.profile_sql(state["raw_source"], cols, is_source=True,
                                     fmt=state["raw_format"])
        rows, err = uc.run_sql(psql)
        if not err and rows:
            profile = profiling.summarise(rows[0], cols)

    return {"raw_schema": raw_schema, "profile": profile,
            "log": _log(state, f"UC ready. Columns: {raw_schema}. "
                               f"Profiled {profile.get('total','?')} rows.")}


# ------------------------------------------------- 3. planner (multi-gold)
def planner_node(state: MedallionState) -> MedallionState:
    llm = get_llm()
    sem_block = sem.render(state.get("semantics"))
    prof = state.get("profile", {}).get("text", "")
    resp = llm.invoke([
        SystemMessage(content=(
            "You are a lead data engineer planning a medallion pipeline on Databricks "
            "Unity Catalog. Given the instruction, raw columns, a data profile and "
            "business semantics, produce JSON:\n"
            '{"plan": "<=10 line prose plan", '
            '"gold_marts": ["snake_case_mart_name", ...], '
            '"keys": {"silver": ["col"], "gold": ["col"]}}\n'
            "gold_marts: one or more business tables to build in the gold layer.")),
        HumanMessage(content=(
            f"Instruction:\n{state['instructions']}\n\n"
            f"Raw columns: {state.get('raw_schema','')}\n\n"
            f"Profile:\n{prof}\n\n{sem_block}")),
    ])
    data = _extract_json(resp.content) or {}
    c, s, name = state["catalog"], state["schema"], state["dataset_name"]
    marts = data.get("gold_marts") or [name]
    gold_tables = [f"{c}.{s}.gold_{re.sub(r'[^a-z0-9_]+','_', m.lower())}" for m in marts]
    layers = state.get("layers") or config.layers
    return {
        "plan": data.get("plan", resp.content),
        "gold_tables": gold_tables,
        "key_columns": data.get("keys", {}),
        "iteration": 0,
        "max_iterations": config.max_iterations,
        "current_layer": layers[0],
        "layer_results": {},
        "log": _log(state, f"Planned. Gold marts: {gold_tables}"),
    }


# ------------------------------------------------------ 4. build layer(s)
def _build_prompt(state, layer, target, prior_err, feedback):
    mode = state.get("write_mode", config.write_mode)
    ingest = state.get("ingest_mode", config.ingest_mode)
    engine = state.get("engine", config.engine)
    keys = (state.get("key_columns") or {}).get(layer, [])
    sem_block = sem.render(state.get("semantics"))
    prof = state.get("profile", {}).get("text", "")

    if layer == "bronze":
        if ingest == "streaming":
            how = (f"Create a STREAMING TABLE {target} using Auto Loader: "
                   f"CREATE OR REFRESH STREAMING TABLE {target} AS SELECT *, "
                   f"current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file "
                   f"FROM STREAM read_files('{state['raw_source']}', format => '{state['raw_format']}').")
        else:
            how = (f"CREATE OR REPLACE TABLE {target} AS SELECT *, current_timestamp() AS "
                   f"_ingested_at, _metadata.file_path AS _source_file FROM "
                   f"read_files('{state['raw_source']}', format => '{state['raw_format']}'). "
                   f"No cleaning.")
    elif layer == "silver":
        src = f"{state['catalog']}.{state['schema']}.bronze_{state['dataset_name']}"
        base = (f"Clean {src}: cast types, trim strings, drop duplicates, filter null keys. "
                f"Keep audit columns.")
        how = _write_clause(mode, target, src, keys, base)
    else:  # gold
        src = f"{state['catalog']}.{state['schema']}.silver_{state['dataset_name']}"
        base = (f"Build the business aggregate '{target}' from {src} per the instruction "
                f"and semantics.")
        how = _write_clause(mode, target, src, keys, base)

    engine_rule = ("Return ONE valid Databricks SQL statement in a ```sql block."
                   if engine == "sql" else
                   "Return a PySpark snippet in a ```python block using spark.sql / DataFrame "
                   f"API that writes to the Delta table {target} (saveAsTable).")
    system = ("You are an expert Databricks developer writing Unity Catalog Delta code. "
              "Use fully-qualified three-level names. No comments. " + engine_rule)
    user = (f"Instruction:\n{state['instructions']}\n\nRaw columns: {state.get('raw_schema','')}\n"
            f"Profile:\n{prof}\n\n{sem_block}\n\n"
            f"TASK ({layer.upper()} · mode={mode} · keys={keys}):\n{how}")
    if prior_err:
        user += f"\n\nPrevious attempt FAILED:\n{prior_err}\nReturn corrected code."
    if feedback:
        user += f"\n\nData-quality feedback to fix:\n{feedback}"
    return system, user


def _write_clause(mode, target, src, keys, base):
    if mode == "incremental" and keys:
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        return (f"{base} Use an idempotent MERGE: first CREATE TABLE IF NOT EXISTS {target} "
                f"with the right schema, then MERGE INTO {target} t USING (<transformed "
                f"SELECT from {src}>) s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT "
                f"MATCHED THEN INSERT *. Return the MERGE statement.")
    return f"{base} Use CREATE OR REPLACE TABLE {target} AS SELECT ... FROM {src}."


def build_layer_node(state: MedallionState) -> MedallionState:
    layer = state["current_layer"]
    engine = state.get("engine", config.engine)
    llm = get_llm()
    uc = get_uc()
    targets = _targets_for_layer(state, layer)
    prior_err = state.get("execution_error", "")
    feedback = state.get("validation_feedback", "")

    built = []          # (table, code, output)
    first_err = ""
    for target, _ in targets:
        system, user = _build_prompt(state, layer, target, prior_err, feedback)
        resp = llm.invoke([SystemMessage(content=system), HumanMessage(content=user)])
        code = _extract_code(resp.content, "sql" if engine == "sql" else "python")

        # EXPLAIN cost/perf + correctness pre-check (SQL engine only).
        if engine == "sql" and config.run_explain and code.lower().startswith(("create", "merge", "select")):
            ok, plan = uc.explain(code)
            if not ok:
                first_err = first_err or f"EXPLAIN failed for {target}: {plan}"
                built.append((target, code, ""))
                continue
        built.append((target, code, ""))

    return {
        "generated_code": "\n\n".join(f"-- {t}\n{c}" for t, c, _ in built),
        "_targets_built": built,   # transient, consumed by executor
        "execution_error": first_err,
        "log": _log(state, f"[{layer}] generated {len(built)} statement(s)"
                           + (f"; EXPLAIN error: {first_err}" if first_err else "")),
    }


# -------------------------------------------------------- 5. approval gate
def approval_node(state: MedallionState) -> MedallionState:
    """Human-in-the-loop: with a checkpointer + interrupt_before, the graph pauses
    here so a human can inspect state['generated_code'] and approve. Auto-approve
    when REQUIRE_APPROVAL is off."""
    if not config.require_approval:
        return {"approved": True}
    return {"approved": bool(state.get("approved", False))}


# ------------------------------------------------------------ 6. executor
def executor_node(state: MedallionState) -> MedallionState:
    layer = state["current_layer"]
    engine = state.get("engine", config.engine)
    built = state.get("_targets_built", [])

    # If EXPLAIN already flagged an error, skip execution and let validation route it.
    if state.get("execution_error"):
        return {"iteration": state.get("iteration", 0) + 1}

    if engine == "pyspark":
        # Persist generated PySpark to workspace + note that execution needs a cluster/job.
        return {"execution_output": "PySpark code generated (run via Jobs on a cluster).",
                "execution_error": "",
                "iteration": state.get("iteration", 0) + 1,
                "log": _log(state, f"[{layer}] PySpark generated for {len(built)} target(s)")}

    uc = get_uc()
    outputs = []
    err = ""
    for target, code, _ in built:
        _, e = uc.run_sql(code)
        if e:
            err = e
            break
        n = uc.count_rows(target)
        outputs.append(f"{target}: {n} rows")
    return {
        "execution_output": "; ".join(outputs),
        "execution_error": err,
        "error_kind": errors.classify(err).kind if err else "",
        "iteration": state.get("iteration", 0) + 1,
        "log": _log(state, f"[{layer}] executed -> {err or '; '.join(outputs)}"),
    }


# ------------------------------------------------- 7. validate (real DQ)
def validate_layer_node(state: MedallionState) -> MedallionState:
    layer = state["current_layer"]
    err = state.get("execution_error", "")
    if err:
        cls = errors.classify(err)
        if cls.should_abort:
            return {"is_valid": False, "aborted": True, "abort_reason": cls.hint,
                    "validation_feedback": f"{cls.kind}: {err}",
                    "log": _log(state, f"[{layer}] ABORT ({cls.kind}): {cls.hint}")}
        return {"is_valid": False, "validation_feedback": f"{cls.kind} error: {err}",
                "log": _log(state, f"[{layer}] invalid ({cls.kind}); will {'retry' if cls.should_retry else 'regenerate'}")}

    if state.get("engine") == "pyspark":
        return {"is_valid": True, "validation_feedback": "",
                "log": _log(state, f"[{layer}] pyspark artifact accepted (execute via Jobs)")}

    uc = get_uc()
    keys = (state.get("key_columns") or {}).get(layer, [])
    results = dict(state.get("layer_results", {}))
    reports = {}
    all_ok = True
    for target, _, _ in state.get("_targets_built", []):
        biz_cols = [c for c in uc.table_columns(target) if not c.startswith("_")]
        r = dq.evaluate(uc.run_sql, target, layer, biz_cols, keys or None,
                        config.min_rows, config.max_null_pct)
        reports[target] = {"summary": r.summary, "checks": r.checks, "passed": r.passed}
        all_ok = all_ok and r.passed
        code = next((c for t, c, _ in state.get("_targets_built", []) if t == target), "")
        results.setdefault(layer, {})
        results[layer] = {"table": target, "rows": uc.count_rows(target),
                          "sql": code, "dq": reports[target]}

    if not all_ok:
        fails = [f"{t}: {[c for c in rep['checks'] if not c['passed']]}"
                 for t, rep in reports.items() if not rep["passed"]]
        return {"is_valid": False, "dq_report": reports,
                "validation_feedback": "DQ failed -> " + " | ".join(fails),
                "log": _log(state, f"[{layer}] DQ failed")}
    return {"is_valid": True, "dq_report": reports, "layer_results": results,
            "validation_feedback": "",
            "log": _log(state, f"[{layer}] DQ passed: " +
                        "; ".join(r["summary"] for r in reports.values()))}


# ---------------------------------------------------------- 8. advance
def advance_node(state: MedallionState) -> MedallionState:
    layers = state.get("layers") or config.layers
    idx = layers.index(state["current_layer"])
    nxt = layers[idx + 1]
    return {"current_layer": nxt, "iteration": 0, "execution_error": "",
            "validation_feedback": "", "log": _log(state, f"Advancing to {nxt}.")}


# ---------------------------------------------------------- 9. finalize
def finalize_node(state: MedallionState) -> MedallionState:
    # Version the run (plan + per-layer SQL + DQ) to the artifacts volume.
    path = ""
    try:
        uc = get_uc()
        fname = versioning.artifact_filename(state.get("run_id", "run"))
        path = uc.write_artifact(state["catalog"], state["schema"], fname,
                                 versioning.manifest_json(state))
    except Exception as e:
        return {"done": True, "log": _log(state, f"Finalize (manifest save skipped: {e})")}
    return {"done": True, "manifest_path": path,
            "log": _log(state, f"Finished. Manifest: {path}")}


# ------------------------------------------------------- routing (edges)
def route_after_clarify(state: MedallionState) -> str:
    return "ask" if state.get("needs_clarification") else "continue"


def route_after_approval(state: MedallionState) -> str:
    return "approved" if state.get("approved") else "rejected"


def route_after_validation(state: MedallionState) -> str:
    if state.get("aborted"):
        return "give_up"
    layers = state.get("layers") or config.layers
    if state.get("is_valid"):
        return "advance" if state["current_layer"] != layers[-1] else "finish"
    if state.get("iteration", 0) >= state.get("max_iterations", config.max_iterations):
        return "give_up"
    return "retry"
