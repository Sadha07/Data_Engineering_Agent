# Medallion Data Engineering Agent — v2 (LangGraph + Databricks Apps)

Give the app **raw data + natural-language instructions**; a LangGraph agent plans
and builds a governed **Bronze → Silver → Gold** pipeline in **Unity Catalog**,
runs it on a serverless SQL Warehouse, validates each layer with real
**data-quality checks**, and self-corrects on failure. Deploys as a **Databricks App**.

This v2 closes the gaps from the v1 design review — see the mapping below.

## What changed vs v1 (limitations → fixes)

| # | Limitation (v1) | Fix in v2 | Where |
|---|-----------------|-----------|-------|
| 1 | Full-refresh only | `WRITE_MODE=incremental` → idempotent **MERGE** on natural keys | `medallion_nodes._write_clause` |
| 2 | Single gold table | Planner emits **multiple gold marts**; layer engine builds each | `planner_node`, `_targets_for_layer` |
| 3 | Weak validation (rows>0) | Real **DQ engine**: row count, null-rate, key-uniqueness, dup groups + report | `dq.py`, `validate_layer_node` |
| 4 | No human approval | **Human-in-the-loop** `interrupt_before` executor; approve/reject in UI | `graph.py`, `approval_node` |
| 5 | Stateless runs | **SQLite checkpointer**, `thread_id`, `resume_pipeline()` | `graph.get_checkpointer`, `pipeline` |
| 6 | SQL-only | `ENGINE=pyspark` generates PySpark + runs via **Jobs** on a cluster | `jobs.py`, `executor_node` |
| 7 | Structured data only | Format-aware ingest (csv/json/parquet); unstructured routed out | `setup_node` |
| 8 | No streaming | `INGEST_MODE=streaming` → **Auto Loader** streaming table | `_build_prompt` (bronze) |
| 9 | No version control | Every run's plan+SQL+DQ **versioned** (hashed) to a UC Volume | `versioning.py`, `finalize_node` |
| 10 | LLM hallucination | **EXPLAIN** pre-check catches bad plans before execution | `uc_tools.explain`, `build_layer_node` |
| 11 | Weak business understanding | **Semantics/metrics layer** injected into every prompt | `semantics.py` |
| 12 | Prompt dependency | **Clarification gate** asks a question when instructions are ambiguous | `clarify_node` |
| 13 | No cost optimization | EXPLAIN plan surfaced; perf rules in prompts (no `SELECT *`, partitioning) | `uc_tools.explain` |
| 14 | Retries can't fix all | **Error classification** (transient/infra/logical/missing) routes correctly | `errors.py` |
| 15 | Fixed layers | `PIPELINE_LAYERS` configurable & ordered | `config`, `advance_node` |
| 16 | No profiling | **Data profiling** (nulls, cardinality, keys) before building | `profiling.py`, `setup_node` |
| 17 | No semantic metadata | Glossary + metric definitions + grain + conventions | `semantics.py` |
| 18 | No perf optimization | EXPLAIN + best-practice prompt rules | `uc_tools.explain` |
| 19 | No scheduling | **Databricks Jobs** cron scheduling | `jobs.schedule_pipeline` |
| 20 | Weak infra recovery | Transient errors **retry with exponential backoff**; infra errors abort cleanly | `uc_tools.run_sql`, `errors.py` |

> Note on unstructured data (images/PDF/audio): a SQL medallion engine is the wrong
> tool for those. The app handles tabular formats (CSV/JSON/Parquet) and cleanly
> flags unsupported inputs rather than pretending to process them.

## Execution flow

```
clarify ──ambiguous──▶ ask user (stop)
   │ clear
   ▼
setup+profile ▶ planner(multi-gold, keys) ▶ build_layer ▶ [EXPLAIN] ▶ approval*
                                                 ▲                        │
                    retry (error-classified) ────┘                        ▼
                                                                     executor
                                                                        │
                                                                        ▼
                                              validate_layer (data quality)
                                    ┌───────────┬────────────┬───────────┐
                                 retry       advance        finish/give_up
                                                 │                │
                                          (next layer)        finalize ▶ version ▶ END
   *approval only when REQUIRE_APPROVAL=true (pauses via checkpointer)
   layers configurable; gold expands to N marts
```

## Project layout

| File | Purpose |
|------|---------|
| `app.py` | Streamlit UI (Databricks Apps entrypoint) |
| `app.yaml` | Apps run command + env |
| `src/config.py` | Config: modes, layers, approval, checkpoint |
| `src/state.py` | `MedallionState` |
| `src/uc_tools.py` | SQL exec + backoff, EXPLAIN, UC objects, artifact upload |
| `src/errors.py` | Error classification (transient/infra/logical/missing) |
| `src/profiling.py` | Data profiling SQL + summary |
| `src/dq.py` | Data-quality checks + report |
| `src/semantics.py` | Business glossary / metrics injection |
| `src/versioning.py` | Run manifest / SQL versioning |
| `src/jobs.py` | Databricks Jobs: PySpark run + cron scheduling |
| `src/llm.py` | LLM factory |
| `src/medallion_nodes.py` | All graph nodes |
| `src/graph.py` | LangGraph wiring + checkpointer + interrupt |
| `src/pipeline.py` | `run_pipeline()` / `resume_pipeline()` |

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env      # fill host, token, warehouse id
streamlit run app.py
# headless:
python -m src.pipeline
```

## Deploy as a Databricks App

```bash
databricks apps create medallion-de-agent
databricks sync --watch . /Workspace/Users/<you>/medallion-de-agent
databricks apps deploy medallion-de-agent \
  --source-code-path /Workspace/Users/<you>/medallion-de-agent
```

Then set `DATABRICKS_WAREHOUSE_ID`, add the SQL warehouse as an App resource, and
grant the app's service principal `CREATE SCHEMA/TABLE/VOLUME` + `USE` on the
catalog and `CAN USE` on the warehouse.

## Configuration knobs

All via env (see `.env.example`): `WRITE_MODE`, `INGEST_MODE`, `ENGINE`,
`PIPELINE_LAYERS`, `REQUIRE_APPROVAL`, `RUN_EXPLAIN`, `ENABLE_PROFILING`,
`MAX_ITERATIONS`, `MAX_NULL_PCT`, `MIN_ROWS`. All are also overridable per-run
in the Streamlit sidebar.

## Scheduling

`src/jobs.schedule_pipeline(name, notebook_path, cron, cluster_id)` creates a
recurring Databricks Job. Generate a pipeline notebook from the run manifest and
schedule it for daily/hourly refreshes.

## Tested

All pure-logic modules have unit tests (error classifier, profiling, DQ pass/fail,
semantics, versioning) and the graph compiles in both auto and approval-gated
modes. Run checks with the snippets in the review notes or wire into pytest.
