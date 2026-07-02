"""Streamlit front-end for the medallion data-engineering agent (v2).

Adds controls for: write mode (full/incremental), ingest mode (batch/streaming),
engine (SQL/PySpark), configurable layers, a business-semantics spec, a human
approval gate, and rich data-quality / versioning output.
"""
from __future__ import annotations

import json
import os

import streamlit as st

from src.config import config
from src.pipeline import resume_pipeline, run_pipeline
from src.semantics import parse as parse_sem
from src.uc_tools import get_uc

st.set_page_config(page_title="Medallion DE Agent", page_icon="🏗️", layout="wide")
st.title("Medallion Data Engineering Agent")
st.caption("Raw data + instructions → governed Bronze / Silver / Gold in Unity Catalog.")

with st.sidebar:
    st.header("Unity Catalog")
    catalog = st.text_input("Catalog", value=config.catalog)
    schema = st.text_input("Schema", value=config.schema)
    volume = st.text_input("Volume (raw)", value=config.volume)

    st.header("Pipeline modes")
    write_mode = st.selectbox("Write mode", ["full_refresh", "incremental"],
                              index=0 if config.write_mode == "full_refresh" else 1)
    ingest_mode = st.selectbox("Ingest mode", ["batch", "streaming"],
                               index=0 if config.ingest_mode == "batch" else 1)
    engine = st.selectbox("Engine", ["sql", "pyspark"],
                          index=0 if config.engine == "sql" else 1)
    layers_txt = st.text_input("Layers (ordered)", value=",".join(config.layers))
    require_approval = st.checkbox("Require human approval before running SQL",
                                   value=config.require_approval)

st.subheader("1. Raw data")
mode = st.radio("Source", ["Upload a file", "Existing path / table"], horizontal=True)
raw_source, raw_format, dataset_name = None, "csv", "dataset"

if mode == "Upload a file":
    up = st.file_uploader("Upload CSV / JSON / Parquet", type=["csv", "json", "parquet"])
    if up is not None:
        dataset_name = st.text_input("Dataset name", value=os.path.splitext(up.name)[0])
        raw_format = up.name.split(".")[-1].lower()
        if st.button("Upload to UC Volume"):
            uc = get_uc()
            uc.ensure_namespace(catalog, schema, volume, config.artifacts_volume)
            dest = uc.volume_path(catalog, schema, volume, up.name)
            uc.upload_to_volume(up.getvalue(), dest)
            st.session_state["raw_source"] = dest
            st.success(f"Uploaded to {dest}")
    raw_source = st.session_state.get("raw_source")
else:
    raw_source = st.text_input("Path or table", value=f"/Volumes/{catalog}/{schema}/{volume}/data.csv")
    raw_format = st.selectbox("Format", ["csv", "json", "parquet"])
    dataset_name = st.text_input("Dataset name", value="dataset")

st.subheader("2. Instructions")
instructions = st.text_area("What should the pipeline produce?", height=120,
    placeholder="Clean the orders data, dedupe on order_id, and build gold marts of "
                "revenue by region/month and active-customer counts.")

with st.expander("3. Business semantics (optional — improves correctness)"):
    st.caption('JSON with glossary / metrics / grain / conventions.')
    semantics_txt = st.text_area("Semantics JSON", height=140, value="",
        placeholder='{"glossary": {"active_customer": "purchased in last 90 days"},\n'
                    ' "metrics": {"churn_rate": "1 - retained/starting"},\n'
                    ' "grain": "one row per region per month"}')

st.subheader("4. Run")
if st.button("Build medallion pipeline", type="primary",
             disabled=not (raw_source and instructions)):
    with st.status("Running agent…", expanded=True) as status:
        try:
            result = run_pipeline(
                instructions=instructions, raw_source=raw_source, raw_format=raw_format,
                dataset_name=dataset_name, catalog=catalog, schema=schema,
                semantics=parse_sem(semantics_txt), write_mode=write_mode,
                ingest_mode=ingest_mode, engine=engine,
                layers=[l.strip() for l in layers_txt.split(",") if l.strip()],
            )
            for line in result.get("log", []):
                st.write("•", line)
            status.update(label="Done", state="complete")
        except Exception as e:
            status.update(label="Failed", state="error"); st.error(str(e)); result = None
    st.session_state["result"] = result

result = st.session_state.get("result")
if result:
    if result.get("needs_clarification"):
        st.warning(f"Clarification needed: {result['clarification_question']}")

    if result.get("aborted"):
        st.error(f"Run aborted: {result.get('abort_reason')}")

    # Approval gate: run paused before executing SQL.
    if require_approval and not result.get("done") and not result.get("needs_clarification"):
        st.info("Review the generated SQL, then approve to execute.")
        st.code(result.get("generated_code", ""), language="sql")
        c1, c2 = st.columns(2)
        if c1.button("✅ Approve & run"):
            st.session_state["result"] = resume_pipeline(result["thread_id"], approved=True)
            st.rerun()
        if c2.button("❌ Reject"):
            st.session_state["result"] = resume_pipeline(result["thread_id"], approved=False)
            st.rerun()

    if result.get("plan"):
        st.subheader("Plan"); st.markdown(result["plan"])

    if result.get("layer_results"):
        st.subheader("Layers & data quality")
        for layer, info in result["layer_results"].items():
            with st.expander(f"{layer.title()} — {info.get('table')} ({info.get('rows')} rows)", expanded=True):
                st.code(info.get("sql", ""), language="sql")
                dq = info.get("dq", {})
                for chk in dq.get("checks", []):
                    icon = "✅" if chk["passed"] else "❌"
                    st.write(f"{icon} **{chk['check']}** — {chk['detail']}")

    if result.get("manifest_path"):
        st.caption(f"Run manifest versioned to: {result['manifest_path']}")
