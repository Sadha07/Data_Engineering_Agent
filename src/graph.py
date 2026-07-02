"""LangGraph wiring (v2): clarification gate, profiling, multi-gold layer engine,
human-approval interrupt, checkpointing, and error-classified routing.
"""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from .config import config
from .medallion_nodes import (
    advance_node, approval_node, build_layer_node, clarify_node, executor_node,
    finalize_node, planner_node, route_after_clarify, route_after_validation,
    setup_node, validate_layer_node,
)
from .state import MedallionState


def build_graph(checkpointer=None):
    g = StateGraph(MedallionState)

    g.add_node("clarify", clarify_node)
    g.add_node("setup", setup_node)
    g.add_node("planner", planner_node)
    g.add_node("build_layer", build_layer_node)
    g.add_node("approval", approval_node)
    g.add_node("executor", executor_node)
    g.add_node("validate_layer", validate_layer_node)
    g.add_node("advance", advance_node)
    g.add_node("finalize", finalize_node)

    g.set_entry_point("clarify")
    g.add_conditional_edges("clarify", route_after_clarify,
                            {"ask": END, "continue": "setup"})
    g.add_edge("setup", "planner")
    g.add_edge("planner", "build_layer")
    g.add_edge("build_layer", "approval")
    g.add_edge("approval", "executor")           # interrupt sits before executor
    g.add_edge("executor", "validate_layer")
    g.add_conditional_edges("validate_layer", route_after_validation, {
        "retry": "build_layer",
        "advance": "advance",
        "finish": "finalize",
        "give_up": "finalize",
    })
    g.add_edge("advance", "build_layer")
    g.add_edge("finalize", END)

    # Human-in-the-loop: pause before executor so a human can inspect the SQL.
    interrupt_before = ["executor"] if config.require_approval else None
    return g.compile(checkpointer=checkpointer, interrupt_before=interrupt_before)


def get_checkpointer():
    """SQLite checkpointer for resumable runs (item: stateless runs)."""
    try:
        from langgraph.checkpoint.sqlite import SqliteSaver
        import sqlite3
        conn = sqlite3.connect(config.checkpoint_path, check_same_thread=False)
        return SqliteSaver(conn)
    except Exception:
        try:
            from langgraph.checkpoint.memory import MemorySaver
            return MemorySaver()
        except Exception:
            return None
