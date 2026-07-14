#!/usr/bin/env python3
"""Shared utilities for Daft OOM reproduction project."""

from __future__ import annotations


def classify_error(e: Exception) -> str:
    """Classify an exception as OOM or other error.

    Checks the full exception chain used by Ray + Daft:
      type(e).__name__  →  e.cause  →  e.__cause__  →  str(e)

    Ray memory monitor kills produce RayTaskError(.cause=OutOfMemoryError),
    NOT ActorDiedError.
    """
    etype = type(e).__name__
    emsg = str(e)

    # Direct type check
    if "OutOfMemory" in etype:
        return "OOM"

    # Check .cause (RayTaskError wraps the real error in .cause)
    cause = getattr(e, "cause", None)
    if cause is not None:
        cause_type = type(cause).__name__
        cause_msg = str(cause)
        if "OutOfMemory" in cause_type:
            return "OOM"
        if "low on memory" in cause_msg.lower():
            return "OOM"
        if "ActorDied" in cause_type:
            return "OOM"

    # Check __cause__ chain
    inner = getattr(e, "__cause__", None)
    if inner is not None and "OutOfMemory" in type(inner).__name__:
        return "OOM"

    # Check message for memory kill patterns
    if "low on memory" in emsg.lower():
        return "OOM"
    if "memory on the node" in emsg.lower():
        return "OOM"
    if "ActorDied" in etype:
        return "OOM"

    # RayTaskError wrapping something OOM-related
    if etype == "RayTaskError" and "OutOfMemory" in emsg:
        return "OOM"

    return f"ERROR({etype})"
