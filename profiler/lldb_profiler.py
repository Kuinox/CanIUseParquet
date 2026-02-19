"""LLDB plugin for profiling CPython memory allocations.

Load this script into an LLDB session with::

    (lldb) command script import profiler/lldb_profiler.py

It registers a ``profile`` command that sets breakpoints on CPython's
memory allocation functions, captures Python stack traces, sizes, and
timestamps, and writes everything to a Parquet file.

Usage inside LLDB::

    (lldb) profile start [output.parquet]
    (lldb) profile stop

Environment variable ``PROFILER_OUTPUT`` can also specify the output path.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional

# Ensure the package directory is importable
_HERE = os.path.dirname(os.path.abspath(__file__))
_PACKAGE_DIR = os.path.dirname(_HERE)
if _PACKAGE_DIR not in sys.path:
    sys.path.insert(0, _PACKAGE_DIR)

try:
    import lldb  # type: ignore[import-not-found]
except ImportError:
    lldb = None  # type: ignore[assignment]

from profiler.cpython_stacktrace import PythonFrame, get_python_stacktrace
from profiler.parquet_writer import AllocationRecord, ParquetWriter

# ---------------------------------------------------------------------------
# Global profiler state
# ---------------------------------------------------------------------------

_writer: Optional[ParquetWriter] = None
_breakpoint_ids: list[int] = []
_start_time_ns: int = 0

# CPython allocation entry points we instrument.
_ALLOC_FUNCTIONS = [
    # Object domain
    ("_PyObject_Malloc", "malloc"),
    ("_PyObject_Realloc", "realloc"),
    ("_PyObject_Free", "free"),
    # Mem domain
    ("_PyMem_Malloc", "malloc"),
    ("_PyMem_Realloc", "realloc"),
    ("_PyMem_Free", "free"),
    # Public API
    ("PyObject_Malloc", "malloc"),
    ("PyObject_Realloc", "realloc"),
    ("PyObject_Free", "free"),
    ("PyMem_Malloc", "malloc"),
    ("PyMem_Realloc", "realloc"),
    ("PyMem_Free", "free"),
]


def _format_stacktrace(frames: list[PythonFrame]) -> str:
    """Render a list of ``PythonFrame`` objects as a newline-separated string."""
    lines = []
    for f in frames:
        lines.append(f"{f.filename}:{f.lineno} in {f.function}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Breakpoint callbacks
# ---------------------------------------------------------------------------


def _on_alloc(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for malloc / realloc functions."""
    global _writer, _start_time_ns
    if _writer is None:
        return False  # auto-continue

    event = "malloc"
    symbol = frame.GetFunctionName() or ""
    for sym, ev in _ALLOC_FUNCTIONS:
        if sym in symbol:
            event = ev
            break

    # First argument is typically the size (for malloc) or ctx pointer
    size = 0
    if event in ("malloc", "realloc"):
        # For CPython allocators the signature is:
        #   void *_PyObject_Malloc(void *ctx, size_t nbytes)
        # The size is the second argument (index 1).
        size_val = frame.FindVariable("nbytes")
        if not size_val.IsValid():
            # Try positional: arg index 1
            if frame.GetFunction().IsValid():
                args = frame.GetVariables(True, False, False, False)
                if args.GetSize() >= 2:
                    size_val = args.GetValueAtIndex(1)
            if not size_val.IsValid():
                size_val = frame.FindRegister("rsi")  # x86-64 SysV 2nd arg
        if size_val.IsValid():
            size = size_val.GetValueAsUnsigned(0)

    address = 0  # we don't have the return value at entry

    stacktrace = get_python_stacktrace(frame)
    ts = time.time_ns() - _start_time_ns

    _writer.add(
        AllocationRecord(
            timestamp_ns=ts,
            event=event,
            size=size,
            address=address,
            python_stacktrace=_format_stacktrace(stacktrace),
        )
    )
    return False  # auto-continue


def _on_free(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for free functions."""
    global _writer, _start_time_ns
    if _writer is None:
        return False

    address = 0
    ptr_val = frame.FindVariable("p")
    if not ptr_val.IsValid():
        if frame.GetFunction().IsValid():
            args = frame.GetVariables(True, False, False, False)
            if args.GetSize() >= 2:
                ptr_val = args.GetValueAtIndex(1)
        if not ptr_val.IsValid():
            ptr_val = frame.FindRegister("rsi")
    if ptr_val.IsValid():
        address = ptr_val.GetValueAsUnsigned(0)

    stacktrace = get_python_stacktrace(frame)
    ts = time.time_ns() - _start_time_ns

    _writer.add(
        AllocationRecord(
            timestamp_ns=ts,
            event="free",
            size=0,
            address=address,
            python_stacktrace=_format_stacktrace(stacktrace),
        )
    )
    return False


# ---------------------------------------------------------------------------
# LLDB command implementation
# ---------------------------------------------------------------------------


def _profile_command(debugger, command, exe_ctx, result, internal_dict):
    """Handle ``profile start [path]`` / ``profile stop`` commands."""
    args = command.strip().split()
    if not args:
        result.AppendMessage("Usage: profile start [output.parquet] | profile stop")
        return

    subcommand = args[0].lower()
    if subcommand == "start":
        _start_profiling(debugger, args[1:], result)
    elif subcommand == "stop":
        _stop_profiling(debugger, result)
    else:
        result.AppendMessage(
            f"Unknown subcommand '{subcommand}'.  Use 'start' or 'stop'."
        )


def _start_profiling(debugger, args, result):
    global _writer, _breakpoint_ids, _start_time_ns

    if _writer is not None:
        result.AppendMessage("Profiling is already active.  Use 'profile stop' first.")
        return

    output_path = (
        args[0]
        if args
        else os.environ.get("PROFILER_OUTPUT", "profile_output.parquet")
    )

    target = debugger.GetSelectedTarget()
    if not target.IsValid():
        result.AppendMessage("No valid target.  Load a Python binary first.")
        return

    _writer = ParquetWriter(output_path)
    _start_time_ns = time.time_ns()
    _breakpoint_ids.clear()

    for func_name, event_kind in _ALLOC_FUNCTIONS:
        bp = target.BreakpointCreateByName(func_name)
        if not bp.IsValid():
            continue
        bp.SetAutoContinue(True)
        _breakpoint_ids.append(bp.GetID())

        callback = _on_free if event_kind == "free" else _on_alloc
        # Register the callback via the script bridge.  The function
        # must be reachable by its qualified module path.
        cb_name = (
            "profiler.lldb_profiler._on_free"
            if event_kind == "free"
            else "profiler.lldb_profiler._on_alloc"
        )
        bp.SetScriptCallbackFunction(cb_name)

    result.AppendMessage(
        f"Profiling started – {len(_breakpoint_ids)} breakpoints set.  "
        f"Output → {output_path}"
    )


def _stop_profiling(debugger, result):
    global _writer, _breakpoint_ids

    if _writer is None:
        result.AppendMessage("Profiling is not active.")
        return

    target = debugger.GetSelectedTarget()
    for bp_id in _breakpoint_ids:
        target.BreakpointDelete(bp_id)
    _breakpoint_ids.clear()

    _writer.close()
    path = _writer._path
    _writer = None
    result.AppendMessage(f"Profiling stopped.  Data written to {path}")


# ---------------------------------------------------------------------------
# LLDB module entry point
# ---------------------------------------------------------------------------


def __lldb_init_module(debugger, internal_dict):  # noqa: N807
    """Called by LLDB when the script is imported via ``command script import``."""
    debugger.HandleCommand(
        'command script add -f profiler.lldb_profiler._profile_command profile'
    )
    print(
        "python_lldb_profiler loaded.  Use 'profile start [output.parquet]' "
        "to begin and 'profile stop' to finish."
    )
