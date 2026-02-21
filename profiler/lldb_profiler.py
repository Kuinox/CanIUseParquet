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
import platform
import sys
import time
from typing import Optional

# Ensure the package directory is importable
_HERE = os.path.dirname(os.path.abspath(__file__))
_PACKAGE_DIR = os.path.dirname(_HERE)
if _PACKAGE_DIR not in sys.path:
    sys.path.insert(0, _PACKAGE_DIR)

import lldb  # type: ignore[import-not-found]

from profiler.cpython_stacktrace import PythonFrame, get_python_stacktrace
from profiler.parquet_writer import AllocationRecord, ParquetWriter

# ---------------------------------------------------------------------------
# Global profiler state
# ---------------------------------------------------------------------------

_writer: Optional[ParquetWriter] = None
_breakpoint_ids: list[int] = []
_start_time_ns: int = 0
_canary_in_progress: bool = False
_canary_hit_count: int = 0

# The module name as seen by LLDB after ``command script import``.
# LLDB registers the script using just the filename stem (e.g.
# ``lldb_profiler``), so we detect it at import time.
_MODULE_NAME: str = __name__

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


def _run_canary(debugger, target, result):
    """Launch a canary Python script to verify hooks are working.

    Runs a tiny script (``python -c "x=[0]*100"``) that triggers CPython
    allocations.  During the canary, breakpoint auto-continue is disabled
    and callbacks return True (stop) so we can stop after just a few hits
    instead of processing every allocation.  This keeps the canary fast
    (seconds, not minutes).  The canary data never enters the final trace.
    """
    global _canary_in_progress, _canary_hit_count, _start_time_ns

    _canary_in_progress = True
    _canary_hit_count = 0

    was_async = debugger.GetAsync()
    debugger.SetAsync(False)

    # Disable auto-continue so we can stop after a few hits.
    for bp_id in _breakpoint_ids:
        bp = target.FindBreakpointByID(bp_id)
        if bp.IsValid():
            bp.SetAutoContinue(False)

    error = lldb.SBError()
    launch_info = lldb.SBLaunchInfo(["-c", "x=[0]*100"])
    process = target.Launch(launch_info, error)

    if error.Fail() or not process or not process.IsValid():
        result.AppendMessage(
            f"⚠  Canary: could not launch process — {error.GetCString()}"
        )
        _canary_in_progress = False
        # Restore auto-continue.
        for bp_id in _breakpoint_ids:
            bp = target.FindBreakpointByID(bp_id)
            if bp.IsValid():
                bp.SetAutoContinue(True)
        debugger.SetAsync(was_async)
        return

    # Continue until we see a few breakpoint hits, then stop early.
    _CANARY_MIN_HITS = 5
    deadline = time.time() + 15
    while process.GetState() == lldb.eStateStopped:
        if _canary_hit_count >= _CANARY_MIN_HITS:
            break
        err = process.Continue()
        if err.Fail():
            break
        if time.time() > deadline:
            break

    # Kill the canary process (no need to wait for full exit).
    if process.GetState() != lldb.eStateExited:
        process.Kill()

    _canary_in_progress = False

    # Restore auto-continue for the real profiling run.
    for bp_id in _breakpoint_ids:
        bp = target.FindBreakpointByID(bp_id)
        if bp.IsValid():
            bp.SetAutoContinue(True)

    debugger.SetAsync(was_async)

    if _canary_hit_count > 0:
        result.AppendMessage(
            f"✓  Canary: {_canary_hit_count} allocation breakpoints hit "
            f"— hooks are working."
        )
    else:
        result.AppendMessage(
            "⚠  Canary: no allocation breakpoints were hit — hooks may "
            "not be working.\n"
            "  Possible causes:\n"
            "  • On Windows: try targeting the base Python interpreter "
            "instead of a venv python.exe\n"
            "  • Symbols may not be available in a stripped binary"
        )

    # Reset the start time so user timestamps begin after the canary.
    _start_time_ns = time.time_ns()


# ---------------------------------------------------------------------------
# Breakpoint callbacks
# ---------------------------------------------------------------------------


def _on_alloc(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for malloc / realloc functions."""
    global _writer, _start_time_ns, _canary_hit_count
    if _canary_in_progress:
        _canary_hit_count += 1
        return True  # stop during canary so we can limit hits
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
        # Internal CPython allocators: void *_PyObject_Malloc(void *ctx, size_t nbytes)
        #   size is 2nd arg (index 1).
        # Public API: void *PyObject_Malloc(size_t size)
        #   size is 1st arg (index 0).
        size_val = frame.FindVariable("nbytes")
        if not size_val.IsValid():
            size_val = frame.FindVariable("size")
        if not size_val.IsValid():
            size_val = frame.FindVariable("new_size")
        if not size_val.IsValid():
            if frame.GetFunction().IsValid():
                args = frame.GetVariables(True, False, False, False)
                is_internal = symbol.startswith("_Py")
                if event == "realloc":
                    # Realloc: void *PyObject_Realloc(void *p, size_t size)
                    #          void *_PyObject_Realloc(void *ctx, void *p, size_t nbytes)
                    arg_idx = 2 if is_internal and args.GetSize() >= 3 else 1
                else:
                    arg_idx = 1 if is_internal and args.GetSize() >= 2 else 0
                if args.GetSize() > arg_idx:
                    size_val = args.GetValueAtIndex(arg_idx)
            if not size_val.IsValid():
                # Register-based fallback when debug info is unavailable.
                # x86-64 SysV ABI (Linux/macOS): rdi, rsi, rdx, rcx, …
                # Microsoft x64 ABI (Windows):   rcx, rdx, r8,  r9, …
                is_internal = symbol.startswith("_Py")
                if platform.system() == "Windows":
                    # Microsoft x64: rcx=1st, rdx=2nd, r8=3rd
                    if event == "realloc":
                        reg = "r8" if is_internal else "rdx"
                    else:
                        reg = "rdx" if is_internal else "rcx"
                else:
                    # SysV (Linux/macOS): rdi=1st, rsi=2nd, rdx=3rd
                    if event == "realloc":
                        reg = "rdx" if is_internal else "rsi"
                    else:
                        reg = "rsi" if is_internal else "rdi"
                size_val = frame.FindRegister(reg)
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
    global _writer, _start_time_ns, _canary_hit_count
    if _canary_in_progress:
        _canary_hit_count += 1
        return True  # stop during canary so we can limit hits
    if _writer is None:
        return False

    symbol = frame.GetFunctionName() or ""

    address = 0
    ptr_val = frame.FindVariable("p")
    if not ptr_val.IsValid():
        ptr_val = frame.FindVariable("ptr")
    if not ptr_val.IsValid():
        if frame.GetFunction().IsValid():
            args = frame.GetVariables(True, False, False, False)
            is_internal = symbol.startswith("_Py")
            # Internal: void _PyObject_Free(void *ctx, void *p) → p is arg 1
            # Public:   void PyObject_Free(void *p) → p is arg 0
            arg_idx = 1 if is_internal and args.GetSize() >= 2 else 0
            if args.GetSize() > arg_idx:
                ptr_val = args.GetValueAtIndex(arg_idx)
        if not ptr_val.IsValid():
            # Register-based fallback when debug info is unavailable.
            # x86-64 SysV ABI (Linux/macOS): rdi, rsi, rdx, rcx, …
            # Microsoft x64 ABI (Windows):   rcx, rdx, r8,  r9, …
            is_internal = symbol.startswith("_Py")
            if platform.system() == "Windows":
                reg = "rdx" if is_internal else "rcx"
            else:
                reg = "rsi" if is_internal else "rdi"
            ptr_val = frame.FindRegister(reg)
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

    resolved_count = 0
    pending_count = 0
    failed_names = []

    for func_name, event_kind in _ALLOC_FUNCTIONS:
        bp = target.BreakpointCreateByName(func_name)
        if not bp.IsValid():
            failed_names.append(func_name)
            continue
        bp.SetAutoContinue(True)
        _breakpoint_ids.append(bp.GetID())

        if bp.GetNumLocations() > 0:
            resolved_count += 1
        else:
            pending_count += 1

        # Register the callback via the script bridge.  The function
        # must be reachable by its module name as seen by LLDB.
        cb_name = (
            f"{_MODULE_NAME}._on_free"
            if event_kind == "free"
            else f"{_MODULE_NAME}._on_alloc"
        )
        bp.SetScriptCallbackFunction(cb_name)

    result.AppendMessage(
        f"Profiling started – {len(_breakpoint_ids)} breakpoints set "
        f"({resolved_count} resolved, {pending_count} pending).  "
        f"Output → {output_path}"
    )
    if pending_count > 0:
        result.AppendMessage(
            f"  {pending_count} breakpoints are pending — they will resolve "
            f"when the target's shared libraries are loaded (after 'run')."
        )
        if platform.system() == "Windows":
            result.AppendMessage(
                "  Note (Windows): internal _Py* symbols are not exported "
                "from python3XX.dll.  Only public API breakpoints "
                "(PyMem_Malloc, PyObject_Malloc, etc.) will resolve."
            )
    if resolved_count == 0 and pending_count == 0:
        result.AppendMessage(
            "⚠  No breakpoints could be set.  Make sure the target is a "
            "CPython binary with allocation symbols."
        )

    # Run a canary: launch a tiny Python script to verify hooks work.
    _run_canary(debugger, target, result)


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
    total = _writer._total_records
    _writer = None

    result.AppendMessage(
        f"Profiling stopped.  {total} allocations recorded.  "
        f"Data written to {path}"
    )


# ---------------------------------------------------------------------------
# LLDB module entry point
# ---------------------------------------------------------------------------


def __lldb_init_module(debugger, internal_dict):  # noqa: N807
    """Called by LLDB when the script is imported via ``command script import``."""
    global _MODULE_NAME
    # LLDB registers the module using just the filename stem, so discover
    # the name it actually used.  ``internal_dict`` is the module's
    # ``__dict__``, which contains ``__name__``.
    _MODULE_NAME = internal_dict.get("__name__", __name__)

    debugger.HandleCommand(
        f'command script add -f {_MODULE_NAME}._profile_command profile'
    )
    print(
        "python_lldb_profiler loaded.  Use 'profile start [output.parquet]' "
        "to begin and 'profile stop' to finish."
    )
