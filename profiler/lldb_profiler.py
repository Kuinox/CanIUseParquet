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

from profiler.cpython_stacktrace import (
    PythonFrame,
    get_python_stacktrace,
    reset_offset_cache,
)
from profiler.parquet_writer import AllocationRecord, ParquetWriter

# ---------------------------------------------------------------------------
# Global profiler state
# ---------------------------------------------------------------------------

_writer: Optional[ParquetWriter] = None
_breakpoint_ids: list[int] = []
_start_time_ns: int = 0
_canary_in_progress: bool = False
_canary_hit_count: int = 0
_capture_stacktrace: bool = True
_sample_rate: int = 1  # capture every Nth event (1 = all)
_hit_counter: int = 0  # total breakpoint hits (for sampling)

# The module name as seen by LLDB after ``command script import``.
# LLDB registers the script using just the filename stem (e.g.
# ``lldb_profiler``), so we detect it at import time.
_MODULE_NAME: str = __name__

# CPython allocation entry points we instrument.
# Public API functions — the common entry points used by Python code.
# These are sufficient for most profiling since internal functions are
# called *by* the public API, so instrumenting both would double-count.
_ALLOC_FUNCTIONS_PUBLIC = [
    ("PyObject_Malloc", "malloc"),
    ("PyObject_Realloc", "realloc"),
    ("PyObject_Free", "free"),
    ("PyMem_Malloc", "malloc"),
    ("PyMem_Realloc", "realloc"),
    ("PyMem_Free", "free"),
]

# Internal CPython allocators — only used with --include-internal.
_ALLOC_FUNCTIONS_INTERNAL = [
    ("_PyObject_Malloc", "malloc"),
    ("_PyObject_Realloc", "realloc"),
    ("_PyObject_Free", "free"),
    ("_PyMem_Malloc", "malloc"),
    ("_PyMem_Realloc", "realloc"),
    ("_PyMem_Free", "free"),
]


def _resolve_real_python(exe_path: str) -> Optional[str]:
    """If *exe_path* is inside a Python venv, return the base interpreter.

    Every venv has a ``pyvenv.cfg`` file in its root directory with a
    ``home`` key pointing to the directory containing the real Python
    binary.  On Windows the venv ``python.exe`` is a thin launcher that
    delegates to ``python3XX.dll`` in the base installation — LLDB may
    not be able to resolve CPython symbols through it.  This function
    detects that situation and returns the path to the base interpreter.

    Returns *None* if this is not a venv or the base interpreter cannot
    be found.
    """
    exe_path = os.path.abspath(exe_path)
    exe_dir = os.path.dirname(exe_path)
    exe_name = os.path.basename(exe_path)

    # pyvenv.cfg lives in the venv root.
    # Windows: venv/Scripts/python.exe → cfg at venv/pyvenv.cfg
    # Unix:    venv/bin/python         → cfg at venv/pyvenv.cfg
    for cfg_dir in [exe_dir, os.path.dirname(exe_dir)]:
        cfg_path = os.path.join(cfg_dir, "pyvenv.cfg")
        if not os.path.isfile(cfg_path):
            continue

        home = None
        with open(cfg_path, encoding="utf-8") as f:
            for line in f:
                key_val = line.split("=", 1)
                if len(key_val) == 2 and key_val[0].strip().lower() == "home":
                    home = key_val[1].strip()
                    break

        if not home or not os.path.isdir(home):
            return None

        # Try the same filename first, then common interpreter names.
        candidates = [exe_name]
        if platform.system() == "Windows":
            candidates += ["python.exe", "python3.exe"]
        else:
            candidates += ["python3", "python"]

        for name in candidates:
            real = os.path.join(home, name)
            if os.path.isfile(real):
                return os.path.abspath(real)

        return None

    return None


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
            f"WARNING: Canary: could not launch process -- {error.GetCString()}"
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

    # Reset the stack trace offset cache -- the canary process is gone
    # and the next process will have different addresses.
    reset_offset_cache()

    # Restore auto-continue for the real profiling run.
    for bp_id in _breakpoint_ids:
        bp = target.FindBreakpointByID(bp_id)
        if bp.IsValid():
            bp.SetAutoContinue(True)

    debugger.SetAsync(was_async)

    if _canary_hit_count > 0:
        result.AppendMessage(
            f"OK: Canary: {_canary_hit_count} allocation breakpoints hit "
            f"-- hooks are working."
        )
    else:
        result.AppendMessage(
            "WARNING: Canary: no allocation breakpoints were hit -- hooks may "
            "not be working.\n"
            "  Possible causes:\n"
            "  * On Windows: try targeting the base Python interpreter "
            "instead of a venv python.exe\n"
            "  * Symbols may not be available in a stripped binary"
        )

    # Reset the start time so user timestamps begin after the canary.
    _start_time_ns = time.time_ns()


# ---------------------------------------------------------------------------
# Breakpoint callbacks
# ---------------------------------------------------------------------------


def _on_alloc(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for malloc / realloc functions."""
    global _writer, _start_time_ns, _canary_hit_count, _hit_counter
    if _canary_in_progress:
        _canary_hit_count += 1
        return True  # stop during canary so we can limit hits
    if _writer is None:
        return False  # auto-continue

    # Sampling: skip events that don't match the sample rate.
    _hit_counter += 1
    if _sample_rate > 1 and (_hit_counter % _sample_rate) != 0:
        return False

    event = "malloc"
    symbol = frame.GetFunctionName() or ""
    all_funcs = _ALLOC_FUNCTIONS_PUBLIC + _ALLOC_FUNCTIONS_INTERNAL
    for sym, ev in all_funcs:
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

    stacktrace_str = ""
    if _capture_stacktrace:
        stacktrace = get_python_stacktrace(frame)
        stacktrace_str = _format_stacktrace(stacktrace)
    ts = time.time_ns() - _start_time_ns

    _writer.add(
        AllocationRecord(
            timestamp_ns=ts,
            event=event,
            size=size,
            address=address,
            python_stacktrace=stacktrace_str,
        )
    )
    return False  # auto-continue


def _on_free(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for free functions."""
    global _writer, _start_time_ns, _canary_hit_count, _hit_counter
    if _canary_in_progress:
        _canary_hit_count += 1
        return True  # stop during canary so we can limit hits
    if _writer is None:
        return False

    # Sampling: skip events that don't match the sample rate.
    _hit_counter += 1
    if _sample_rate > 1 and (_hit_counter % _sample_rate) != 0:
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

    stacktrace_str = ""
    if _capture_stacktrace:
        stacktrace = get_python_stacktrace(frame)
        stacktrace_str = _format_stacktrace(stacktrace)
    ts = time.time_ns() - _start_time_ns

    _writer.add(
        AllocationRecord(
            timestamp_ns=ts,
            event="free",
            size=0,
            address=address,
            python_stacktrace=stacktrace_str,
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
        result.AppendMessage(
            "Usage: profile start [output.parquet] [--no-stacktrace] "
            "[--sample-rate N] [--include-internal] | profile stop"
        )
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
    global _writer, _breakpoint_ids, _start_time_ns, _capture_stacktrace
    global _sample_rate, _hit_counter

    if _writer is not None:
        result.AppendMessage("Profiling is already active.  Use 'profile stop' first.")
        return

    # Parse flags.
    positional = []
    _capture_stacktrace = True
    _sample_rate = 1
    _hit_counter = 0
    include_internal = False
    i = 0
    while i < len(args):
        if args[i] == "--no-stacktrace":
            _capture_stacktrace = False
        elif args[i] == "--include-internal":
            include_internal = True
        elif args[i] == "--sample-rate" and i + 1 < len(args):
            i += 1
            try:
                _sample_rate = max(1, int(args[i]))
            except ValueError:
                result.AppendMessage(
                    f"Invalid --sample-rate value: {args[i]}.  Using 1."
                )
                _sample_rate = 1
        else:
            positional.append(args[i])
        i += 1

    output_path = (
        positional[0]
        if positional
        else os.environ.get("PROFILER_OUTPUT", "profile_output.parquet")
    )

    target = debugger.GetSelectedTarget()
    if not target.IsValid():
        result.AppendMessage("No valid target.  Load a Python binary first.")
        return

    # Detect venv and resolve to base interpreter for reliable symbol access.
    exe = target.GetExecutable()
    if exe.IsValid():
        exe_path = exe.fullpath or os.path.join(
            exe.GetDirectory() or "", exe.GetFilename() or ""
        )
        real_python = _resolve_real_python(exe_path)
        if real_python is not None:
            result.AppendMessage(
                f"Detected venv Python: {exe_path}\n"
                f"  Resolving to base interpreter: {real_python}"
            )
            error = lldb.SBError()
            new_target = debugger.CreateTarget(
                real_python, None, None, True, error
            )
            if new_target.IsValid():
                debugger.SetSelectedTarget(new_target)
                target = new_target
            else:
                result.AppendMessage(
                    f"  WARNING: Could not create target for base interpreter: "
                    f"{error.GetCString()}\n"
                    f"  Continuing with venv Python."
                )

    _writer = ParquetWriter(output_path)
    _start_time_ns = time.time_ns()
    _breakpoint_ids.clear()

    # Select which functions to instrument.
    # Default: public API only (6 functions).  This avoids double-counting
    # since PyMem_Malloc calls _PyMem_Malloc internally.
    alloc_functions = list(_ALLOC_FUNCTIONS_PUBLIC)
    if include_internal:
        alloc_functions += _ALLOC_FUNCTIONS_INTERNAL

    resolved_count = 0
    pending_count = 0
    failed_names = []

    for func_name, event_kind in alloc_functions:
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
        f"Profiling started - {len(_breakpoint_ids)} breakpoints set "
        f"({resolved_count} resolved, {pending_count} pending).  "
        f"Output: {output_path}"
    )
    if _sample_rate > 1:
        result.AppendMessage(
            f"  Sampling: capturing 1 out of every "
            f"{_sample_rate} events to reduce overhead."
        )
    if not _capture_stacktrace:
        result.AppendMessage(
            "  Stack trace capture disabled (--no-stacktrace).  "
            "This significantly reduces overhead."
        )
    if pending_count > 0:
        result.AppendMessage(
            f"  {pending_count} breakpoints are pending -- they will resolve "
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
            "WARNING: No breakpoints could be set.  Make sure the target is a "
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

    msg = f"Profiling stopped.  {total} allocations recorded."
    if _sample_rate > 1:
        msg += (
            f"  ({_hit_counter} total breakpoint hits, "
            f"sampled at 1/{_sample_rate}.)"
        )
    msg += f"  Data written to {path}"
    result.AppendMessage(msg)


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
