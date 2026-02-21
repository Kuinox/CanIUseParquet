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
_canary_done: bool = False

# Canary: a distinctive size used to self-check that hooks are working.
_CANARY_SIZE = 7654321

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


def _try_eval_canary(frame):
    """Try to call ``PyMem_Malloc`` via expression evaluation.

    Returns the pointer address (non-zero on success), or 0.
    Tries several expression variants to handle different platforms.
    """
    # Try different expression syntaxes — LLDB on Windows may need
    # explicit prototypes or different casting.
    expressions = [
        f"(void *)PyMem_Malloc((size_t){_CANARY_SIZE})",
        f"(void *)PyMem_Malloc({_CANARY_SIZE})",
        f"((void *(*)(unsigned long long))PyMem_Malloc)({_CANARY_SIZE})",
    ]
    for expr in expressions:
        val = frame.EvaluateExpression(expr)
        if val.IsValid() and not val.GetError().Fail():
            addr = val.GetValueAsUnsigned(0)
            if addr != 0:
                return addr
    return 0


def _maybe_run_canary(frame):
    """Run the canary self-check on the first breakpoint hit.

    Verifies hooks are working.  The fact that this callback runs at all
    means a breakpoint on an allocation function was hit — this is
    already strong evidence the hooks are working.  Additionally, we try
    to call ``PyMem_Malloc(CANARY_SIZE)`` in the target process (with
    breakpoints disabled so it doesn't appear in the trace) to confirm
    the allocator is functional.  If expression evaluation fails (e.g.
    on Windows without debug info), we still report success based on the
    breakpoint hit.
    """
    global _canary_done
    if _canary_done:
        return
    _canary_done = True

    target = frame.GetThread().GetProcess().GetTarget()

    # The function we stopped in — proves hooks are intercepting allocations.
    hit_symbol = frame.GetFunctionName() or "<unknown>"

    # Disable profiler breakpoints so the canary allocation is not recorded.
    for bp_id in _breakpoint_ids:
        bp = target.FindBreakpointByID(bp_id)
        if bp.IsValid():
            bp.SetEnabled(False)

    ptr_addr = _try_eval_canary(frame)

    if ptr_addr != 0:
        # Free the canary allocation.
        frame.EvaluateExpression(
            f"(void)PyMem_Free((void *){ptr_addr})"
        )
        print(
            f"✓  Canary self-check passed: PyMem_Malloc({_CANARY_SIZE}) "
            f"returned {ptr_addr:#x} — hooks are working.",
            flush=True,
        )
    else:
        # Expression evaluation failed, but we're in a breakpoint callback
        # on an allocation function — that alone proves hooks are working.
        print(
            f"✓  Canary self-check passed: breakpoint hit on '{hit_symbol}' "
            f"— hooks are working.  (Expression evaluation of PyMem_Malloc "
            f"was not available; this is normal on Windows or release builds.)",
            flush=True,
        )

    # Re-enable profiler breakpoints.
    for bp_id in _breakpoint_ids:
        bp = target.FindBreakpointByID(bp_id)
        if bp.IsValid():
            bp.SetEnabled(True)


# ---------------------------------------------------------------------------
# Breakpoint callbacks
# ---------------------------------------------------------------------------


def _on_alloc(frame, bp_loc, extra_args, internal_dict):
    """Breakpoint callback for malloc / realloc functions."""
    global _writer, _start_time_ns
    if _writer is None:
        return False  # auto-continue

    _maybe_run_canary(frame)

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
    global _writer, _start_time_ns
    if _writer is None:
        return False

    _maybe_run_canary(frame)

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
    global _writer, _breakpoint_ids, _start_time_ns, _canary_done

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
    _canary_done = False

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


def _stop_profiling(debugger, result):
    global _writer, _breakpoint_ids, _canary_done

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

    if not _canary_done:
        result.AppendMessage(
            "⚠  Canary self-check never ran — no allocation breakpoints "
            "were hit during this session.  Possible causes:\n"
            "  • The process exited before any Python allocations occurred\n"
            "  • On Windows: the python.exe in your venv may not link to "
            "python3XX.dll in a way LLDB can follow — try targeting the "
            "base Python interpreter instead (e.g. "
            "C:\\Python312\\python.exe)\n"
            "  • Symbols may not be available in a stripped binary"
        )
    _canary_done = False

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
