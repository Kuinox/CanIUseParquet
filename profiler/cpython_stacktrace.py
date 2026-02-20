"""Extract Python stack traces from a CPython process via LLDB.

This module reads the CPython interpreter's internal data structures
(PyThreadState, PyFrameObject / _PyInterpreterFrame) through LLDB's
SBValue API to reconstruct the Python-level call stack.

It supports CPython 3.9 – 3.13+.  Starting with 3.11 the frame
representation changed from ``PyFrameObject`` to an internal
``_PyInterpreterFrame`` struct.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import lldb  # type: ignore[import-not-found]


@dataclass
class PythonFrame:
    """A single frame in a Python stack trace."""

    filename: str
    function: str
    lineno: int


def _read_pyunicode(sbvalue) -> str:
    """Read a ``PyUnicodeObject*`` and return its contents as a Python str.

    Works for the *compact ASCII* representation used for most identifiers
    and short file-paths in CPython.
    """
    if not sbvalue or not sbvalue.IsValid():
        return "<unknown>"

    target = sbvalue.GetTarget()
    process = target.GetProcess()

    addr = sbvalue.GetValueAsUnsigned(0)
    if addr == 0:
        return "<null>"

    # Read the kind/state packed field to determine which storage to use.
    # PyASCIIObject.state is a bitfield at offset after ob_refcnt + ob_type +
    # length + hash.  We use EvaluateExpression as the simplest portable
    # approach.
    err = lldb.SBError()

    # Try the compact-ASCII fast path first: data lives right after the
    # PyASCIIObject struct.  We can read the ``length`` field and then
    # grab length bytes from the end of the struct.
    length_val = sbvalue.GetChildMemberWithName("length")
    if not length_val.IsValid():
        # Pre-3.12 layout: access through the struct member
        length_val = sbvalue.GetChildMemberWithName("ob_size")
    if not length_val.IsValid():
        return "<unreadable>"

    length = length_val.GetValueAsUnsigned(0)
    if length == 0 or length > 4096:
        return "<empty>" if length == 0 else "<too-long>"

    # The compact-ASCII data lives immediately after the PyASCIIObject struct.
    # ``sizeof(PyASCIIObject)`` gives us the header size.
    type_obj = sbvalue.GetType()
    if type_obj.IsPointerType():
        type_obj = type_obj.GetPointeeType()
    header_size = type_obj.GetByteSize()
    if header_size == 0:
        # Fallback: try evaluating expression for the string
        result = sbvalue.GetTarget().EvaluateExpression(
            f"(const char *)((char *){addr} + sizeof(PyASCIIObject))"
        )
        if result.IsValid():
            summary = result.GetSummary()
            if summary:
                return summary.strip('"')
        return "<unreadable>"

    data_addr = addr + header_size
    buf = process.ReadMemory(data_addr, length, err)
    if err.Fail() or buf is None:
        return "<unreadable>"
    return buf.decode("utf-8", errors="replace")


def _get_thread_state(frame):
    """Return an ``SBValue`` pointing to the current ``PyThreadState``."""
    target = frame.GetThread().GetProcess().GetTarget()

    # Try the modern path: _PyRuntime global
    runtime = target.FindFirstGlobalVariable("_PyRuntime")
    if runtime.IsValid():
        tstate = runtime.GetChildMemberWithName("gilstate").GetChildMemberWithName(
            "tstate_current"
        )
        if tstate.IsValid() and tstate.GetValueAsUnsigned(0) != 0:
            return tstate.Dereference()

    # Fallback: _PyThreadState_Current (older CPython builds)
    tstate_current = target.FindFirstGlobalVariable("_PyThreadState_Current")
    if tstate_current.IsValid():
        return tstate_current.Dereference()

    return None


def get_python_stacktrace(frame) -> List[PythonFrame]:
    """Return the current Python stack trace by walking CPython internals.

    *frame* is an ``lldb.SBFrame`` from any breakpoint callback.

    The returned list is ordered from innermost (most recent call) to
    outermost.
    """
    tstate = _get_thread_state(frame)
    if tstate is None:
        return []

    frames: List[PythonFrame] = []

    # CPython 3.11+ uses ``cframe`` -> ``current_frame`` which points to
    # ``_PyInterpreterFrame``.  Older versions use ``frame`` which points
    # to ``PyFrameObject``.
    current = None

    # Try 3.11+ path
    cframe = tstate.GetChildMemberWithName("cframe")
    if cframe.IsValid():
        current = cframe.GetChildMemberWithName("current_frame")
        if current.IsValid() and current.GetValueAsUnsigned(0) != 0:
            return _walk_interpreter_frames(current)

    # Try 3.13+ path: current_frame directly on tstate
    current = tstate.GetChildMemberWithName("current_frame")
    if current.IsValid() and current.GetValueAsUnsigned(0) != 0:
        return _walk_interpreter_frames(current)

    # Fallback: older CPython (<3.11) with PyFrameObject
    pyframe = tstate.GetChildMemberWithName("frame")
    if pyframe.IsValid() and pyframe.GetValueAsUnsigned(0) != 0:
        return _walk_pyframe_objects(pyframe)

    return frames


def _walk_interpreter_frames(iframe) -> List[PythonFrame]:
    """Walk ``_PyInterpreterFrame`` linked list (CPython 3.11+)."""
    frames: List[PythonFrame] = []
    seen: set = set()
    while iframe.IsValid() and iframe.GetValueAsUnsigned(0) != 0:
        addr = iframe.GetValueAsUnsigned(0)
        if addr in seen:
            break
        seen.add(addr)

        # Dereference if pointer
        if iframe.GetType().IsPointerType():
            iframe = iframe.Dereference()

        code = iframe.GetChildMemberWithName("f_code")
        if code.IsValid():
            if code.GetType().IsPointerType():
                code = code.Dereference()
            pf = _extract_frame_info(code, iframe)
            frames.append(pf)

        # Move to previous frame
        prev = iframe.GetChildMemberWithName("previous")
        if not prev.IsValid():
            prev = iframe.GetChildMemberWithName("f_back")
        if not prev.IsValid() or prev.GetValueAsUnsigned(0) == 0:
            break
        iframe = prev
    return frames


def _walk_pyframe_objects(pyframe) -> List[PythonFrame]:
    """Walk ``PyFrameObject`` linked list (CPython <3.11)."""
    frames: List[PythonFrame] = []
    seen: set = set()
    while pyframe.IsValid() and pyframe.GetValueAsUnsigned(0) != 0:
        addr = pyframe.GetValueAsUnsigned(0)
        if addr in seen:
            break
        seen.add(addr)

        if pyframe.GetType().IsPointerType():
            pyframe = pyframe.Dereference()

        code = pyframe.GetChildMemberWithName("f_code")
        if code.IsValid():
            if code.GetType().IsPointerType():
                code = code.Dereference()
            pf = _extract_frame_info(code, pyframe)
            frames.append(pf)

        pyframe = pyframe.GetChildMemberWithName("f_back")
    return frames


def _extract_frame_info(code, frame_obj) -> PythonFrame:
    """Extract filename, function name, and line number from a code object."""
    filename = "<unknown>"
    function = "<unknown>"
    lineno = 0

    co_filename = code.GetChildMemberWithName("co_filename")
    if co_filename.IsValid() and co_filename.GetValueAsUnsigned(0) != 0:
        filename = _read_pyunicode(co_filename)

    co_name = code.GetChildMemberWithName("co_name")
    if not co_name.IsValid() or co_name.GetValueAsUnsigned(0) == 0:
        co_name = code.GetChildMemberWithName("co_qualname")
    if co_name.IsValid() and co_name.GetValueAsUnsigned(0) != 0:
        function = _read_pyunicode(co_name)

    # Line number
    lineno_val = frame_obj.GetChildMemberWithName("f_lineno")
    if lineno_val.IsValid():
        lineno = lineno_val.GetValueAsUnsigned(0)

    return PythonFrame(filename=filename, function=function, lineno=lineno)
