"""Extract Python stack traces from a CPython process via LLDB.

This module reads the CPython interpreter's internal data structures
(PyThreadState, PyFrameObject / _PyInterpreterFrame) through LLDB's
SBValue API to reconstruct the Python-level call stack.

It supports CPython 3.9 -- 3.13+.  Starting with 3.11 the frame
representation changed from ``PyFrameObject`` to an internal
``_PyInterpreterFrame`` struct.

Performance note: struct offsets and the ``_PyRuntime`` address are
cached after the first call to avoid repeated symbol lookups.  The
stack walk uses ``process.ReadMemory()`` with cached offsets whenever
possible, falling back to ``SBValue`` tree walking only during the
initial discovery phase.
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


# ---------------------------------------------------------------------------
# Offset cache -- populated once per process, reused for every breakpoint hit
# ---------------------------------------------------------------------------

_tstate_ptr_offset: Optional[int] = None  # offset of tstate_current inside _PyRuntime
_runtime_addr: Optional[int] = None
_frame_field: Optional[str] = None  # "cframe", "current_frame", or "frame"
_cframe_offset: Optional[int] = None  # offset of cframe/current_frame in PyThreadState
_current_frame_in_cframe_offset: Optional[int] = None  # offset of current_frame in CFrame
_iframe_previous_offset: Optional[int] = None
_iframe_f_code_offset: Optional[int] = None
_code_co_filename_offset: Optional[int] = None
_code_co_name_offset: Optional[int] = None
_frame_f_lineno_offset: Optional[int] = None
_unicode_length_offset: Optional[int] = None
_unicode_header_size: Optional[int] = None
_ptr_size: int = 0
_max_frames: int = 30
_offsets_initialized: bool = False


def reset_offset_cache():
    """Reset the offset cache.  Call when the target process restarts."""
    global _offsets_initialized, _runtime_addr, _tstate_ptr_offset
    global _frame_field, _cframe_offset, _current_frame_in_cframe_offset
    global _iframe_previous_offset, _iframe_f_code_offset
    global _code_co_filename_offset, _code_co_name_offset
    global _frame_f_lineno_offset, _unicode_length_offset, _unicode_header_size
    global _ptr_size
    _offsets_initialized = False
    _runtime_addr = None
    _tstate_ptr_offset = None
    _frame_field = None
    _cframe_offset = None
    _current_frame_in_cframe_offset = None
    _iframe_previous_offset = None
    _iframe_f_code_offset = None
    _code_co_filename_offset = None
    _code_co_name_offset = None
    _frame_f_lineno_offset = None
    _unicode_length_offset = None
    _unicode_header_size = None
    _ptr_size = 0


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


def _read_pyunicode_fast(process: lldb.SBProcess, addr: int) -> str:
    """Read a ``PyUnicodeObject*`` using direct memory reads with cached offsets."""
    if addr == 0 or _unicode_length_offset is None or _unicode_header_size is None:
        return "<unknown>"

    try:
        err = lldb.SBError()

        # Read the length field.
        length_bytes = process.ReadMemory(addr + _unicode_length_offset, _ptr_size, err)
        if err.Fail() or length_bytes is None or len(length_bytes) != _ptr_size:
            return "<unreadable>"
        length = int.from_bytes(length_bytes, byteorder="little", signed=False)

        if length == 0 or length > 4096:
            return "<empty>" if length == 0 else "<too-long>"

        # Read compact-ASCII data after the header.
        data_addr = addr + _unicode_header_size
        buf = process.ReadMemory(data_addr, length, err)
        if err.Fail() or buf is None:
            return "<unreadable>"
        return buf.decode("utf-8", errors="replace")
    except Exception:
        return "<unreadable>"


def _read_ptr(process: lldb.SBProcess, addr: int) -> int:
    """Read a pointer-sized value from *addr*."""
    if addr == 0 or _ptr_size == 0:
        return 0
    err = lldb.SBError()
    try:
        buf = process.ReadMemory(addr, _ptr_size, err)
    except Exception:
        return 0
    if err.Fail() or buf is None or len(buf) != _ptr_size:
        return 0
    return int.from_bytes(buf, byteorder="little", signed=False)


def _init_offsets(frame) -> bool:
    """Discover and cache struct field offsets using SBValue (called once)."""
    global _offsets_initialized, _runtime_addr, _tstate_ptr_offset
    global _frame_field, _cframe_offset, _current_frame_in_cframe_offset
    global _iframe_previous_offset, _iframe_f_code_offset
    global _code_co_filename_offset, _code_co_name_offset
    global _frame_f_lineno_offset, _unicode_length_offset, _unicode_header_size
    global _ptr_size

    target = frame.GetThread().GetProcess().GetTarget()
    process = target.GetProcess()
    _ptr_size = target.GetAddressByteSize()

    # Find _PyRuntime address and tstate_current offset.
    runtime = target.FindFirstGlobalVariable("_PyRuntime")
    if not runtime.IsValid():
        return False

    _runtime_addr = runtime.GetLoadAddress()
    if _runtime_addr == lldb.LLDB_INVALID_ADDRESS:
        _runtime_addr = runtime.AddressOf().GetValueAsUnsigned(0)

    gilstate = runtime.GetChildMemberWithName("gilstate")
    if not gilstate.IsValid():
        return False

    tstate_current = gilstate.GetChildMemberWithName("tstate_current")
    if not tstate_current.IsValid():
        return False

    # Compute offset of tstate_current relative to _PyRuntime.
    tstate_load = tstate_current.GetLoadAddress()
    if tstate_load == lldb.LLDB_INVALID_ADDRESS:
        tstate_load = tstate_current.AddressOf().GetValueAsUnsigned(0)
    if tstate_load == 0 or _runtime_addr == 0:
        return False
    _tstate_ptr_offset = tstate_load - _runtime_addr

    # Read the actual tstate to discover its layout.
    tstate_addr = _read_ptr(process, _runtime_addr + _tstate_ptr_offset)
    if tstate_addr == 0:
        return False

    tstate_val = tstate_current.Dereference()
    if not tstate_val.IsValid():
        return False

    # Determine which field points to the current Python frame.
    cframe_val = tstate_val.GetChildMemberWithName("cframe")
    if cframe_val.IsValid() and cframe_val.GetValueAsUnsigned(0) != 0:
        _frame_field = "cframe"
        _cframe_offset = _get_member_offset(tstate_val, "cframe")
        cframe_deref = cframe_val.Dereference() if cframe_val.GetType().IsPointerType() else cframe_val
        _current_frame_in_cframe_offset = _get_member_offset(cframe_deref, "current_frame")
    else:
        current_frame_val = tstate_val.GetChildMemberWithName("current_frame")
        if current_frame_val.IsValid() and current_frame_val.GetValueAsUnsigned(0) != 0:
            _frame_field = "current_frame"
            _cframe_offset = _get_member_offset(tstate_val, "current_frame")
        else:
            frame_val = tstate_val.GetChildMemberWithName("frame")
            if frame_val.IsValid():
                _frame_field = "frame"
                _cframe_offset = _get_member_offset(tstate_val, "frame")
            else:
                return False

    # Discover _PyInterpreterFrame / PyFrameObject offsets.
    # We need to find an actual frame to inspect.
    iframe_val = None
    if _frame_field == "cframe":
        cframe_deref = cframe_val.Dereference() if cframe_val.GetType().IsPointerType() else cframe_val
        iframe_val = cframe_deref.GetChildMemberWithName("current_frame")
    elif _frame_field == "current_frame":
        iframe_val = tstate_val.GetChildMemberWithName("current_frame")
    elif _frame_field == "frame":
        iframe_val = tstate_val.GetChildMemberWithName("frame")

    if iframe_val is None or not iframe_val.IsValid() or iframe_val.GetValueAsUnsigned(0) == 0:
        return False

    if iframe_val.GetType().IsPointerType():
        iframe_deref = iframe_val.Dereference()
    else:
        iframe_deref = iframe_val

    _iframe_previous_offset = _get_member_offset(iframe_deref, "previous")
    if _iframe_previous_offset is None:
        _iframe_previous_offset = _get_member_offset(iframe_deref, "f_back")
    _iframe_f_code_offset = _get_member_offset(iframe_deref, "f_code")
    _frame_f_lineno_offset = _get_member_offset(iframe_deref, "f_lineno")

    # Code object offsets.
    code_val = iframe_deref.GetChildMemberWithName("f_code")
    if code_val.IsValid() and code_val.GetValueAsUnsigned(0) != 0:
        if code_val.GetType().IsPointerType():
            code_deref = code_val.Dereference()
        else:
            code_deref = code_val
        _code_co_filename_offset = _get_member_offset(code_deref, "co_filename")
        _code_co_name_offset = _get_member_offset(code_deref, "co_name")
        if _code_co_name_offset is None:
            _code_co_name_offset = _get_member_offset(code_deref, "co_qualname")

        # Unicode string offsets.
        co_name_val = code_deref.GetChildMemberWithName("co_name")
        if not co_name_val.IsValid():
            co_name_val = code_deref.GetChildMemberWithName("co_qualname")
        if co_name_val.IsValid() and co_name_val.GetValueAsUnsigned(0) != 0:
            if co_name_val.GetType().IsPointerType():
                uni_deref = co_name_val.Dereference()
            else:
                uni_deref = co_name_val
            _unicode_length_offset = _get_member_offset(uni_deref, "length")
            uni_type = uni_deref.GetType()
            _unicode_header_size = uni_type.GetByteSize()

    _offsets_initialized = True
    return True


def _get_member_offset(sbvalue, member_name: str) -> Optional[int]:
    """Return the byte offset of *member_name* within *sbvalue*'s struct."""
    member = sbvalue.GetChildMemberWithName(member_name)
    if not member.IsValid():
        return None

    parent_addr = sbvalue.GetLoadAddress()
    if parent_addr == lldb.LLDB_INVALID_ADDRESS:
        parent_addr = sbvalue.AddressOf().GetValueAsUnsigned(0)
    member_addr = member.GetLoadAddress()
    if member_addr == lldb.LLDB_INVALID_ADDRESS:
        member_addr = member.AddressOf().GetValueAsUnsigned(0)
    if parent_addr == 0 or member_addr == 0:
        return None
    return member_addr - parent_addr


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

    On the first call, struct offsets are discovered via SBValue (slow).
    Subsequent calls use cached offsets with direct ``ReadMemory()`` for
    much better performance.
    """
    global _offsets_initialized

    try:
        process = frame.GetThread().GetProcess()
        if not process or not process.IsValid():
            return []

        # Fast path: use cached offsets with direct memory reads.
        if _offsets_initialized and _runtime_addr is not None:
            return _get_stacktrace_fast(process)

        # First call: try to initialize offset cache.
        if _init_offsets(frame):
            return _get_stacktrace_fast(process)

        # Fallback: SBValue-based walk (slow but always works).
        return _get_stacktrace_slow(frame)
    except Exception:
        return []


def _get_stacktrace_fast(process: lldb.SBProcess) -> List[PythonFrame]:
    """Walk the Python frame chain using cached offsets and ReadMemory()."""
    if _runtime_addr is None or _tstate_ptr_offset is None:
        return []

    tstate_addr = _read_ptr(process, _runtime_addr + _tstate_ptr_offset)
    if tstate_addr == 0:
        return []

    # Get the current interpreter frame pointer.
    iframe_addr = 0
    if _frame_field == "cframe" and _cframe_offset is not None:
        cframe_ptr = _read_ptr(process, tstate_addr + _cframe_offset)
        if cframe_ptr == 0:
            return []
        if _current_frame_in_cframe_offset is not None:
            iframe_addr = _read_ptr(process, cframe_ptr + _current_frame_in_cframe_offset)
    elif _cframe_offset is not None:
        iframe_addr = _read_ptr(process, tstate_addr + _cframe_offset)

    if iframe_addr == 0:
        return []

    frames: List[PythonFrame] = []
    seen: set = set()

    while iframe_addr != 0 and len(frames) < _max_frames:
        if iframe_addr in seen:
            break
        seen.add(iframe_addr)

        filename = "<unknown>"
        function = "<unknown>"
        lineno = 0

        # Read f_code pointer.
        if _iframe_f_code_offset is not None:
            code_addr = _read_ptr(process, iframe_addr + _iframe_f_code_offset)
            if code_addr != 0:
                if _code_co_filename_offset is not None:
                    fname_ptr = _read_ptr(process, code_addr + _code_co_filename_offset)
                    if fname_ptr != 0:
                        filename = _read_pyunicode_fast(process, fname_ptr)
                if _code_co_name_offset is not None:
                    name_ptr = _read_ptr(process, code_addr + _code_co_name_offset)
                    if name_ptr != 0:
                        function = _read_pyunicode_fast(process, name_ptr)

        # Read f_lineno.
        if _frame_f_lineno_offset is not None:
            err = lldb.SBError()
            lineno_bytes = process.ReadMemory(
                iframe_addr + _frame_f_lineno_offset, 4, err
            )
            if not err.Fail() and lineno_bytes:
                lineno = int.from_bytes(lineno_bytes, byteorder="little", signed=False)

        frames.append(PythonFrame(filename=filename, function=function, lineno=lineno))

        # Move to previous frame.
        if _iframe_previous_offset is not None:
            iframe_addr = _read_ptr(process, iframe_addr + _iframe_previous_offset)
        else:
            break

    return frames


def _get_stacktrace_slow(frame) -> List[PythonFrame]:
    """Fallback: walk frames using SBValue (slow, used only if offset init fails)."""
    tstate = _get_thread_state(frame)
    if tstate is None:
        return []

    # CPython 3.11+ uses ``cframe`` -> ``current_frame``
    cframe = tstate.GetChildMemberWithName("cframe")
    if cframe.IsValid():
        current = cframe.GetChildMemberWithName("current_frame")
        if current.IsValid() and current.GetValueAsUnsigned(0) != 0:
            return _walk_interpreter_frames(current)

    # 3.13+ path: current_frame directly on tstate
    current = tstate.GetChildMemberWithName("current_frame")
    if current.IsValid() and current.GetValueAsUnsigned(0) != 0:
        return _walk_interpreter_frames(current)

    # Older CPython (<3.11)
    pyframe = tstate.GetChildMemberWithName("frame")
    if pyframe.IsValid() and pyframe.GetValueAsUnsigned(0) != 0:
        return _walk_pyframe_objects(pyframe)

    return []


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
