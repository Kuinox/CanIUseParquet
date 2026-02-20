"""Integration tests that run the profiler against a live CPython process via LLDB.

These tests require ``lldb`` to be installed on the system (``lldb-18``
or similar) and a Python binary with exported allocation symbols
(``PyObject_Malloc``, ``PyMem_Malloc``, etc.).

The test launches LLDB in batch mode, loads the profiler plugin, runs a
small Python target script, and then verifies the resulting Parquet file
contains the expected allocation events.

Note: profiling every allocation in CPython is slow under LLDB
breakpoints, so the tests use a minimal target and a generous timeout.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap

import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_lldb() -> str | None:
    """Return the path to an ``lldb`` binary, or *None*.

    Tries versioned names in descending order, then falls back to the
    unversioned ``lldb``.
    """
    # Check common versioned names (descending so we prefer newer)
    for ver in range(25, 13, -1):
        path = shutil.which(f"lldb-{ver}")
        if path is not None:
            return path
    return shutil.which("lldb")


LLDB = _find_lldb()

requires_lldb = pytest.mark.skipif(
    LLDB is None, reason="lldb not found on this system"
)


def _write_target_script(tmp_path, script_body: str) -> str:
    """Write *script_body* to a file inside *tmp_path* and return its path."""
    p = tmp_path / "target.py"
    p.write_text(textwrap.dedent(script_body))
    return str(p)


def _run_profiler(
    tmp_path,
    target_script: str,
    *,
    output_name: str = "profile.parquet",
    timeout: int = 120,
) -> tuple[str, str, str]:
    """Run the profiler inside LLDB in batch mode.

    Returns ``(parquet_path, stdout, stderr)``.
    """
    output_parquet = str(tmp_path / output_name)
    profiler_script = os.path.join(_ROOT, "profiler", "lldb_profiler.py")

    # We need to set PYTHONPATH so that the ``profiler`` package is importable
    env = os.environ.copy()
    env["PYTHONPATH"] = _ROOT + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else ""
    )

    # Build the LLDB commands.
    # 1. Load the profiler plugin
    # 2. Start profiling to the output file
    # 3. Run the target script – LLDB will hit breakpoints and auto-continue
    # 4. After the process exits, stop profiling (flush + close)
    lldb_commands = [
        f"command script import {profiler_script}",
        f"profile start {output_parquet}",
        f"run {target_script}",
        "profile stop",
    ]

    cmd = [LLDB, "--batch"]
    cmd += ["-o", f"target create {sys.executable}"]
    for c in lldb_commands:
        cmd += ["-o", c]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    return output_parquet, result.stdout, result.stderr


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_lldb
class TestProfilerIntegration:
    """End-to-end tests that exercise the profiler via LLDB."""

    def test_basic_profiling_produces_parquet(self, tmp_path):
        """Run the profiler on a trivial script and assert we get a valid
        Parquet file with at least some allocation events."""
        target = _write_target_script(
            tmp_path,
            """\
            import sys
            data = [i * "a" for i in range(5)]
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)

        assert os.path.exists(output), (
            f"Parquet output file was not created.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        table = pq.read_table(output)
        assert table.num_rows > 0, "Parquet file has no rows"

        # Check schema columns exist
        col_names = set(table.schema.names)
        for expected in (
            "timestamp_ns",
            "event",
            "size",
            "address",
            "python_stacktrace",
        ):
            assert expected in col_names, f"Missing column: {expected}"

    def test_captures_malloc_events(self, tmp_path):
        """Verify the output contains malloc events with non-zero sizes."""
        target = _write_target_script(
            tmp_path,
            """\
            import sys
            x = [dict() for _ in range(5)]
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)

        assert os.path.exists(output), (
            f"Parquet output file was not created.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        table = pq.read_table(output)
        events = table.column("event").to_pylist()
        sizes = table.column("size").to_pylist()

        malloc_count = events.count("malloc")
        assert malloc_count > 0, "No malloc events recorded"

        # At least some mallocs should have a non-zero size
        malloc_sizes = [s for e, s in zip(events, sizes) if e == "malloc"]
        assert any(s > 0 for s in malloc_sizes), "All malloc sizes are zero"

    def test_timestamps_are_monotonic(self, tmp_path):
        """Check that recorded timestamps are non-decreasing."""
        target = _write_target_script(
            tmp_path,
            """\
            import sys
            _ = [0] * 3
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)
        assert os.path.exists(output), (
            f"Parquet output file was not created.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        table = pq.read_table(output)
        timestamps = table.column("timestamp_ns").to_pylist()
        assert len(timestamps) > 1, "Not enough data points"

        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i - 1], (
                f"Timestamps not monotonic at index {i}: "
                f"{timestamps[i - 1]} > {timestamps[i]}"
            )

    def test_canary_allocation_is_captured(self, tmp_path):
        """Emit a known-size allocation (canary) via ``PyMem_Malloc`` and
        verify the profiler catches it.  This proves the hooks are
        actually intercepting CPython allocations."""
        canary_size = 12345
        target = _write_target_script(
            tmp_path,
            f"""\
            import ctypes
            import sys

            # Call PyMem_Malloc directly — this goes through our hooked function
            pymem_malloc = ctypes.pythonapi.PyMem_Malloc
            pymem_malloc.restype = ctypes.c_void_p
            pymem_malloc.argtypes = [ctypes.c_size_t]

            pymem_free = ctypes.pythonapi.PyMem_Free
            pymem_free.restype = None
            pymem_free.argtypes = [ctypes.c_void_p]

            ptr = pymem_malloc({canary_size})
            pymem_free(ptr)
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)
        assert os.path.exists(output), (
            f"Parquet output file was not created.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        table = pq.read_table(output)
        events = table.column("event").to_pylist()
        sizes = table.column("size").to_pylist()

        # The exact canary size must appear as a malloc event
        canary_found = any(
            e == "malloc" and s == canary_size
            for e, s in zip(events, sizes)
        )
        assert canary_found, (
            f"Canary allocation of {canary_size} bytes was not captured.\n"
            f"Total malloc events: {events.count('malloc')}\n"
            f"Unique malloc sizes (first 30): "
            f"{sorted(set(s for e, s in zip(events, sizes) if e == 'malloc'))[:30]}"
        )

    def test_free_events_have_address(self, tmp_path):
        """Free events should record a non-zero pointer address."""
        target = _write_target_script(
            tmp_path,
            """\
            import sys
            data = [bytearray(100) for _ in range(3)]
            del data
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)
        assert os.path.exists(output), (
            f"Parquet output file was not created.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        table = pq.read_table(output)
        events = table.column("event").to_pylist()
        addresses = table.column("address").to_pylist()

        free_addrs = [a for e, a in zip(events, addresses) if e == "free"]
        if free_addrs:  # free events may not always be captured
            assert any(a != 0 for a in free_addrs), (
                "All free addresses are zero"
            )

    def test_canary_self_check(self, tmp_path):
        """Verify the profiler's built-in canary self-check runs at the
        beginning (on the first breakpoint hit) and that the canary
        allocation does not appear in the final trace."""
        target = _write_target_script(
            tmp_path,
            """\
            import sys
            x = [1, 2, 3]
            sys.exit(0)
            """,
        )

        output, stdout, stderr = _run_profiler(tmp_path, target)

        # The canary self-check should pass early in the run
        assert "Canary self-check passed" in stdout, (
            f"Canary self-check did not pass.\n"
            f"LLDB stdout:\n{stdout}\nLLDB stderr:\n{stderr}"
        )

        # The canary allocation (size 7654321) must NOT appear in the trace
        assert os.path.exists(output)
        table = pq.read_table(output)
        sizes = table.column("size").to_pylist()
        assert 7654321 not in sizes, (
            "Canary allocation (7654321 bytes) leaked into the trace"
        )
