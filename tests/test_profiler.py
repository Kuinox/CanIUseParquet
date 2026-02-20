"""Tests for the Parquet writer and data structures."""

import os
import tempfile

import pyarrow.parquet as pq
import pytest

from profiler.parquet_writer import AllocationRecord, ParquetWriter, SCHEMA


class TestAllocationRecord:
    def test_fields(self):
        rec = AllocationRecord(
            timestamp_ns=1000,
            event="malloc",
            size=256,
            address=0xDEAD,
            python_stacktrace="test.py:1 in main",
        )
        assert rec.timestamp_ns == 1000
        assert rec.event == "malloc"
        assert rec.size == 256
        assert rec.address == 0xDEAD
        assert rec.python_stacktrace == "test.py:1 in main"


class TestParquetWriter:
    def test_write_and_read_single_batch(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        writer = ParquetWriter(path, flush_every=100)
        for i in range(10):
            writer.add(
                AllocationRecord(
                    timestamp_ns=i * 100,
                    event="malloc",
                    size=64 * (i + 1),
                    address=0x1000 + i,
                    python_stacktrace=f"file.py:{i} in func_{i}",
                )
            )
        writer.close()

        table = pq.read_table(path)
        assert table.num_rows == 10
        assert table.schema.equals(SCHEMA)
        assert table.column("event").to_pylist() == ["malloc"] * 10
        assert table.column("size").to_pylist() == [64 * (i + 1) for i in range(10)]

    def test_auto_flush(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        writer = ParquetWriter(path, flush_every=5)
        for i in range(12):
            writer.add(
                AllocationRecord(
                    timestamp_ns=i,
                    event="free" if i % 3 == 0 else "malloc",
                    size=i * 10,
                    address=i,
                    python_stacktrace="",
                )
            )
        writer.close()

        table = pq.read_table(path)
        assert table.num_rows == 12

    def test_empty_close(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        writer = ParquetWriter(path)
        writer.close()
        assert not os.path.exists(path)

    def test_multiple_event_types(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        writer = ParquetWriter(path)
        writer.add(AllocationRecord(0, "malloc", 100, 0x1, "a.py:1 in f"))
        writer.add(AllocationRecord(1, "realloc", 200, 0x1, "a.py:2 in g"))
        writer.add(AllocationRecord(2, "free", 0, 0x1, "a.py:3 in h"))
        writer.close()

        table = pq.read_table(path)
        assert table.num_rows == 3
        assert table.column("event").to_pylist() == ["malloc", "realloc", "free"]
