"""Write profiling records to an Apache Parquet file.

Records are buffered in memory and flushed to disk in row-group sized
batches so that memory consumption stays bounded even for long profiling
sessions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class AllocationRecord:
    """A single allocation / deallocation event."""

    timestamp_ns: int
    event: str  # "malloc", "realloc", "free"
    size: int
    address: int
    python_stacktrace: str  # newline-separated frame strings


# Arrow schema shared by all output files.
SCHEMA = pa.schema(
    [
        pa.field("timestamp_ns", pa.int64()),
        pa.field("event", pa.string()),
        pa.field("size", pa.int64()),
        pa.field("address", pa.uint64()),
        pa.field("python_stacktrace", pa.string()),
    ]
)


class ParquetWriter:
    """Accumulates ``AllocationRecord`` objects and writes them to Parquet."""

    def __init__(
        self,
        path: str,
        flush_every: int = 10_000,
    ) -> None:
        self._path = path
        self._flush_every = flush_every
        self._records: List[AllocationRecord] = []
        self._writer: Optional[pq.ParquetWriter] = None

    def add(self, record: AllocationRecord) -> None:
        self._records.append(record)
        if len(self._records) >= self._flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._records:
            return

        arrays = [
            pa.array([r.timestamp_ns for r in self._records], type=pa.int64()),
            pa.array([r.event for r in self._records], type=pa.string()),
            pa.array([r.size for r in self._records], type=pa.int64()),
            pa.array([r.address for r in self._records], type=pa.uint64()),
            pa.array(
                [r.python_stacktrace for r in self._records], type=pa.string()
            ),
        ]
        table = pa.table(
            {name: arr for name, arr in zip(SCHEMA.names, arrays)},
            schema=SCHEMA,
        )
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._path, SCHEMA)
        self._writer.write_table(table)
        self._records.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
