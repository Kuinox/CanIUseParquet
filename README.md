# python_lldb_profiler

A memory profiler for CPython that uses an LLDB plugin to trace allocation
functions (`_PyObject_Malloc`, `PyMem_Malloc`, etc.), capture Python stack
traces, allocation sizes, and timestamps, and write the results to an
Apache Parquet file.  A bundled static web viewer uses
[DuckDB WASM](https://duckdb.org/docs/api/wasm/overview.html) for
querying and [Three.js](https://threejs.org/) for 3-D visualization.

## Prerequisites

- **LLDB** (version 14 or later) — install via your system package manager:
  ```bash
  # Ubuntu / Debian
  sudo apt install lldb

  # macOS (ships with Xcode command-line tools)
  xcode-select --install

  # Windows — install via the LLVM installer or winget
  winget install LLVM.LLVM
  ```

  On Windows, make sure the LLVM `bin` directory (e.g.
  `C:\Program Files\LLVM\bin`) is on your `PATH`.

- **Python 3.9+** — a standard CPython build with exported allocation symbols.

- **PyArrow** — required by the profiler to write Parquet files:
  ```bash
  pip install pyarrow
  ```

## Usage

### Interactive mode

1. Start LLDB with your Python binary:
   ```bash
   lldb python
   ```

2. Inside the LLDB session, load the profiler plugin and start profiling:
   ```
   (lldb) command script import profiler/lldb_profiler.py
   (lldb) profile start output.parquet
   (lldb) run my_script.py
   ```

3. Once the program finishes (or you interrupt it), stop profiling:
   ```
   (lldb) profile stop
   ```

   The trace is written to `output.parquet`.

### Batch mode (non-interactive)

You can run everything in a single command without entering the LLDB
shell:

```bash
# Linux / macOS
lldb --batch \
  -o "target create python" \
  -o "command script import profiler/lldb_profiler.py" \
  -o "profile start output.parquet" \
  -o "run my_script.py" \
  -o "profile stop"
```

```powershell
# Windows (PowerShell)
lldb --batch `
  -o "target create python" `
  -o "command script import profiler/lldb_profiler.py" `
  -o "profile start output.parquet" `
  -o "run my_script.py" `
  -o "profile stop"
```

```cmd
rem Windows (Command Prompt)
lldb --batch -o "target create python" -o "command script import profiler/lldb_profiler.py" -o "profile start output.parquet" -o "run my_script.py" -o "profile stop"
```

### Environment variable

Instead of passing the output path to `profile start`, you can set
`PROFILER_OUTPUT`:

```bash
# Linux / macOS
export PROFILER_OUTPUT=output.parquet
```

```powershell
# Windows (PowerShell)
$env:PROFILER_OUTPUT = "output.parquet"
```

```cmd
rem Windows (Command Prompt)
set PROFILER_OUTPUT=output.parquet
```

Then just run `profile start` (without arguments) inside LLDB.

## Output schema

Each allocation event is recorded as a row in the Parquet file:

| Field               | Type     | Description                              |
|---------------------|----------|------------------------------------------|
| `timestamp_ns`      | `int64`  | Nanoseconds since profiling started      |
| `event`             | `string` | `malloc`, `realloc`, or `free`           |
| `size`              | `int64`  | Requested allocation size in bytes       |
| `address`           | `uint64` | Pointer address (for `free` events)      |
| `python_stacktrace` | `string` | Python-level call stack at time of event |

## Analysing the trace

Open `viewer/index.html` in a browser and load the `.parquet` file.
The viewer provides:

* **Summary statistics** — total events, bytes allocated, duration.
* **SQL query console** — run arbitrary DuckDB SQL against the data.
* **3-D scatter plot** — allocations plotted over time (X) vs size (Y)
  with colour coding (blue → small, red → large).

You can also query the Parquet file directly with any tool that reads
Parquet (pandas, DuckDB CLI, etc.):

```python
import pyarrow.parquet as pq

table = pq.read_table("output.parquet")
print(table.to_pandas().head())
```

## Running the tests

Install dev dependencies and run with pytest:

```bash
pip install pyarrow pytest
python -m pytest tests/ -v
```

- **Unit tests** (`tests/test_profiler.py`) — test ParquetWriter and data
  structures. No LLDB required.
- **Integration tests** (`tests/test_integration.py`) — run the full
  profiler end-to-end via `lldb --batch` against a live CPython process.
  These require LLDB to be installed and are skipped automatically if it
  is not available.

## Project structure

```
profiler/
  __init__.py              # package marker
  lldb_profiler.py         # LLDB plugin entry point & commands
  cpython_stacktrace.py    # Python stack trace extraction via LLDB
  parquet_writer.py        # buffered Parquet writer
viewer/
  index.html               # static web viewer (DuckDB WASM + Three.js)
tests/
  test_profiler.py         # unit tests (ParquetWriter, AllocationRecord)
  test_integration.py      # integration tests (real LLDB + CPython)
pyproject.toml             # packaging metadata
```

## How it works

1. **LLDB plugin** (`profiler/lldb_profiler.py`) registers a `profile`
   command.  `profile start` creates breakpoints on key CPython
   allocation functions and attaches Python callbacks.
2. **Stack trace extraction** (`profiler/cpython_stacktrace.py`) walks
   the CPython interpreter's internal frame structures
   (`_PyInterpreterFrame` on 3.11+, `PyFrameObject` on older versions)
   to reconstruct the Python-level call stack.
3. **Parquet output** (`profiler/parquet_writer.py`) buffers records in
   memory and flushes them to Parquet row groups via PyArrow to keep
   memory usage bounded.
4. **Web viewer** (`viewer/index.html`) loads the Parquet file entirely
   client-side using DuckDB WASM for SQL and Three.js for rendering.

## License

MIT