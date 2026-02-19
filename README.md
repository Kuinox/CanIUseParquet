# python_lldb_profiler

A memory profiler for CPython that uses an LLDB plugin to trace allocation
functions (`_PyObject_Malloc`, `PyMem_Malloc`, etc.), capture Python stack
traces, allocation sizes, and timestamps, and write the results to an
Apache Parquet file.  A bundled static web viewer uses
[DuckDB WASM](https://duckdb.org/docs/api/wasm/overview.html) for
querying and [Three.js](https://threejs.org/) for 3-D visualisation.

## Quick start

### 1. Install dependencies

```bash
pip install pyarrow
```

### 2. Profile a Python program

Start LLDB with your Python binary and the target script:

```bash
lldb -- python my_script.py
```

Inside the LLDB session, load the profiler and start tracing:

```
(lldb) command script import profiler/lldb_profiler.py
(lldb) profile start output.parquet
(lldb) run
# … let the program execute …
(lldb) profile stop
```

The profiler sets breakpoints on CPython's internal allocation functions.
Each hit records:

| Field               | Description                                 |
|---------------------|---------------------------------------------|
| `timestamp_ns`      | Nanoseconds since profiling started         |
| `event`             | `malloc`, `realloc`, or `free`              |
| `size`              | Requested allocation size in bytes          |
| `address`           | Pointer address (for `free` events)         |
| `python_stacktrace` | Python-level call stack at time of event    |

### 3. Analyse the trace

Open `viewer/index.html` in a browser and load the `.parquet` file.
The viewer provides:

* **Summary statistics** – total events, bytes allocated, duration.
* **SQL query console** – run arbitrary DuckDB SQL against the data.
* **3-D scatter plot** – allocations plotted over time (X) vs size (Y)
  with colour coding (blue → small, red → large).

## Project structure

```
profiler/
  __init__.py              # package marker
  lldb_profiler.py         # LLDB plugin entry point & commands
  cpython_stacktrace.py    # Python stack trace extraction via LLDB
  parquet_writer.py        # buffered Parquet writer
viewer/
  index.html               # static web viewer (DuckDB WASM + Three.js)
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