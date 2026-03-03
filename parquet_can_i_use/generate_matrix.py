#!/usr/bin/env python3
"""
Master script to run all Parquet feature test CLIs and generate the compatibility matrix.

Usage:
    python generate_matrix.py [--skip-build] [--only TOOL...]

Each CLI outputs a JSON report. This script collects all reports and generates
parquet_can_i_use.md with the results.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CLI_DIR = SCRIPT_DIR / "cli"
RESULTS_DIR = SCRIPT_DIR / "results"
OUTPUT_FILE = SCRIPT_DIR.parent / "parquet_can_i_use.md"
VERSION_HISTORY_FILE = SCRIPT_DIR / "version_history.json"

# All tools and their build/run commands
TOOLS = {
    "pyarrow": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_pyarrow.py")],
        "deps": "pip install pyarrow",
    },
    "fastparquet": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_fastparquet.py")],
        "deps": "pip install fastparquet pandas",
    },
    "polars": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_polars.py")],
        "deps": "pip install polars",
    },
    "duckdb": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_duckdb.py")],
        "deps": "pip install duckdb",
    },
    "parquet-rs": {
        "build": ["cargo", "build", "--release"],
        "build_cwd": str(CLI_DIR / "rust" / "test_parquet_rs"),
        "run": [str(CLI_DIR / "rust" / "test_parquet_rs" / "target" / "release" / "test_parquet_rs")],
        "deps": "cargo (Rust toolchain)",
    },
    "parquet-go": {
        "build": ["go", "build", "-o", "test_parquet_go"],
        "build_cwd": str(CLI_DIR / "go" / "test_parquet_go"),
        "run": [str(CLI_DIR / "go" / "test_parquet_go" / "test_parquet_go")],
        "deps": "go (Go toolchain)",
    },
    "parquet-java": {
        "build": ["mvn", "-q", "package", "-DskipTests"],
        "build_cwd": str(CLI_DIR / "java" / "test_parquet_java"),
        "run": ["java", "-jar", str(CLI_DIR / "java" / "test_parquet_java" / "target" / "test-parquet-java-1.0-SNAPSHOT.jar")],
        "deps": "maven, JDK 17+",
    },
    "parquet-dotnet": {
        "build": ["dotnet", "build", "-c", "Release", "-v", "q"],
        "build_cwd": str(CLI_DIR / "dotnet" / "test_parquet_dotnet"),
        "run": ["dotnet", "run", "--project", str(CLI_DIR / "dotnet" / "test_parquet_dotnet"), "-c", "Release", "--no-build"],
        "deps": ".NET 8.0 SDK",
    },
}

# Ordered categories and features for the matrix
COMPRESSION_CODECS = ["NONE", "SNAPPY", "GZIP", "BROTLI", "LZO", "LZ4", "LZ4_RAW", "ZSTD"]
ENCODINGS = ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
             "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY", "BYTE_STREAM_SPLIT"]
ENCODING_TYPES = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "STRING", "BINARY"]
LOGICAL_TYPES = ["STRING", "DATE", "TIME_MILLIS", "TIME_MICROS", "TIME_NANOS",
                 "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS", "TIMESTAMP_NANOS", "INT96",
                 "DECIMAL", "UUID", "JSON", "FLOAT16", "ENUM", "BSON", "INTERVAL"]
NESTED_TYPES = ["LIST", "MAP", "STRUCT", "NESTED_LIST", "NESTED_MAP", "DEEP_NESTING"]
ADVANCED_FEATURES = ["STATISTICS", "PAGE_INDEX", "BLOOM_FILTER", "DATA_PAGE_V2",
                     "COLUMN_ENCRYPTION", "PREDICATE_PUSHDOWN", "PROJECTION_PUSHDOWN", "SCHEMA_EVOLUTION"]

# Display names for the tools
TOOL_DISPLAY_NAMES = {
    "pyarrow": "PyArrow",
    "fastparquet": "fastparquet",
    "polars": "Polars",
    "duckdb": "DuckDB",
    "parquet-rs": "parquet-rs",
    "parquet-go": "parquet-go",
    "parquet-java": "parquet-java",
    "parquet-dotnet": "parquet-dotnet",
}


def run_tool(name, tool_config, skip_build=False):
    """Build and run a tool, return its JSON result."""
    print(f"  Testing {name}...", end=" ", flush=True)

    # Build step
    if not skip_build and tool_config.get("build"):
        try:
            subprocess.run(
                tool_config["build"],
                cwd=tool_config.get("build_cwd"),
                capture_output=True,
                text=True,
                check=True,
                timeout=300,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"BUILD FAILED: {e}")
            return None

    # Run step
    try:
        result = subprocess.run(
            tool_config["run"],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )
        data = json.loads(result.stdout)
        print(f"OK (v{data.get('version', '?')})")
        return data
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired) as e:
        print(f"FAILED: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            print(f"    stderr: {e.stderr[:200]}")
        return None


def load_version_history():
    """Load version history from JSON file."""
    if VERSION_HISTORY_FILE.exists():
        with open(VERSION_HISTORY_FILE) as f:
            return json.load(f)
    return {}


def symbol(val, version_str=None):
    """Convert a boolean/None to a matrix symbol, optionally with version info."""
    if val is True:
        if version_str:
            return f"✅ {version_str}+"
        return "✅"
    elif val is False:
        return "❌"
    else:
        return "➖"


def generate_table(headers, rows, feature_key, results, version_history):
    """Generate a markdown table for a category."""
    tool_names = [TOOL_DISPLAY_NAMES.get(t, t) for t in results.keys()]
    lines = []

    # Header
    header = f"| {headers[0]} | " + " | ".join(tool_names) + " |"
    sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
    lines.append(header)
    lines.append(sep)

    # Rows
    for feature in rows:
        display = feature.replace("_", " ") if feature.startswith("TIMESTAMP") or feature.startswith("TIME") else feature
        cells = []
        for tool_id, data in results.items():
            if data is None:
                cells.append("➖")
            else:
                category = data.get(feature_key, {})
                val = category.get(feature)
                # Get version info
                vh = version_history.get(tool_id, {}).get(feature_key, {}).get(feature)
                cells.append(symbol(val, vh))
        line = f"| {display} | " + " | ".join(cells) + " |"
        lines.append(line)

    return "\n".join(lines)


def generate_encoding_type_table(results, version_history):
    """Generate the encoding × type cross-matrix table."""
    tool_ids = list(results.keys())
    lines = []

    for enc_name in ENCODINGS:
        lines.append(f"### {enc_name}")
        lines.append("")

        tool_names = [TOOL_DISPLAY_NAMES.get(t, t) for t in tool_ids]
        header = f"| Type | " + " | ".join(tool_names) + " |"
        sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
        lines.append(header)
        lines.append(sep)

        for ptype in ENCODING_TYPES:
            cells = []
            for tool_id in tool_ids:
                data = results.get(tool_id)
                if data is None:
                    cells.append("➖")
                else:
                    enc_data = data.get("encoding", {}).get(enc_name, {})
                    if isinstance(enc_data, dict):
                        val = enc_data.get(ptype)
                        cells.append(symbol(val))
                    elif isinstance(enc_data, bool):
                        # Legacy format (single bool for the whole encoding)
                        cells.append(symbol(enc_data))
                    else:
                        cells.append("➖")
            line = f"| {ptype} | " + " | ".join(cells) + " |"
            lines.append(line)

        lines.append("")

    return "\n".join(lines)


def generate_markdown(results, version_history):
    """Generate the full markdown document from test results."""
    lines = []

    lines.append("# Can I Use: Parquet Format Support Matrix")
    lines.append("")
    lines.append("A comprehensive compatibility reference for Apache Parquet features across libraries and query engines.")
    lines.append("**This matrix is auto-generated by running actual tests against each library.**")
    lines.append("")
    lines.append("> **Legend:** ✅ = Supported (with version introduced when known) | ❌ = Not supported | ➖ = Not tested / unavailable")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Tools & Versions
    lines.append("## Tools & Versions Tested")
    lines.append("")
    lines.append("| Tool | Version | Language |")
    lines.append("|---|---|---|")
    lang_map = {
        "pyarrow": "Python",
        "fastparquet": "Python",
        "polars": "Rust / Python",
        "duckdb": "C++",
        "parquet-rs": "Rust",
        "parquet-go": "Go",
        "parquet-java": "Java",
        "parquet-dotnet": "C# / .NET",
    }
    for tool_id, data in results.items():
        display = TOOL_DISPLAY_NAMES.get(tool_id, tool_id)
        version = data.get("version", "N/A") if data else "N/A"
        lang = lang_map.get(tool_id, "?")
        lines.append(f"| {display} | {version} | {lang} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Compression
    lines.append("## Compression Codecs")
    lines.append("")
    lines.append(generate_table(["Codec"], COMPRESSION_CODECS, "compression", results, version_history))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Encoding × Type
    lines.append("## Encoding Types × Data Types")
    lines.append("")
    lines.append("Each encoding is tested with each physical data type to show which combinations are supported.")
    lines.append("")
    lines.append(generate_encoding_type_table(results, version_history))
    lines.append("---")
    lines.append("")

    # Logical Types
    lines.append("## Logical Types")
    lines.append("")
    lines.append(generate_table(["Logical Type"], LOGICAL_TYPES, "logical_types", results, version_history))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Nested Types
    lines.append("## Nested & Complex Types")
    lines.append("")
    lines.append(generate_table(["Type"], NESTED_TYPES, "nested_types", results, version_history))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Advanced Features
    lines.append("## Advanced Features")
    lines.append("")
    lines.append(generate_table(["Feature"], ADVANCED_FEATURES, "advanced_features", results, version_history))
    lines.append("")
    lines.append("---")
    lines.append("")

    # How to reproduce
    lines.append("## How to Reproduce")
    lines.append("")
    lines.append("Each result is generated by running a small CLI program that tests actual Parquet read/write operations.")
    lines.append("")
    lines.append("```bash")
    lines.append("# Install Python dependencies")
    lines.append("pip install pyarrow fastparquet polars duckdb pandas")
    lines.append("")
    lines.append("# Run the matrix generator")
    lines.append("cd parquet_can_i_use")
    lines.append("python generate_matrix.py")
    lines.append("```")
    lines.append("")
    lines.append("Individual CLIs can be run separately:")
    lines.append("")
    lines.append("```bash")
    lines.append("# Python libraries")
    lines.append("python cli/python/test_pyarrow.py")
    lines.append("python cli/python/test_fastparquet.py")
    lines.append("python cli/python/test_polars.py")
    lines.append("python cli/python/test_duckdb.py")
    lines.append("")
    lines.append("# Rust")
    lines.append("cd cli/rust/test_parquet_rs && cargo run --release")
    lines.append("")
    lines.append("# Go")
    lines.append("cd cli/go/test_parquet_go && go run .")
    lines.append("")
    lines.append("# Java")
    lines.append("cd cli/java/test_parquet_java && mvn package -q && java -jar target/test-parquet-java-1.0-SNAPSHOT.jar")
    lines.append("")
    lines.append("# .NET")
    lines.append("cd cli/dotnet/test_parquet_dotnet && dotnet run")
    lines.append("```")
    lines.append("")
    lines.append("Each CLI outputs a JSON report that is saved in `parquet_can_i_use/results/`.")
    lines.append("")

    # Sources
    lines.append("## Sources")
    lines.append("")
    lines.append("- [Apache Parquet Format Specification](https://github.com/apache/parquet-format)")
    lines.append("- [Apache Parquet Implementation Status](https://parquet.apache.org/docs/file-format/implementationstatus/)")
    lines.append("- [PyArrow Parquet Documentation](https://arrow.apache.org/docs/python/parquet.html)")
    lines.append("- [DuckDB Parquet Documentation](https://duckdb.org/docs/data/parquet/overview)")
    lines.append("- [Polars Parquet Documentation](https://docs.pola.rs/user-guide/io/parquet/)")
    lines.append("- [parquet-rs (arrow-rs) Documentation](https://docs.rs/parquet/latest/parquet/)")
    lines.append("- [parquet-dotnet Documentation](https://github.com/aloneguid/parquet-dotnet)")
    lines.append("- [fastparquet Documentation](https://fastparquet.readthedocs.io/)")
    lines.append("- [parquet-go Documentation](https://github.com/parquet-go/parquet-go)")
    lines.append("")
    lines.append("*Auto-generated by `generate_matrix.py`. Re-run to update with latest library versions.*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet compatibility matrix")
    parser.add_argument("--skip-build", action="store_true", help="Skip build step for compiled languages")
    parser.add_argument("--only", nargs="*", help="Only run specific tools")
    parser.add_argument("--load-results", action="store_true", help="Load results from files instead of running tests")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)

    # Load version history
    version_history = load_version_history()

    if args.load_results:
        # Load from saved results
        results = {}
        for tool_id in TOOLS:
            result_file = RESULTS_DIR / f"{tool_id}.json"
            if result_file.exists():
                with open(result_file) as f:
                    results[tool_id] = json.load(f)
            else:
                results[tool_id] = None
    else:
        # Run tests
        tools_to_run = args.only if args.only else list(TOOLS.keys())
        results = {}

        print("Running Parquet feature tests...")
        print()

        for tool_id in TOOLS:
            if tool_id in tools_to_run:
                data = run_tool(tool_id, TOOLS[tool_id], skip_build=args.skip_build)
                results[tool_id] = data
                # Save result
                if data:
                    with open(RESULTS_DIR / f"{tool_id}.json", "w") as f:
                        json.dump(data, f, indent=2)
            else:
                # Try to load from saved results
                result_file = RESULTS_DIR / f"{tool_id}.json"
                if result_file.exists():
                    with open(result_file) as f:
                        results[tool_id] = json.load(f)
                else:
                    results[tool_id] = None

    # Generate markdown
    print()
    print("Generating matrix...")
    markdown = generate_markdown(results, version_history)

    with open(OUTPUT_FILE, "w") as f:
        f.write(markdown)

    print(f"Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
