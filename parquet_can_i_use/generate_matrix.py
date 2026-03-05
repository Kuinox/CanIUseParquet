#!/usr/bin/env python3
"""
Master script to generate the Parquet compatibility matrix.

Reads multi-version test results from results/ and generates:
1. A combined JSON data file for the Next.js site (site/public/data/matrix.json)
2. A markdown file (parquet_can_i_use.md) for reference

Usage:
    python generate_matrix.py [--skip-build] [--only TOOL...] [--load-results]
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
OUTPUT_MD = SCRIPT_DIR.parent / "parquet_can_i_use.md"
OUTPUT_JSON = SCRIPT_DIR / "site" / "public" / "data" / "matrix.json"
VERSIONS_FILE = SCRIPT_DIR / "versions.json"

# Ordered categories and features
COMPRESSION_CODECS = ["NONE", "SNAPPY", "GZIP", "BROTLI", "LZO", "LZ4", "LZ4_RAW", "ZSTD"]
ENCODINGS = ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
             "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
             "BYTE_STREAM_SPLIT", "BYTE_STREAM_SPLIT_EXTENDED"]
ENCODING_TYPES = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"]
LOGICAL_TYPES = ["STRING", "DATE", "TIME_MILLIS", "TIME_MICROS", "TIME_NANOS",
                 "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS", "TIMESTAMP_NANOS", "INT96",
                 "DECIMAL", "UUID", "JSON", "FLOAT16", "ENUM", "BSON", "INTERVAL",
                 "UNKNOWN", "VARIANT", "GEOMETRY", "GEOGRAPHY"]
NESTED_TYPES = ["LIST", "MAP", "STRUCT", "NESTED_LIST", "NESTED_MAP", "DEEP_NESTING"]
ADVANCED_FEATURES = ["STATISTICS", "PAGE_INDEX", "BLOOM_FILTER", "DATA_PAGE_V2",
                     "COLUMN_ENCRYPTION", "SIZE_STATISTICS", "PAGE_CRC32",
                     "PREDICATE_PUSHDOWN", "PROJECTION_PUSHDOWN", "SCHEMA_EVOLUTION"]

TOOL_DISPLAY_NAMES = {
    "pyarrow": "PyArrow",
    "fastparquet": "fastparquet",
    "polars": "Polars",
    "duckdb": "DuckDB",
    "parquet-rs": "parquet-rs",
    "parquet-go": "parquet-go",
    "arrow-go": "arrow-go",
    "parquet-java": "parquet-java",
    "parquet-dotnet": "parquet-dotnet",
    "parquet-sharp": "ParquetSharp",
    "hyparquet": "hyparquet",
    "spark": "Apache Spark",
    "trino": "Trino",
}

TOOL_LANGUAGES = {
    "pyarrow": "Python",
    "fastparquet": "Python",
    "polars": "Rust / Python",
    "duckdb": "C++",
    "parquet-rs": "Rust",
    "parquet-go": "Go",
    "arrow-go": "Go",
    "parquet-java": "Java",
    "parquet-dotnet": "C# / .NET",
    "parquet-sharp": "C# / .NET",
    "hyparquet": "JavaScript",
    "spark": "Java / Python",
    "trino": "Java",
}

TOOL_ORDER = ["pyarrow", "fastparquet", "polars", "duckdb",
              "parquet-rs", "parquet-go", "arrow-go", "parquet-java", "parquet-dotnet",
              "parquet-sharp", "hyparquet", "spark", "trino"]

# Encoding × Type combinations that are valid per the Apache Parquet format spec.
# Combinations not listed here are not defined by the spec; if a library cannot
# produce them either, we mark them as "not_applicable" (gray) rather than red.
# Libraries are free to support extras beyond the spec.
SPEC_VALID_ENCODING_TYPES = {
    "PLAIN":                    {"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"},
    "PLAIN_DICTIONARY":         {"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"},
    "RLE_DICTIONARY":           {"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"},
    "RLE":                      {"BOOLEAN", "INT32", "INT64"},
    "BIT_PACKED":               set(),  # deprecated; not for data pages
    "DELTA_BINARY_PACKED":      {"INT32", "INT64"},
    "DELTA_LENGTH_BYTE_ARRAY":  {"BYTE_ARRAY"},
    "DELTA_BYTE_ARRAY":         {"BYTE_ARRAY"},
    "BYTE_STREAM_SPLIT":        {"FLOAT", "DOUBLE", "INT32", "INT64"},
    # BYTE_STREAM_SPLIT_EXTENDED (format 2.11.0) adds FLOAT16 and FIXED_LEN_BYTE_ARRAY;
    # we proxy this with FLOAT as the test type since FLOAT16 is not in ENCODING_TYPES.
    "BYTE_STREAM_SPLIT_EXTENDED": {"FLOAT", "DOUBLE", "INT32", "INT64"},
}

# For running single-version tests (fallback)
TOOLS = {
    "pyarrow": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_pyarrow.py")],
    },
    "fastparquet": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_fastparquet.py")],
    },
    "polars": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_polars.py")],
    },
    "duckdb": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_duckdb.py")],
    },
    "parquet-rs": {
        "build": ["cargo", "build", "--release"],
        "build_cwd": str(CLI_DIR / "rust" / "test_parquet_rs"),
        "run": [str(CLI_DIR / "rust" / "test_parquet_rs" / "target" / "release" / "test_parquet_rs")],
    },
    "parquet-go": {
        "build": ["go", "build", "-o", "test_parquet_go"],
        "build_cwd": str(CLI_DIR / "go" / "test_parquet_go"),
        "run": [str(CLI_DIR / "go" / "test_parquet_go" / "test_parquet_go")],
    },
    "arrow-go": {
        "build": ["go", "build", "-o", "test_arrow_go"],
        "build_cwd": str(CLI_DIR / "go" / "test_arrow_go"),
        "run": [str(CLI_DIR / "go" / "test_arrow_go" / "test_arrow_go")],
    },
    "parquet-java": {
        "build": ["mvn", "-q", "package", "-DskipTests"],
        "build_cwd": str(CLI_DIR / "java" / "test_parquet_java"),
        "run": ["java", "-jar", str(CLI_DIR / "java" / "test_parquet_java" / "target" / "test-parquet-java-1.0-SNAPSHOT.jar")],
    },
    "parquet-dotnet": {
        "build": ["dotnet", "build", "-c", "Release", "-v", "q"],
        "build_cwd": str(CLI_DIR / "dotnet" / "test_parquet_dotnet"),
        "run": ["dotnet", "run", "--project", str(CLI_DIR / "dotnet" / "test_parquet_dotnet"), "-c", "Release", "--no-build"],
    },
    "parquet-sharp": {
        "build": ["dotnet", "build", "-c", "Release", "-v", "q"],
        "build_cwd": str(CLI_DIR / "dotnet" / "test_parquet_sharp"),
        "run": ["dotnet", "run", "--project", str(CLI_DIR / "dotnet" / "test_parquet_sharp"), "-c", "Release", "--no-build"],
    },
    "hyparquet": {
        "build": ["npm", "install", "--prefer-offline"],
        "build_cwd": str(CLI_DIR / "javascript" / "test_hyparquet"),
        "run": ["node", str(CLI_DIR / "javascript" / "test_hyparquet" / "index.js")],
    },
    "spark": {
        "build": None,
        "run": [sys.executable, str(CLI_DIR / "python" / "test_spark.py")],
    },
    "trino": {
        "build": ["mvn", "-q", "package", "-DskipTests"],
        "build_cwd": str(CLI_DIR / "java" / "test_trino"),
        "run": ["java", "-jar", str(CLI_DIR / "java" / "test_trino" / "target" / "test-trino-1.0-SNAPSHOT.jar")],
    },
}


def run_tool(name, tool_config, skip_build=False):
    """Build and run a tool, return its JSON result."""
    print(f"  Testing {name}...", end=" ", flush=True)

    if not skip_build and tool_config.get("build"):
        try:
            subprocess.run(
                tool_config["build"],
                cwd=tool_config.get("build_cwd"),
                capture_output=True, text=True, check=True, timeout=300,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"BUILD FAILED: {e}")
            return None

    try:
        result = subprocess.run(
            tool_config["run"],
            capture_output=True, text=True, check=True, timeout=120,
        )
        data = json.loads(result.stdout)
        print(f"OK (v{data.get('version', '?')})")
        return data
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired) as e:
        print(f"FAILED: {e}")
        return None


def find_first_version(version_results, category, feature, sub_feature=None, access="write"):
    """Find the first version where a feature was supported (for write or read)."""
    for vr in version_results:
        cat_data = vr.get(category, {})
        if sub_feature:
            val = cat_data.get(feature, {})
            if isinstance(val, dict):
                val = val.get(sub_feature)
        else:
            val = cat_data.get(feature)
        # Handle both old bool format and new {"write": bool, "read": bool} format
        if isinstance(val, dict) and ("write" in val or "read" in val):
            val = val.get(access)
        if val is True:
            return vr.get("tested_version") or vr.get("version")
    return None


def _get_rw(entry, access):
    """Extract write or read support from an entry (handles old bool and new dict format)."""
    if isinstance(entry, bool):
        return entry
    if isinstance(entry, dict):
        if "write" in entry or "read" in entry:
            return bool(entry.get(access, False))
        # not_applicable or other metadata dict - no rw keys
        return False
    return bool(entry) if entry is not None else False


def _version_sort_key(filepath, tool_id):
    """Return a sortable key for a versioned result file by semantic version."""
    version_str = filepath.stem[len(tool_id) + 1:]
    try:
        return tuple(int(x) for x in version_str.split("."))
    except ValueError:
        return (float("inf"), float("inf"), float("inf"))


def load_multiversion_results():
    """Load multi-version results from results/all_versions.json or individual files."""
    combined_file = RESULTS_DIR / "all_versions.json"
    if combined_file.exists():
        with open(combined_file) as f:
            return json.load(f)

    # Fall back to individual result files
    results = {}
    for tool_id in TOOL_ORDER:
        tool_results = []
        # Look for versioned result files, sorted by semantic version
        for f in sorted(RESULTS_DIR.glob(f"{tool_id}-*.json"),
                        key=lambda f: _version_sort_key(f, tool_id)):
            with open(f) as fh:
                tool_results.append(json.load(fh))
        # Fall back to un-versioned result file
        if not tool_results:
            single = RESULTS_DIR / f"{tool_id}.json"
            if single.exists():
                with open(single) as fh:
                    tool_results.append(json.load(fh))
        if tool_results:
            results[tool_id] = tool_results
    return results


def load_version_dates() -> dict:
    """Load version release dates from versions.json (version_dates field per tool)."""
    if VERSIONS_FILE.exists():
        with open(VERSIONS_FILE) as f:
            config = json.load(f)
        return {
            tool_id: tool_cfg.get("version_dates", {})
            for tool_id, tool_cfg in config.items()
            if not tool_id.startswith("_")
        }
    return {}


def load_all_versions() -> dict:
    """Load all versions from versions.json (ordered oldest to newest) per tool."""
    if VERSIONS_FILE.exists():
        with open(VERSIONS_FILE) as f:
            config = json.load(f)
        return {
            tool_id: tool_cfg.get("versions", [])
            for tool_id, tool_cfg in config.items()
            if not tool_id.startswith("_")
        }
    return {}


def build_matrix_data(multiversion_results):
    """Build the complete matrix data structure for the site."""
    version_dates = load_version_dates()
    all_versions_map = load_all_versions()

    matrix = {
        "tools": {},
        "categories": {
            "compression": COMPRESSION_CODECS,
            "encoding": ENCODINGS,
            "encoding_types": ENCODING_TYPES,
            "logical_types": LOGICAL_TYPES,
            "nested_types": NESTED_TYPES,
            "advanced_features": ADVANCED_FEATURES,
        },
    }

    for tool_id in TOOL_ORDER:
        version_results = multiversion_results.get(tool_id, [])
        if not version_results:
            continue

        latest = version_results[-1]
        tested_versions = [vr.get("tested_version") or vr.get("version") for vr in version_results]

        tool_data = {
            "display_name": TOOL_DISPLAY_NAMES.get(tool_id, tool_id),
            "language": TOOL_LANGUAGES.get(tool_id, "?"),
            "latest_version": latest.get("version", "?"),
            "tested_versions": tested_versions,
            "all_versions": all_versions_map.get(tool_id, []),
            "version_dates": version_dates.get(tool_id, {}),
            "compression": {},
            "encoding": {},
            "logical_types": {},
            "nested_types": {},
            "advanced_features": {},
        }

        # Compression
        for codec in COMPRESSION_CODECS:
            entry = latest.get("compression", {}).get(codec)
            write_ok = _get_rw(entry, "write")
            read_ok = _get_rw(entry, "read")
            write_since = find_first_version(version_results, "compression", codec, access="write")
            read_since = find_first_version(version_results, "compression", codec, access="read")
            tool_data["compression"][codec] = {
                "write": write_ok,
                "write_since": write_since,
                "read": read_ok,
                "read_since": read_since,
            }

        # Encoding x Type
        for enc in ENCODINGS:
            tool_data["encoding"][enc] = {}
            enc_data = latest.get("encoding", {}).get(enc, {})
            for ptype in ENCODING_TYPES:
                if isinstance(enc_data, dict) and ("write" in enc_data or "read" in enc_data):
                    # enc_data is itself a rw entry (whole encoding)
                    entry = enc_data
                elif isinstance(enc_data, dict):
                    entry = enc_data.get(ptype)
                elif isinstance(enc_data, bool):
                    entry = enc_data
                else:
                    entry = None
                write_ok = _get_rw(entry, "write")
                read_ok = _get_rw(entry, "read")
                write_since = find_first_version(version_results, "encoding", enc, ptype, access="write")
                read_since = find_first_version(version_results, "encoding", enc, ptype, access="read")
                is_supported = write_ok or read_ok
                cell = {
                    "write": write_ok,
                    "write_since": write_since,
                    "read": read_ok,
                    "read_since": read_since,
                }
                # If neither supported and the spec doesn't define this combination,
                # mark as not_applicable (shown as gray) instead of red.
                if not is_supported:
                    spec_valid = SPEC_VALID_ENCODING_TYPES.get(enc, set())
                    if ptype not in spec_valid:
                        cell["not_applicable"] = True
                tool_data["encoding"][enc][ptype] = cell

        # Logical Types
        for lt in LOGICAL_TYPES:
            entry = latest.get("logical_types", {}).get(lt)
            write_ok = _get_rw(entry, "write")
            read_ok = _get_rw(entry, "read")
            write_since = find_first_version(version_results, "logical_types", lt, access="write")
            read_since = find_first_version(version_results, "logical_types", lt, access="read")
            tool_data["logical_types"][lt] = {
                "write": write_ok,
                "write_since": write_since,
                "read": read_ok,
                "read_since": read_since,
            }

        # Nested Types
        for nt in NESTED_TYPES:
            entry = latest.get("nested_types", {}).get(nt)
            write_ok = _get_rw(entry, "write")
            read_ok = _get_rw(entry, "read")
            write_since = find_first_version(version_results, "nested_types", nt, access="write")
            read_since = find_first_version(version_results, "nested_types", nt, access="read")
            tool_data["nested_types"][nt] = {
                "write": write_ok,
                "write_since": write_since,
                "read": read_ok,
                "read_since": read_since,
            }

        # Advanced Features
        for af in ADVANCED_FEATURES:
            entry = latest.get("advanced_features", {}).get(af)
            write_ok = _get_rw(entry, "write")
            read_ok = _get_rw(entry, "read")
            write_since = find_first_version(version_results, "advanced_features", af, access="write")
            read_since = find_first_version(version_results, "advanced_features", af, access="read")
            tool_data["advanced_features"][af] = {
                "write": write_ok,
                "write_since": write_since,
                "read": read_ok,
                "read_since": read_since,
            }

        matrix["tools"][tool_id] = tool_data

    available_tools = list(matrix["tools"].keys())
    matrix["build_metadata"] = {
        "expected_tools": TOOL_ORDER,
        "available_tools": available_tools,
        "missing_tools": [t for t in TOOL_ORDER if t not in available_tools],
    }

    return matrix


def symbol(entry):
    """Convert a feature entry to a markdown symbol."""
    if isinstance(entry, dict):
        if entry.get("not_applicable"):
            return "➖"
        # New format with write/read keys
        if "write" in entry or "read" in entry:
            write = entry.get("write", False)
            read = entry.get("read", False)
            write_since = entry.get("write_since")
            read_since = entry.get("read_since")
            if write and read:
                since = write_since or read_since
                return f"✅ {since}+" if since else "✅"
            elif write and not read:
                since = f" {write_since}+" if write_since else ""
                return f"W✅{since} R❌"
            elif not write and read:
                since = f" {read_since}+" if read_since else ""
                return f"W❌ R✅{since}"
            else:
                return "❌"
        # Legacy format
        if entry.get("supported"):
            since = entry.get("since")
            if since:
                return f"✅ {since}+"
            return "✅"
        return "❌"
    if entry is True:
        return "✅"
    if entry is False:
        return "❌"
    return "➖"


def generate_markdown(matrix_data):
    """Generate markdown from matrix data."""
    tools = matrix_data["tools"]
    tool_ids = list(tools.keys())
    tool_names = [tools[t]["display_name"] for t in tool_ids]
    lines = []

    lines.append("# Can I Use: Parquet Format Support Matrix")
    lines.append("")
    lines.append("A comprehensive compatibility reference for Apache Parquet features across libraries and query engines.")
    lines.append("**This matrix is auto-generated by running actual tests against each library version.**")
    lines.append("")
    lines.append("> **Legend:** ✅ X.Y.Z+ = Supported (verified since version X.Y.Z) | ❌ = Not supported | ➖ = Not tested")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Tools & Versions
    lines.append("## Tools & Versions Tested")
    lines.append("")
    lines.append("| Tool | Latest Version | Versions Tested | Language |")
    lines.append("|---|---|---|---|")
    for tid in tool_ids:
        t = tools[tid]
        versions_str = ", ".join(t.get("tested_versions", []))
        lines.append(f"| {t['display_name']} | {t['latest_version']} | {versions_str} | {t['language']} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Compression
    lines.append("## Compression Codecs")
    lines.append("")
    header = "| Codec | " + " | ".join(tool_names) + " |"
    sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
    lines.append(header)
    lines.append(sep)
    for codec in COMPRESSION_CODECS:
        cells = [symbol(tools[t]["compression"].get(codec, {})) for t in tool_ids]
        lines.append(f"| {codec} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Encoding x Type
    lines.append("## Encoding Types × Data Types")
    lines.append("")
    for enc in ENCODINGS:
        lines.append(f"### {enc}")
        lines.append("")
        header = "| Type | " + " | ".join(tool_names) + " |"
        sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
        lines.append(header)
        lines.append(sep)
        for ptype in ENCODING_TYPES:
            cells = [symbol(tools[t]["encoding"].get(enc, {}).get(ptype, {})) for t in tool_ids]
            lines.append(f"| {ptype} | " + " | ".join(cells) + " |")
        lines.append("")
    lines.append("---")
    lines.append("")

    # Logical Types
    lines.append("## Logical Types")
    lines.append("")
    header = "| Logical Type | " + " | ".join(tool_names) + " |"
    sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
    lines.append(header)
    lines.append(sep)
    for lt in LOGICAL_TYPES:
        cells = [symbol(tools[t]["logical_types"].get(lt, {})) for t in tool_ids]
        lines.append(f"| {lt} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Nested Types
    lines.append("## Nested & Complex Types")
    lines.append("")
    header = "| Type | " + " | ".join(tool_names) + " |"
    sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
    lines.append(header)
    lines.append(sep)
    for nt in NESTED_TYPES:
        cells = [symbol(tools[t]["nested_types"].get(nt, {})) for t in tool_ids]
        lines.append(f"| {nt} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Advanced Features
    lines.append("## Advanced Features")
    lines.append("")
    header = "| Feature | " + " | ".join(tool_names) + " |"
    sep = "|---|" + "|".join(["---"] * len(tool_names)) + "|"
    lines.append(header)
    lines.append(sep)
    for af in ADVANCED_FEATURES:
        cells = [symbol(tools[t]["advanced_features"].get(af, {})) for t in tool_ids]
        lines.append(f"| {af} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("---")
    lines.append("")

    lines.append("## How to Reproduce")
    lines.append("")
    lines.append("```bash")
    lines.append("cd parquet_can_i_use")
    lines.append("python run_multiversion.py  # Test multiple versions")
    lines.append("python generate_matrix.py --load-results  # Generate matrix from results")
    lines.append("```")
    lines.append("")
    lines.append("*Auto-generated from verified test results.*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet compatibility matrix")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--load-results", action="store_true")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)

    if args.load_results:
        multiversion_results = load_multiversion_results()
    else:
        tools_to_run = args.only if args.only else list(TOOLS.keys())
        multiversion_results = {}

        print("Running Parquet feature tests...")
        print()

        for tool_id in TOOL_ORDER:
            if tool_id in tools_to_run and tool_id in TOOLS:
                data = run_tool(tool_id, TOOLS[tool_id], skip_build=args.skip_build)
                if data:
                    multiversion_results[tool_id] = [data]
                    with open(RESULTS_DIR / f"{tool_id}.json", "w") as f:
                        json.dump(data, f, indent=2)
            else:
                single = RESULTS_DIR / f"{tool_id}.json"
                if single.exists():
                    with open(single) as f:
                        multiversion_results[tool_id] = [json.load(f)]

    matrix_data = build_matrix_data(multiversion_results)

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(matrix_data, f, indent=2)
    print(f"JSON data written to {OUTPUT_JSON}")

    markdown = generate_markdown(matrix_data)
    with open(OUTPUT_MD, "w") as f:
        f.write(markdown)
    print(f"Markdown written to {OUTPUT_MD}")


if __name__ == "__main__":
    main()
