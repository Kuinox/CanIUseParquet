#!/usr/bin/env python3
"""
Compare CanIUseParquet results with the Apache Parquet implementation status page.

The Apache page data is fetched from https://github.com/apache/parquet-site/tree/asf-site/data/implementations/support

Highlights potential bugs:
  🔴 Apache says supported but we say not — may indicate a bug in our test or an outdated result.
  🟡 We say supported but Apache says not — may indicate a false positive in our test.

Usage:
    python compare_with_apache.py [--tool TOOL...]
"""

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RESULTS_DIR = SCRIPT_DIR / "results"

# ---------------------------------------------------------------------------
# Feature mapping: Apache feature ID → (our category, our feature key)
# ---------------------------------------------------------------------------
FEATURE_MAP = {
    # Compression
    "compression-uncompressed":   ("compression", "NONE"),
    "compression-snappy":         ("compression", "SNAPPY"),
    "compression-gzip":           ("compression", "GZIP"),
    "compression-brotli":         ("compression", "BROTLI"),
    "compression-lzo":            ("compression", "LZO"),
    "compression-lz4-deprecated": ("compression", "LZ4"),
    "compression-lz4-raw":        ("compression", "LZ4_RAW"),
    "compression-zstd":           ("compression", "ZSTD"),
    # Logical types
    "logical-string":             ("logical_types", "STRING"),
    "logical-date":               ("logical_types", "DATE"),
    "logical-time-int32":         ("logical_types", "TIME_MILLIS"),
    "logical-time-int64":         ("logical_types", "TIME_MICROS"),
    "logical-timestamp-int64":    ("logical_types", "TIMESTAMP_MICROS"),
    "logical-decimal-int32":      ("logical_types", "DECIMAL"),
    "logical-decimal-int64":      ("logical_types", "DECIMAL"),
    "logical-float16":            ("logical_types", "FLOAT16"),
    "logical-uuid":               ("logical_types", "UUID"),
    "logical-json":               ("logical_types", "JSON"),
    "logical-bson":               ("logical_types", "BSON"),
    "logical-enum":               ("logical_types", "ENUM"),
    "logical-interval":           ("logical_types", "INTERVAL"),
    "logical-variant":            ("logical_types", "VARIANT"),
    "logical-geometry":           ("logical_types", "GEOMETRY"),
    "logical-geography":          ("logical_types", "GEOGRAPHY"),
    "logical-list":               ("nested_types",  "LIST"),
    "logical-map":                ("nested_types",  "MAP"),
    "logical-unknown":            ("logical_types", "UNKNOWN"),
    # Encodings (evaluated as any-type-supported)
    "encoding-plain":                    ("encoding_overall", "PLAIN"),
    "encoding-plain-dictionary":         ("encoding_overall", "PLAIN_DICTIONARY"),
    "encoding-rle-dictionary":           ("encoding_overall", "RLE_DICTIONARY"),
    "encoding-rle":                      ("encoding_overall", "RLE"),
    "encoding-bit-packed":               ("encoding_overall", "BIT_PACKED"),
    "encoding-delta-binary-packed":      ("encoding_overall", "DELTA_BINARY_PACKED"),
    "encoding-delta-length-byte-array":  ("encoding_overall", "DELTA_LENGTH_BYTE_ARRAY"),
    "encoding-delta-byte-array":         ("encoding_overall", "DELTA_BYTE_ARRAY"),
    "encoding-byte-stream-split":        ("encoding_overall", "BYTE_STREAM_SPLIT"),
    "encoding-byte-stream-split-extended": ("encoding_overall", "BYTE_STREAM_SPLIT_EXTENDED"),
    # Format features
    "format-bloom-filters":      ("advanced_features", "BLOOM_FILTER"),
    "format-page-index":         ("advanced_features", "PAGE_INDEX"),
    "format-data-page-v2":       ("advanced_features", "DATA_PAGE_V2"),
    "format-stats-min-max":      ("advanced_features", "STATISTICS"),
    "format-modular-encryption": ("advanced_features", "COLUMN_ENCRYPTION"),
    "format-size-statistics":    ("advanced_features", "SIZE_STATISTICS"),
    "format-page-crc32":         ("advanced_features", "PAGE_CRC32"),
}

# ---------------------------------------------------------------------------
# Apache implementation status data (manually synced from parquet-site repo).
# Source: https://github.com/apache/parquet-site/tree/asf-site/data/implementations/support
# Update this dict when Apache updates their page.
# ---------------------------------------------------------------------------
APACHE_DATA = {
    "arrow-rs": {
        "compression-uncompressed": "full", "compression-snappy": "full",
        "compression-gzip": "full", "compression-brotli": "full",
        "compression-lzo": "none", "compression-lz4-deprecated": "full",
        "compression-lz4-raw": "full", "compression-zstd": "full",
        "logical-string": "full", "logical-date": "full",
        "logical-time-int32": "full", "logical-time-int64": "full",
        "logical-timestamp-int64": "full",
        "logical-decimal-int32": "full", "logical-decimal-int64": "full",
        "logical-float16": "full", "logical-uuid": "full",
        "logical-json": "full", "logical-bson": "full",
        "logical-enum": "full", "logical-interval": "full",
        "logical-variant": "full",  # since 56.0.0
        "logical-geometry": "full",  # since 57.0.0
        "logical-geography": "full",  # since 57.0.0
        "logical-list": "full", "logical-map": "full", "logical-unknown": "full",
        "encoding-plain": "full", "encoding-plain-dictionary": "full",
        "encoding-rle-dictionary": "full", "encoding-rle": "full",
        "encoding-bit-packed": "none",
        "encoding-delta-binary-packed": "full",
        "encoding-delta-length-byte-array": "full",
        "encoding-delta-byte-array": "full",
        "encoding-byte-stream-split": "full",
        "encoding-byte-stream-split-extended": "full",
        "format-bloom-filters": "full", "format-page-index": "full",
        "format-data-page-v2": "full", "format-stats-min-max": "full",
        "format-modular-encryption": "full",  # since 54.3.0 read, 55.0.0 write
        "format-size-statistics": "full", "format-page-crc32": "full",
    },
    "polars": {
        "compression-uncompressed": "full", "compression-snappy": "full",
        "compression-gzip": "full", "compression-brotli": "full",
        "compression-lzo": "read", "compression-lz4-deprecated": "none",
        "compression-lz4-raw": "full", "compression-zstd": "full",
        "logical-string": "full", "logical-date": "full",
        "logical-time-int32": "read", "logical-time-int64": "full",
        "logical-timestamp-int64": "full",
        "logical-decimal-int32": "read", "logical-decimal-int64": "read",
        "logical-float16": "full", "logical-uuid": "read",
        "logical-json": "none", "logical-bson": "none",
        "logical-enum": "read", "logical-interval": "read",
        "logical-variant": "none", "logical-geometry": "read",
        "logical-geography": "read",
        "logical-list": "full", "logical-map": "read", "logical-unknown": "full",
        "encoding-plain": "full", "encoding-plain-dictionary": "read",
        "encoding-rle-dictionary": "full", "encoding-rle": "read",
        "encoding-bit-packed": "none",
        "encoding-delta-binary-packed": "read",
        "encoding-delta-length-byte-array": "read",
        "encoding-delta-byte-array": "read",
        "encoding-byte-stream-split": "read",
        "encoding-byte-stream-split-extended": "none",
        "format-bloom-filters": "none", "format-page-index": "read",
        "format-data-page-v2": "read", "format-stats-min-max": "full",
        "format-modular-encryption": "none",
        "format-size-statistics": "read", "format-page-crc32": "read",
    },
    "duckdb": {
        "compression-uncompressed": "full", "compression-snappy": "full",
        "compression-gzip": "full", "compression-brotli": "full",
        "compression-lzo": "none", "compression-lz4-deprecated": "none",
        "compression-lz4-raw": "full", "compression-zstd": "full",
        "logical-string": "full", "logical-date": "full",
        "logical-time-int32": "full", "logical-time-int64": "full",
        "logical-timestamp-int64": "full",
        "logical-decimal-int32": "full", "logical-decimal-int64": "full",
        "logical-float16": "full",  # since 1.3.0
        "logical-uuid": "full", "logical-json": "full",
        "logical-bson": "none",
        "logical-enum": "full", "logical-interval": "full",
        "logical-variant": "full",   # since 1.4.0
        "logical-geometry": "full",  # since 1.4.0, requires spatial extension
        "logical-geography": "full",  # since 1.4.0, requires spatial extension
        "logical-list": "full", "logical-map": "full", "logical-unknown": "full",
        "encoding-plain": "full", "encoding-plain-dictionary": "read",
        "encoding-rle-dictionary": "full", "encoding-rle": "full",
        "encoding-bit-packed": "none",
        "encoding-delta-binary-packed": "full",
        "encoding-delta-length-byte-array": "full",
        "encoding-delta-byte-array": "full",
        "encoding-byte-stream-split": "full",
        "encoding-byte-stream-split-extended": "full",  # since 1.2.0
        "format-bloom-filters": "full", "format-page-index": "read",
        "format-data-page-v2": "full", "format-stats-min-max": "full",
        "format-modular-encryption": "full",
        "format-size-statistics": "read", "format-page-crc32": "read",
    },
    "parquet-java": {
        "compression-uncompressed": "full", "compression-snappy": "full",
        "compression-gzip": "full", "compression-brotli": "full",
        "compression-lzo": "none", "compression-lz4-deprecated": "none",
        "compression-lz4-raw": "full", "compression-zstd": "full",
        "logical-string": "full", "logical-date": "full",
        "logical-time-int32": "full", "logical-time-int64": "full",
        "logical-timestamp-int64": "full",
        "logical-decimal-int32": "full", "logical-decimal-int64": "full",
        "logical-float16": "full",
        "logical-uuid": "full", "logical-json": "full",
        "logical-bson": "full",
        "logical-enum": "full", "logical-interval": "full",
        "logical-variant": "full",  # since 1.16.0
        "logical-geometry": "full",  # since 1.16.0
        "logical-geography": "full",  # since 1.16.0
        "logical-list": "full", "logical-map": "full", "logical-unknown": "full",
        "encoding-plain": "full", "encoding-plain-dictionary": "full",
        "encoding-rle-dictionary": "full", "encoding-rle": "full",
        "encoding-bit-packed": "full",
        "encoding-delta-binary-packed": "full",
        "encoding-delta-length-byte-array": "full",
        "encoding-delta-byte-array": "full",
        "encoding-byte-stream-split": "full",
        "encoding-byte-stream-split-extended": "full",
        "format-bloom-filters": "full", "format-page-index": "full",
        "format-data-page-v2": "full", "format-stats-min-max": "full",
        "format-modular-encryption": "full",
        "format-size-statistics": "full", "format-page-crc32": "full",
    },
}

# Our tool ID → Apache engine ID
LIBRARY_MAP = {
    "parquet-rs":   "arrow-rs",
    "polars":       "polars",
    "duckdb":       "duckdb",
    "parquet-java": "parquet-java",
}

# Apache version at which a feature was introduced (for context in reports)
APACHE_VERSION_NOTES = {
    ("arrow-rs", "logical-variant"):           "since v56.0.0",
    ("arrow-rs", "logical-geometry"):          "since v57.0.0",
    ("arrow-rs", "logical-geography"):         "since v57.0.0",
    ("arrow-rs", "format-modular-encryption"): "since v54.3.0 read / v55.0.0 write",
    ("duckdb",   "logical-float16"):           "since v1.3.0",
    ("duckdb",   "logical-variant"):           "since v1.4.0",
    ("duckdb",   "logical-geometry"):          "since v1.4.0; requires spatial extension",
    ("duckdb",   "logical-geography"):         "since v1.4.0; requires spatial extension",
    ("parquet-java", "logical-variant"):       "since v1.16.0",
    ("parquet-java", "logical-geometry"):      "since v1.16.0",
    ("parquet-java", "logical-geography"):     "since v1.16.0",
}


def get_our_support(our_data: dict, category: str, feature: str):
    """Return (write, read) booleans from our result data."""
    if category == "encoding_overall":
        enc_data = our_data.get("encoding", {}).get(feature, {})
        if not enc_data:
            return (False, False)
        writes = any(v.get("write", False) for v in enc_data.values() if isinstance(v, dict))
        reads  = any(v.get("read",  False) for v in enc_data.values() if isinstance(v, dict))
        return (writes, reads)
    entry = our_data.get(category, {}).get(feature, {})
    if isinstance(entry, dict):
        return (entry.get("write", False), entry.get("read", False))
    return (False, False)


def apache_to_rw(status: str):
    """Convert Apache status string to (write_supported, read_supported) booleans."""
    return {
        "full":  (True,  True),
        "read":  (False, True),
        "write": (True,  False),
        "none":  (False, False),
    }.get(status, (False, False))


def compare_tool(our_id: str, apache_id: str, our_data: dict, verbose: bool = False):
    """Compare a single tool's data against Apache's status. Returns (apache_more, we_more)."""
    apache_support = APACHE_DATA.get(apache_id, {})
    our_version = our_data.get("version", "?")
    apache_more, we_more = [], []

    seen_features = set()  # avoid reporting the same (cat, feat) twice
    for apache_feat, (our_cat, our_feat) in FEATURE_MAP.items():
        key = (our_cat, our_feat)
        apache_status = apache_support.get(apache_feat)
        if apache_status is None:
            continue

        a_write, a_read = apache_to_rw(apache_status)
        o_write, o_read = get_our_support(our_data, our_cat, our_feat)

        if a_write == o_write and a_read == o_read:
            continue
        if key in seen_features:
            continue
        seen_features.add(key)

        note = APACHE_VERSION_NOTES.get((apache_id, apache_feat), "")
        entry = {
            "feature":        f"{our_cat}/{our_feat}",
            "apache_id":      apache_feat,
            "apache_status":  apache_status,
            "our_write":      o_write,
            "our_read":       o_read,
            "note":           note,
        }

        if a_write > o_write or a_read > o_read:
            apache_more.append(entry)
        else:
            we_more.append(entry)

    return apache_more, we_more, our_version


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tool", nargs="*", help="Only compare specific tools (e.g. polars duckdb)")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    args = parser.parse_args()

    tools_to_check = {k: v for k, v in LIBRARY_MAP.items()
                      if (not args.tool or k in args.tool)}

    if not tools_to_check:
        print("No tools matched. Available:", ", ".join(LIBRARY_MAP.keys()))
        sys.exit(1)

    all_results = {}
    grand_apache_more = 0
    grand_we_more = 0

    for our_id, apache_id in tools_to_check.items():
        result_file = RESULTS_DIR / f"{our_id}.json"
        if not result_file.exists():
            print(f"[{our_id}] result file not found: {result_file}", file=sys.stderr)
            continue

        with open(result_file) as f:
            our_data = json.load(f)

        apache_more, we_more, our_version = compare_tool(our_id, apache_id, our_data)
        all_results[our_id] = {
            "our_version":  our_version,
            "apache_id":    apache_id,
            "apache_more":  apache_more,
            "we_more":      we_more,
        }
        grand_apache_more += len(apache_more)
        grand_we_more += len(we_more)

    if args.json:
        print(json.dumps(all_results, indent=2))
        return

    print("=" * 80)
    print("DISCREPANCIES vs APACHE PARQUET IMPLEMENTATION STATUS PAGE")
    print("  Source: https://parquet.apache.org/docs/file-format/implementationstatus/")
    print("=" * 80)

    for our_id, data in all_results.items():
        apache_more = data["apache_more"]
        we_more     = data["we_more"]
        print(f"\n{'─' * 70}")
        print(f"  {our_id}  (Apache: {data['apache_id']})  —  tested version: {data['our_version']}")
        print(f"{'─' * 70}")

        if not apache_more and not we_more:
            print("  ✓ No discrepancies")
            continue

        if apache_more:
            print(f"\n  🔴 Apache says SUPPORTED but we say NOT ({len(apache_more)}):")
            print("     Possible causes: bug in our test, outdated result, or version mismatch.")
            for d in apache_more:
                note = f"  [{d['note']}]" if d["note"] else ""
                print(f"     [{d['apache_status']:5s}] {d['feature']:<40s}  "
                      f"ours W={int(d['our_write'])} R={int(d['our_read'])}{note}")

        if we_more:
            print(f"\n  🟡 We say SUPPORTED but Apache says NOT ({len(we_more)}):")
            print("     Possible causes: false positive in our test, or library quirk.")
            for d in we_more:
                note = f"  [{d['note']}]" if d["note"] else ""
                print(f"     [{d['apache_status']:5s}] {d['feature']:<40s}  "
                      f"ours W={int(d['our_write'])} R={int(d['our_read'])}{note}")

    print(f"\n{'=' * 80}")
    print(f"TOTAL: {grand_apache_more} where Apache says more, "
          f"{grand_we_more} where we say more")
    print("=" * 80)


if __name__ == "__main__":
    main()
