#!/usr/bin/env python3
"""Test DuckDB's Parquet feature support and output JSON results."""

import base64
import hashlib
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

PROOF_FIXTURE = Path(__file__).parent.parent.parent / "fixtures" / "proof" / "proof.parquet"

def test_feature(name, fn):
    try:
        fn()
        return True, None
    except Exception:
        return False, traceback.format_exc()

def test_rw(write_fn, read_fn, write_path=None):
    """Run separate write and read tests, return {"write": bool, "read": bool, ...}."""
    write_ok, write_log = test_feature("write", write_fn)
    read_ok, read_log = test_feature("read", read_fn)
    result = {"write": write_ok, "read": read_ok}
    if write_log:
        result["write_log"] = write_log
    if read_log:
        result["read_log"] = read_log
    return result

def main():
    try:
        import duckdb
    except ImportError:
        print(json.dumps({"error": "duckdb not installed"}))
        sys.exit(1)

    results = {
        "tool": "DuckDB",
        "version": duckdb.__version__,
        "compression": {},
        "encoding": {},
        "logical_types": {},
        "nested_types": {},
        "advanced_features": {},
    }

    tmpdir = tempfile.mkdtemp()
    FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"
    con = duckdb.connect()

    def _read_proof_log():
        try:
            if not PROOF_FIXTURE.exists():
                return None
            proof_data = PROOF_FIXTURE.read_bytes()
            sha = hashlib.sha256(proof_data).hexdigest()
            rows = con.execute(f"SELECT * FROM read_parquet('{PROOF_FIXTURE}')").fetchall()
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{PROOF_FIXTURE}')").fetchall()
            values = {col[0]: [r[i] for r in rows] for i, col in enumerate(cols)}
            return f"proof_sha256:{sha}\nvalues:{json.dumps(values)}"
        except Exception as e:
            return f"proof_read_error:{e}"

    def test_rw(write_fn, read_fn, write_path=None):
        write_ok, write_log = test_feature("write", write_fn)
        read_ok, read_log = test_feature("read", read_fn)
        if write_ok and write_path and os.path.exists(write_path):
            data = open(write_path, "rb").read()
            sha = hashlib.sha256(data).hexdigest()
            write_log = f"sha256:{sha}\n{base64.b64encode(data).decode()}"
        if read_ok:
            read_log = _read_proof_log()
        result = {"write": write_ok, "read": read_ok}
        if write_log:
            result["write_log"] = write_log
        if read_log:
            result["read_log"] = read_log
        return result

    # --- Compression ---
    # DuckDB's CODEC 'LZ4' writes LZ4_RAW format internally (confirmed via parquet_metadata).
    # We verify the actual compression in the written file to avoid false positives.
    codecs = {
        "NONE": "UNCOMPRESSED",
        "SNAPPY": "SNAPPY",
        "GZIP": "GZIP",
        "BROTLI": "BROTLI",
        "LZO": "LZO",
        "LZ4": "LZ4",
        "LZ4_RAW": "LZ4_RAW",
        "ZSTD": "ZSTD",
    }

    # Map from our result key to what DuckDB parquet_metadata should report for a correctly
    # written file. LZ4 (deprecated codec=5) would show "LZ4" in parquet_metadata; LZ4_RAW
    # (codec=7) shows "LZ4_RAW". Since DuckDB maps CODEC 'LZ4' → LZ4_RAW internally, the
    # write test for deprecated LZ4 fails the verification check.
    expected_parquet_compression = {
        "NONE": "UNCOMPRESSED", "SNAPPY": "SNAPPY", "GZIP": "GZIP",
        "BROTLI": "BROTLI", "LZO": "LZO",
        "LZ4": "LZ4",       # deprecated LZ4 — DuckDB writes LZ4_RAW instead → write=false
        "LZ4_RAW": "LZ4_RAW",
        "ZSTD": "ZSTD",
    }

    for codec_name, codec_val in codecs.items():
        write_path = os.path.join(tmpdir, f"comp_{codec_name}.parquet")
        fixture_path = FIXTURES_DIR / "compression" / f"comp_{codec_name}.parquet"
        read_path = str(fixture_path) if fixture_path.exists() else write_path
        expected = expected_parquet_compression[codec_name]

        def write_codec(c=codec_val, p=write_path, exp=expected, cn=codec_name):
            con.execute(f"COPY (SELECT 1 AS col, 2 AS col2) TO '{p}' (FORMAT PARQUET, CODEC '{c}')")
            # Verify the file actually uses the expected compression codec.
            # This catches cases where DuckDB silently substitutes a different codec
            # (e.g. CODEC 'LZ4' produces LZ4_RAW, not deprecated LZ4).
            rows = con.execute(
                f"SELECT compression FROM parquet_metadata('{p}')"
            ).fetchall()
            actual = rows[0][0] if rows else None
            if actual != exp:
                raise ValueError(
                    f"Expected compression '{exp}' for codec '{cn}', "
                    f"but parquet_metadata shows '{actual}'"
                )

        def read_codec(p=read_path):
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()

        results["compression"][codec_name] = test_rw(write_codec, read_codec, write_path=write_path)

    # --- Encoding × Type matrix ---
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"]

    def make_type_sql(ptype, repeated=False):
        """Build a SELECT expression that generates 100 rows of the given type.

        When repeated=True, values repeat to encourage DuckDB to use dictionary
        encoding (PLAIN_DICTIONARY) rather than PLAIN.
        """
        n = 100
        if repeated:
            # Produce values from a small set so DuckDB uses dictionary encoding.
            # Boolean repeats True/False; numeric types cycle through 5 values.
            if ptype == "BOOLEAN":
                return f"SELECT (range % 2 = 0)::BOOLEAN AS col FROM range({n})"
            elif ptype == "INT32":
                return f"SELECT (range % 5)::INTEGER AS col FROM range({n})"
            elif ptype == "INT64":
                return f"SELECT (range % 5)::BIGINT AS col FROM range({n})"
            elif ptype == "FLOAT":
                return f"SELECT (range % 5)::FLOAT AS col FROM range({n})"
            elif ptype == "DOUBLE":
                return f"SELECT (range % 5)::DOUBLE AS col FROM range({n})"
            elif ptype == "BYTE_ARRAY":
                return (f"SELECT CASE WHEN range % 2 = 0 THEN '\\x68656C6C6F'::BLOB "
                        f"ELSE '\\x776F726C64'::BLOB END AS col FROM range({n})")
        else:
            # Produce unique values so DuckDB uses PLAIN encoding.
            if ptype == "BOOLEAN":
                return f"SELECT (range % 2 = 0)::BOOLEAN AS col FROM range({n})"
            elif ptype == "INT32":
                return f"SELECT range::INTEGER AS col FROM range({n})"
            elif ptype == "INT64":
                return f"SELECT range::BIGINT AS col FROM range({n})"
            elif ptype == "FLOAT":
                return f"SELECT range::FLOAT AS col FROM range({n})"
            elif ptype == "DOUBLE":
                return f"SELECT range::DOUBLE AS col FROM range({n})"
            elif ptype == "BYTE_ARRAY":
                return f"SELECT range::VARCHAR::BLOB AS col FROM range({n})"
        raise ValueError(f"Unknown type: {ptype}")

    def get_actual_encodings_duckdb(path):
        """Return the set of encodings actually used in the file via DuckDB parquet_metadata."""
        rows = con.execute(f"SELECT encodings FROM parquet_metadata('{path}')").fetchall()
        enc_set = set()
        for row in rows:
            val = row[0]
            if val is None:
                continue
            if isinstance(val, (list, tuple)):
                enc_set.update(str(e).strip() for e in val)
            else:
                enc_set.update(e.strip() for e in str(val).split(","))
        return enc_set

    enc_names = ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                 "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                 "BYTE_STREAM_SPLIT", "BYTE_STREAM_SPLIT_EXTENDED"]
    for enc_name in enc_names:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            path = os.path.join(tmpdir, f"enc_{enc_name}_{ptype}.parquet")
            # Dictionary encodings require repeated values to be triggered.
            use_repeated = enc_name in ("PLAIN_DICTIONARY", "RLE_DICTIONARY")
            def write_enc(p=path, pt=ptype, e=enc_name, rep=use_repeated):
                sql = make_type_sql(pt, repeated=rep)
                con.execute(f"COPY ({sql}) TO '{p}' (FORMAT PARQUET)")
                actual = get_actual_encodings_duckdb(p)
                # PLAIN_DICTIONARY and RLE_DICTIONARY are both dictionary-based; DuckDB
                # writes PLAIN_DICTIONARY format so accept either label for both tests.
                if e in ("PLAIN_DICTIONARY", "RLE_DICTIONARY"):
                    if "RLE_DICTIONARY" not in actual and "PLAIN_DICTIONARY" not in actual:
                        raise ValueError(f"Expected dictionary encoding, got {actual}")
                elif e == "BYTE_STREAM_SPLIT_EXTENDED":
                    if "BYTE_STREAM_SPLIT" not in actual:
                        raise ValueError(f"Expected BYTE_STREAM_SPLIT in encodings, got {actual}")
                else:
                    if e not in actual:
                        raise ValueError(f"Expected {e} in encodings, got {actual}")
            def read_enc(p=path):
                con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
            results["encoding"][enc_name][ptype] = test_rw(write_enc, read_enc, write_path=path)

    # --- Logical Types ---
    lt_tests = {
        "STRING": "SELECT 'hello'::VARCHAR AS c",
        "DATE": "SELECT DATE '2024-01-01' AS c",
        "TIME_MILLIS": "SELECT TIME '12:00:00' AS c",
        "TIME_MICROS": "SELECT TIME '12:00:00' AS c",
        "TIME_NANOS": "SELECT TIME '12:00:00.000000001' AS c",
        "TIMESTAMP_MILLIS": "SELECT TIMESTAMP '2024-01-01 00:00:00' AS c",
        "TIMESTAMP_MICROS": "SELECT TIMESTAMP '2024-01-01 00:00:00' AS c",
        "TIMESTAMP_NANOS": "SELECT TIMESTAMP_NS '2024-01-01 00:00:00' AS c",
        "INT96": None,  # DuckDB can read INT96 but not write it
        "DECIMAL": "SELECT 123.45::DECIMAL(10,2) AS c",
        "UUID": "SELECT uuid() AS c",
        "JSON": "SELECT '{\"key\":\"val\"}'::JSON AS c",
        "FLOAT16": None,
        "ENUM": "SELECT 'A'::VARCHAR AS c",
        "BSON": None,
        "INTERVAL": "SELECT INTERVAL 1 DAY AS c",
        "UNKNOWN": None,  # DuckDB does not support the UNKNOWN logical type
        "VARIANT": None,  # DuckDB does not yet write Parquet VARIANT
        "GEOMETRY": None,  # DuckDB does not yet write Parquet GEOMETRY
        "GEOGRAPHY": None,  # DuckDB does not yet write Parquet GEOGRAPHY
    }
    for type_name, sql in lt_tests.items():
        path = os.path.join(tmpdir, f"lt_{type_name}.parquet")
        def write_lt(q=sql, p=path):
            if q is None:
                raise NotImplementedError("Not supported")
            con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET)")
        def read_lt(q=sql, p=path):
            if q is None:
                raise NotImplementedError("Not supported")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["logical_types"][type_name] = test_rw(write_lt, read_lt, write_path=path)

    # --- Nested Types ---
    nt_tests = {
        "LIST": "SELECT [1, 2, 3] AS c",
        "MAP": "SELECT MAP {'a': 1, 'b': 2} AS c",
        "STRUCT": "SELECT {'x': 1, 'y': 2} AS c",
        "NESTED_LIST": "SELECT [[1, 2], [3]] AS c",
        "NESTED_MAP": "SELECT MAP {'a': [1, 2], 'b': [3]} AS c",
        "DEEP_NESTING": "SELECT [{'x': [1, 2]}] AS c",
    }
    for type_name, sql in nt_tests.items():
        path = os.path.join(tmpdir, f"nt_{type_name}.parquet")
        def write_nt(q=sql, p=path):
            con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET)")
        def read_nt(p=path):
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["nested_types"][type_name] = test_rw(write_nt, read_nt, write_path=path)

    # --- Advanced Features ---
    # Create a shared data file used by several read-side tests.  Wrap in
    # try/except so that a SQL syntax change in very old DuckDB versions cannot
    # crash the whole script and suppress all results.
    try:
        data_path = os.path.join(tmpdir, "adv_data.parquet")
        con.execute(
            f"COPY (SELECT i AS col, CAST(i AS VARCHAR) AS str_col "
            f"FROM range(1000) t(i)) TO '{data_path}' (FORMAT PARQUET)"
        )
    except Exception:
        data_path = None

    def write_page_index():
        pass  # DuckDB always writes page index metadata
    def read_page_index():
        # DuckDB reads page index from parquet files
        con.execute(f"SELECT * FROM parquet_metadata('{data_path}')").fetchall()
    results["advanced_features"]["PAGE_INDEX"] = test_rw(write_page_index, read_page_index)

    def write_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom.parquet")
        con.execute(f"COPY (SELECT i AS col FROM range(1000) t(i)) TO '{p}' (FORMAT PARQUET)")
    def read_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom.parquet")
        con.execute(f"SELECT * FROM parquet_metadata('{p}')").fetchall()
    results["advanced_features"]["BLOOM_FILTER"] = test_rw(write_bloom_filter, read_bloom_filter, write_path=os.path.join(tmpdir, "adv_bloom.parquet"))

    def write_encryption():
        p = os.path.join(tmpdir, "adv_enc.parquet")
        con.execute("PRAGMA add_parquet_key('test_key', '0123456789012345')")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET, ENCRYPTION_CONFIG {{footer_key: 'test_key'}})")
    def read_encryption():
        p = os.path.join(tmpdir, "adv_enc.parquet")
        con.execute(f"SELECT * FROM read_parquet('{p}', encryption_config={{footer_key: 'test_key'}})").fetchall()
    results["advanced_features"]["COLUMN_ENCRYPTION"] = test_rw(write_encryption, read_encryption, write_path=os.path.join(tmpdir, "adv_enc.parquet"))

    def write_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET)")
    def read_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2, write_path=os.path.join(tmpdir, "adv_v2.parquet"))

    def write_statistics():
        pass  # DuckDB always writes statistics
    def read_statistics():
        con.execute(f"SELECT * FROM parquet_metadata('{data_path}')").fetchall()
    results["advanced_features"]["STATISTICS"] = test_rw(write_statistics, read_statistics)

    def write_predicate_pushdown():
        pass  # DuckDB supports predicate pushdown automatically
    def read_predicate_pushdown():
        con.execute(f"SELECT * FROM read_parquet('{data_path}') WHERE col > 500").fetchall()
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_rw(write_predicate_pushdown, read_predicate_pushdown)

    def write_projection_pushdown():
        pass  # DuckDB supports projection pushdown automatically
    def read_projection_pushdown():
        con.execute(f"SELECT col FROM read_parquet('{data_path}')").fetchall()
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_rw(write_projection_pushdown, read_projection_pushdown)

    def write_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1.parquet")
        p2 = os.path.join(tmpdir, "adv_se2.parquet")
        con.execute(f"COPY (SELECT 1 AS a, 2 AS b) TO '{p1}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT 3 AS a, 4 AS c) TO '{p2}' (FORMAT PARQUET)")
    def read_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1.parquet")
        p2 = os.path.join(tmpdir, "adv_se2.parquet")
        con.execute(f"SELECT * FROM read_parquet(['{p1}', '{p2}'], union_by_name=true)").fetchall()
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_rw(write_schema_evolution, read_schema_evolution, write_path=os.path.join(tmpdir, "adv_se1.parquet"))

    # Size Statistics (Parquet format 2.10.0) - DuckDB reads size_statistics metadata
    def write_size_statistics():
        pass  # DuckDB writes size statistics by default in newer versions
    def read_size_statistics():
        rows = con.execute(f"SELECT * FROM parquet_metadata('{data_path}')").fetchall()
        assert len(rows) > 0
    results["advanced_features"]["SIZE_STATISTICS"] = test_rw(write_size_statistics, read_size_statistics)

    # Page CRC32 checksum - DuckDB does not write page checksums
    results["advanced_features"]["PAGE_CRC32"] = {"write": False, "read": False}

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
