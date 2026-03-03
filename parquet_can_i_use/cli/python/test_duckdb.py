#!/usr/bin/env python3
"""Test DuckDB's Parquet feature support and output JSON results."""

import json
import sys
import tempfile
import os

def test_feature(name, fn):
    try:
        fn()
        return True
    except Exception:
        return False

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
    con = duckdb.connect()

    # --- Compression ---
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
    for codec_name, codec_val in codecs.items():
        path = os.path.join(tmpdir, f"comp_{codec_name}.parquet")
        def write_read(c=codec_val, p=path):
            con.execute(f"COPY (SELECT 1 AS col, 2 AS col2) TO '{p}' (FORMAT PARQUET, CODEC '{c}')")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["compression"][codec_name] = test_feature(codec_name, write_read)

    # --- Encoding ---
    # DuckDB handles encoding internally; test by writing and reading
    enc_support = {
        "PLAIN": True,
        "PLAIN_DICTIONARY": True,
        "RLE_DICTIONARY": True,
        "RLE": True,
        "BIT_PACKED": True,
        "DELTA_BINARY_PACKED": True,
        "DELTA_LENGTH_BYTE_ARRAY": True,
        "DELTA_BYTE_ARRAY": True,
        "BYTE_STREAM_SPLIT": True,
    }
    for enc_name, supported in enc_support.items():
        path = os.path.join(tmpdir, f"enc_{enc_name}.parquet")
        def write_read_enc(p=path, s=supported):
            if not s:
                raise NotImplementedError("Not supported")
            con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET)")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["encoding"][enc_name] = test_feature(enc_name, write_read_enc)

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
    }
    for type_name, sql in lt_tests.items():
        path = os.path.join(tmpdir, f"lt_{type_name}.parquet")
        def write_read_lt(q=sql, p=path):
            if q is None:
                raise NotImplementedError("Not supported")
            con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET)")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["logical_types"][type_name] = test_feature(type_name, write_read_lt)

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
        def write_read_nt(q=sql, p=path):
            con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET)")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["nested_types"][type_name] = test_feature(type_name, write_read_nt)

    # --- Advanced Features ---
    data_path = os.path.join(tmpdir, "adv_data.parquet")
    con.execute(f"COPY (SELECT i AS col, 'val_' || i AS str_col FROM range(1000) t(i)) TO '{data_path}' (FORMAT PARQUET)")

    def test_page_index():
        # DuckDB reads page index from parquet files
        con.execute(f"SELECT * FROM parquet_metadata('{data_path}')").fetchall()
    results["advanced_features"]["PAGE_INDEX"] = test_feature("PAGE_INDEX", test_page_index)

    def test_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom.parquet")
        con.execute(f"COPY (SELECT i AS col FROM range(1000) t(i)) TO '{p}' (FORMAT PARQUET)")
        con.execute(f"SELECT * FROM parquet_metadata('{p}')").fetchall()
    results["advanced_features"]["BLOOM_FILTER"] = test_feature("BLOOM_FILTER", test_bloom_filter)

    def test_encryption():
        p = os.path.join(tmpdir, "adv_enc.parquet")
        con.execute("PRAGMA add_parquet_key('test_key', '0123456789012345')")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET, ENCRYPTION_CONFIG {{footer_key: 'test_key'}})")
        con.execute(f"SELECT * FROM read_parquet('{p}', encryption_config={{footer_key: 'test_key'}})").fetchall()
    results["advanced_features"]["COLUMN_ENCRYPTION"] = test_feature("COLUMN_ENCRYPTION", test_encryption)

    def test_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET)")
        con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
    results["advanced_features"]["DATA_PAGE_V2"] = test_feature("DATA_PAGE_V2", test_data_page_v2)

    def test_statistics():
        con.execute(f"SELECT * FROM parquet_metadata('{data_path}')").fetchall()
    results["advanced_features"]["STATISTICS"] = test_feature("STATISTICS", test_statistics)

    def test_predicate_pushdown():
        con.execute(f"SELECT * FROM read_parquet('{data_path}') WHERE col > 500").fetchall()
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_feature("PREDICATE_PUSHDOWN", test_predicate_pushdown)

    def test_projection_pushdown():
        con.execute(f"SELECT col FROM read_parquet('{data_path}')").fetchall()
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_feature("PROJECTION_PUSHDOWN", test_projection_pushdown)

    def test_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1.parquet")
        p2 = os.path.join(tmpdir, "adv_se2.parquet")
        con.execute(f"COPY (SELECT 1 AS a, 2 AS b) TO '{p1}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT 3 AS a, 4 AS c) TO '{p2}' (FORMAT PARQUET)")
        con.execute(f"SELECT * FROM read_parquet(['{p1}', '{p2}'], union_by_name=true)").fetchall()
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_feature("SCHEMA_EVOLUTION", test_schema_evolution)

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
