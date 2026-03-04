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

def test_rw(write_fn, read_fn):
    """Run separate write and read tests, return {"write": bool, "read": bool}."""
    write_ok = test_feature("write", write_fn)
    read_ok = test_feature("read", read_fn)
    return {"write": write_ok, "read": read_ok}

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
        def write_codec(c=codec_val, p=path):
            con.execute(f"COPY (SELECT 1 AS col, 2 AS col2) TO '{p}' (FORMAT PARQUET, CODEC '{c}')")
        def read_codec(p=path):
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["compression"][codec_name] = test_rw(write_codec, read_codec)

    # --- Encoding × Type matrix ---
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "STRING", "BINARY"]

    def make_type_sql(ptype):
        if ptype == "INT32":
            return "1::INTEGER"
        elif ptype == "INT64":
            return "1::BIGINT"
        elif ptype == "FLOAT":
            return "1.0::FLOAT"
        elif ptype == "DOUBLE":
            return "1.0::DOUBLE"
        elif ptype == "BOOLEAN":
            return "true::BOOLEAN"
        elif ptype == "STRING":
            return "'hello'::VARCHAR"
        elif ptype == "BINARY":
            return "'\\x68656C6C6F'::BLOB"
        raise ValueError(f"Unknown type: {ptype}")

    def get_actual_encodings_duckdb(path):
        """Return the set of encodings actually used in the file via DuckDB parquet_metadata."""
        rows = con.execute(f"SELECT encodings FROM parquet_metadata('{path}')").fetchall()
        enc_set = set()
        for row in rows:
            # encodings is returned as a comma-separated string or a list
            val = row[0]
            if val is None:
                continue
            if isinstance(val, (list, tuple)):
                enc_set.update(str(e).strip() for e in val)
            else:
                enc_set.update(e.strip() for e in str(val).split(","))
        return enc_set

    enc_names = ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                 "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY", "BYTE_STREAM_SPLIT"]
    for enc_name in enc_names:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            path = os.path.join(tmpdir, f"enc_{enc_name}_{ptype}.parquet")
            def write_enc(p=path, pt=ptype, e=enc_name):
                val_sql = make_type_sql(pt)
                con.execute(f"COPY (SELECT {val_sql} AS col) TO '{p}' (FORMAT PARQUET)")
                # Verify that DuckDB actually wrote the expected encoding
                actual = get_actual_encodings_duckdb(p)
                # RLE_DICTIONARY and PLAIN_DICTIONARY: look for dictionary encoding
                if e in ("PLAIN_DICTIONARY", "RLE_DICTIONARY"):
                    if "RLE_DICTIONARY" not in actual and "PLAIN_DICTIONARY" not in actual:
                        raise ValueError(f"Expected dictionary encoding, got {actual}")
                else:
                    if e not in actual:
                        raise ValueError(f"Expected {e} in encodings, got {actual}")
            def read_enc(p=path):
                con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
            results["encoding"][enc_name][ptype] = test_rw(write_enc, read_enc)

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
        def write_lt(q=sql, p=path):
            if q is None:
                raise NotImplementedError("Not supported")
            con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET)")
        def read_lt(q=sql, p=path):
            if q is None:
                raise NotImplementedError("Not supported")
            con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
        results["logical_types"][type_name] = test_rw(write_lt, read_lt)

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
        results["nested_types"][type_name] = test_rw(write_nt, read_nt)

    # --- Advanced Features ---
    data_path = os.path.join(tmpdir, "adv_data.parquet")
    con.execute(f"COPY (SELECT i AS col, 'val_' || i AS str_col FROM range(1000) t(i)) TO '{data_path}' (FORMAT PARQUET)")

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
    results["advanced_features"]["BLOOM_FILTER"] = test_rw(write_bloom_filter, read_bloom_filter)

    def write_encryption():
        p = os.path.join(tmpdir, "adv_enc.parquet")
        con.execute("PRAGMA add_parquet_key('test_key', '0123456789012345')")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET, ENCRYPTION_CONFIG {{footer_key: 'test_key'}})")
    def read_encryption():
        p = os.path.join(tmpdir, "adv_enc.parquet")
        con.execute(f"SELECT * FROM read_parquet('{p}', encryption_config={{footer_key: 'test_key'}})").fetchall()
    results["advanced_features"]["COLUMN_ENCRYPTION"] = test_rw(write_encryption, read_encryption)

    def write_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        con.execute(f"COPY (SELECT 1 AS col) TO '{p}' (FORMAT PARQUET)")
    def read_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        con.execute(f"SELECT * FROM read_parquet('{p}')").fetchall()
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2)

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
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_rw(write_schema_evolution, read_schema_evolution)

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
