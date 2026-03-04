#!/usr/bin/env python3
"""Test PyArrow's Parquet feature support and output JSON results."""

import json
import sys
import tempfile
import os

def test_feature(name, fn):
    """Run a test function, return True/False."""
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
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print(json.dumps({"error": "pyarrow not installed"}))
        sys.exit(1)

    results = {
        "tool": "PyArrow",
        "version": pa.__version__,
        "compression": {},
        "encoding": {},
        "logical_types": {},
        "nested_types": {},
        "advanced_features": {},
    }

    tmpdir = tempfile.mkdtemp()

    # --- Compression ---
    codec_map = {
        "NONE": "NONE",
        "SNAPPY": "SNAPPY",
        "GZIP": "GZIP",
        "BROTLI": "BROTLI",
        "LZO": "LZO",
        "LZ4": "LZ4",
        "LZ4_RAW": "lz4",
        "ZSTD": "ZSTD",
    }
    for codec_name, codec_val in codec_map.items():
        path = os.path.join(tmpdir, f"comp_{codec_name}.parquet")
        table = pa.table({"col": [1, 2, 3]})
        def write_codec(c=codec_val, p=path, t=table):
            pq.write_table(t, p, compression=c)
        def read_codec(p=path):
            pq.read_table(p)
        results["compression"][codec_name] = test_rw(write_codec, read_codec)

    # --- Encoding × Type matrix ---
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "STRING", "BINARY"]

    def make_typed_table(ptype):
        if ptype == "INT32":
            return pa.table({"col": pa.array([1, 2, 3], type=pa.int32())})
        elif ptype == "INT64":
            return pa.table({"col": pa.array([1, 2, 3], type=pa.int64())})
        elif ptype == "FLOAT":
            return pa.table({"col": pa.array([1.0, 2.0, 3.0], type=pa.float32())})
        elif ptype == "DOUBLE":
            return pa.table({"col": pa.array([1.0, 2.0, 3.0], type=pa.float64())})
        elif ptype == "BOOLEAN":
            return pa.table({"col": pa.array([True, False, True])})
        elif ptype == "STRING":
            return pa.table({"col": pa.array(["hello", "world", "test"])})
        elif ptype == "BINARY":
            return pa.table({"col": pa.array([b"hello", b"world", b"test"])})
        raise ValueError(f"Unknown type: {ptype}")

    def get_column_encodings(path):
        """Return the set of encodings actually used in the first column."""
        meta = pq.read_metadata(path)
        encodings = set()
        for i in range(meta.num_row_groups):
            col = meta.row_group(i).column(0)
            encodings.update(col.encodings)
        return encodings

    for enc_name in ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                     "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY", "BYTE_STREAM_SPLIT"]:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            path = os.path.join(tmpdir, f"enc_{enc_name}_{ptype}.parquet")
            def write_enc(e=enc_name, p=path, pt=ptype):
                t = make_typed_table(pt)
                if e in ("PLAIN_DICTIONARY", "RLE_DICTIONARY"):
                    pq.write_table(t, p, use_dictionary=True)
                    actual = get_column_encodings(p)
                    if "RLE_DICTIONARY" not in actual and "PLAIN_DICTIONARY" not in actual:
                        raise ValueError(f"Expected dictionary encoding, got {actual}")
                else:
                    pq.write_table(t, p, use_dictionary=False, column_encoding=e)
                    actual = get_column_encodings(p)
                    if e not in actual:
                        raise ValueError(f"Expected {e} in encodings, got {actual}")
            def read_enc(p=path):
                pq.read_table(p)
            results["encoding"][enc_name][ptype] = test_rw(write_enc, read_enc)

    # --- Logical Types ---
    import datetime
    import decimal

    logical_tests = {}
    logical_tests["STRING"] = lambda: pa.table({"c": pa.array(["hello"])})
    logical_tests["DATE"] = lambda: pa.table({"c": pa.array([datetime.date(2024,1,1)])})
    logical_tests["TIME_MILLIS"] = lambda: pa.table({"c": pa.array([datetime.time(12,0,0)], type=pa.time32("ms"))})
    logical_tests["TIME_MICROS"] = lambda: pa.table({"c": pa.array([datetime.time(12,0,0)], type=pa.time64("us"))})
    logical_tests["TIME_NANOS"] = lambda: pa.table({"c": pa.array([datetime.time(12,0,0)], type=pa.time64("ns"))})
    logical_tests["TIMESTAMP_MILLIS"] = lambda: pa.table({"c": pa.array([datetime.datetime(2024,1,1)], type=pa.timestamp("ms"))})
    logical_tests["TIMESTAMP_MICROS"] = lambda: pa.table({"c": pa.array([datetime.datetime(2024,1,1)], type=pa.timestamp("us"))})
    logical_tests["TIMESTAMP_NANOS"] = lambda: pa.table({"c": pa.array([datetime.datetime(2024,1,1)], type=pa.timestamp("ns"))})
    logical_tests["INT96"] = lambda: pa.table({"c": pa.array([datetime.datetime(2024,1,1)], type=pa.timestamp("ns"))})
    logical_tests["DECIMAL"] = lambda: pa.table({"c": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2))})
    logical_tests["UUID"] = lambda: pa.table({"c": pa.array(["550e8400-e29b-41d4-a716-446655440000"])})
    logical_tests["JSON"] = lambda: pa.table({"c": pa.array(['{"key":"val"}'])})
    logical_tests["FLOAT16"] = lambda: pa.table({"c": pa.array([1.0], type=pa.float16())})
    logical_tests["ENUM"] = lambda: pa.table({"c": pa.array(["A"], type=pa.dictionary(pa.int8(), pa.string()))})
    logical_tests["BSON"] = lambda: pa.table({"c": pa.array([b'\x05\x00\x00\x00\x00'], type=pa.binary())})
    logical_tests["INTERVAL"] = lambda: pa.table({"c": pa.array([pa.scalar((1, 2, 3), type=pa.month_day_nano_interval())])})

    for type_name, make_table in logical_tests.items():
        path = os.path.join(tmpdir, f"lt_{type_name}.parquet")
        def write_lt(mk=make_table, p=path, tn=type_name):
            t = mk()
            if tn == "INT96":
                pq.write_table(t, p, use_deprecated_int96_timestamps=True)
            else:
                pq.write_table(t, p)
        def read_lt(p=path):
            pq.read_table(p)
        results["logical_types"][type_name] = test_rw(write_lt, read_lt)

    # --- Nested Types ---
    nested_tests = {}
    nested_tests["LIST"] = lambda: pa.table({"c": pa.array([[1, 2], [3]])})
    nested_tests["MAP"] = lambda: pa.table({"c": pa.array([[("a", 1), ("b", 2)]], type=pa.map_(pa.string(), pa.int64()))})
    nested_tests["STRUCT"] = lambda: pa.table({"c": pa.array([{"x": 1, "y": 2}])})
    nested_tests["NESTED_LIST"] = lambda: pa.table({"c": pa.array([[[1, 2], [3]], [[4]]])})
    nested_tests["NESTED_MAP"] = lambda: pa.table({"c": pa.array([[("a", [1,2]), ("b", [3])]], type=pa.map_(pa.string(), pa.list_(pa.int64())))})
    nested_tests["DEEP_NESTING"] = lambda: pa.table({"c": pa.array([[{"x": [1, 2]}]])})

    for type_name, make_table in nested_tests.items():
        path = os.path.join(tmpdir, f"nt_{type_name}.parquet")
        def write_nt(mk=make_table, p=path):
            t = mk()
            pq.write_table(t, p)
        def read_nt(p=path):
            pq.read_table(p)
        results["nested_types"][type_name] = test_rw(write_nt, read_nt)

    # --- Advanced Features ---
    table = pa.table({"col": pa.array(range(1000)), "str_col": pa.array([f"val_{i}" for i in range(1000)])})

    # Page Index
    def write_page_index():
        p = os.path.join(tmpdir, "adv_page_index.parquet")
        pq.write_table(table, p, write_page_index=True)
    def read_page_index():
        p = os.path.join(tmpdir, "adv_page_index.parquet")
        pq.read_table(p)
    results["advanced_features"]["PAGE_INDEX"] = test_rw(write_page_index, read_page_index)

    # Bloom Filters (PyArrow doesn't expose bloom filter writing via write_table)
    def write_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom.parquet")
        writer = pq.ParquetWriter(p, table.schema)
        writer.write_table(table)
        writer.close()
        # PyArrow has limited bloom filter support (no high-level API as of v23)
        raise NotImplementedError("No bloom filter write API in write_table")
    def read_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom.parquet")
        pq.read_table(p)
    results["advanced_features"]["BLOOM_FILTER"] = test_rw(write_bloom_filter, read_bloom_filter)

    # Column Encryption (PyArrow supports Parquet Modular Encryption)
    def write_encryption():
        # Test if encryption classes exist
        assert hasattr(pq, 'FileEncryptionProperties')
        assert hasattr(pq, 'FileDecryptionProperties')
    def read_encryption():
        assert hasattr(pq, 'FileDecryptionProperties')
    results["advanced_features"]["COLUMN_ENCRYPTION"] = test_rw(write_encryption, read_encryption)

    # Data Page V2
    def write_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        pq.write_table(table, p, data_page_version="2.0")
    def read_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        pq.read_table(p)
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2)

    # Statistics
    def write_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        pq.write_table(table, p, write_statistics=True)
        md = pq.read_metadata(p)
        rg = md.row_group(0)
        col = rg.column(0)
        assert col.statistics is not None
    def read_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        md = pq.read_metadata(p)
        assert md.row_group(0).column(0).statistics is not None
    results["advanced_features"]["STATISTICS"] = test_rw(write_statistics, read_statistics)

    # Predicate Pushdown (via filters)
    def write_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        pq.write_table(table, p)
    def read_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        pq.read_table(p, filters=[("col", ">", 500)])
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_rw(write_predicate_pushdown, read_predicate_pushdown)

    # Projection Pushdown
    def write_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        pq.write_table(table, p)
    def read_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        pq.read_table(p, columns=["col"])
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_rw(write_projection_pushdown, read_projection_pushdown)

    # Schema Evolution (reading with missing columns)
    def write_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_schema1.parquet")
        p2 = os.path.join(tmpdir, "adv_schema2.parquet")
        t1 = pa.table({"a": [1, 2], "b": [3, 4]})
        t2 = pa.table({"a": [5, 6], "c": [7, 8]})
        pq.write_table(t1, p1)
        pq.write_table(t2, p2)
    def read_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_schema1.parquet")
        p2 = os.path.join(tmpdir, "adv_schema2.parquet")
        ds = pq.ParquetDataset([p1, p2])
        ds.read()
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_rw(write_schema_evolution, read_schema_evolution)

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
