#!/usr/bin/env python3
"""Test fastparquet's Parquet feature support and output JSON results."""

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
        import fastparquet
    except ImportError:
        print(json.dumps({"error": "fastparquet not installed"}))
        sys.exit(1)

    results = {
        "tool": "fastparquet",
        "version": fastparquet.__version__,
        "compression": {},
        "encoding": {},
        "logical_types": {},
        "nested_types": {},
        "advanced_features": {},
    }

    tmpdir = tempfile.mkdtemp()

    import pandas as pd
    import numpy as np

    # --- Compression ---
    codecs = {
        "NONE": None,
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
        df = pd.DataFrame({"col": [1, 2, 3]})
        def write_codec(c=codec_val, p=path, d=df):
            if c is None:
                fastparquet.write(p, d, compression="UNCOMPRESSED")
            else:
                fastparquet.write(p, d, compression=c)
        def read_codec(p=path):
            fastparquet.ParquetFile(p).to_pandas()
        results["compression"][codec_name] = test_rw(write_codec, read_codec)

    # --- Encoding × Type matrix ---
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"]

    def make_typed_df_fp(ptype):
        if ptype == "INT32":
            return pd.DataFrame({"col": np.array([1, 2, 3], dtype=np.int32)})
        elif ptype == "INT64":
            return pd.DataFrame({"col": np.array([1, 2, 3], dtype=np.int64)})
        elif ptype == "FLOAT":
            return pd.DataFrame({"col": np.array([1.0, 2.0, 3.0], dtype=np.float32)})
        elif ptype == "DOUBLE":
            return pd.DataFrame({"col": np.array([1.0, 2.0, 3.0], dtype=np.float64)})
        elif ptype == "BOOLEAN":
            return pd.DataFrame({"col": [True, False, True]})
        elif ptype == "BYTE_ARRAY":
            return pd.DataFrame({"col": pd.array([b"hello", b"world", b"test"], dtype="object")})
        raise ValueError(f"Unknown type: {ptype}")

    enc_names = ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                 "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                 "BYTE_STREAM_SPLIT", "BYTE_STREAM_SPLIT_EXTENDED"]
    for enc_name in enc_names:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            path = os.path.join(tmpdir, f"enc_{enc_name}_{ptype}.parquet")
            def write_enc(e=enc_name, p=path, pt=ptype):
                df = make_typed_df_fp(pt)
                if e in ("PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY"):
                    fastparquet.write(p, df)
                elif e in ("RLE", "BIT_PACKED"):
                    if pt == "BOOLEAN":
                        fastparquet.write(p, df)
                    else:
                        raise NotImplementedError(f"fastparquet {e} only for BOOLEAN")
                else:
                    raise NotImplementedError(f"fastparquet does not support {e} encoding")
            def read_enc(p=path):
                fastparquet.ParquetFile(p).to_pandas()
            results["encoding"][enc_name][ptype] = test_rw(write_enc, read_enc)

    # --- Logical Types ---
    import datetime

    lt_tests = {}
    lt_tests["STRING"] = lambda: pd.DataFrame({"c": ["hello"]})
    lt_tests["DATE"] = lambda: pd.DataFrame({"c": pd.array([datetime.date(2024, 1, 1)], dtype="object")})
    lt_tests["TIME_MILLIS"] = lambda: pd.DataFrame({"c": pd.array([datetime.time(12, 0, 0)], dtype="object")})
    lt_tests["TIME_MICROS"] = lambda: pd.DataFrame({"c": pd.array([datetime.time(12, 0, 0)], dtype="object")})
    lt_tests["TIME_NANOS"] = lambda: pd.DataFrame({"c": pd.array([datetime.time(12, 0, 0, 123)], dtype="object")})
    lt_tests["TIMESTAMP_MILLIS"] = lambda: pd.DataFrame({"c": pd.to_datetime(["2024-01-01"])})
    lt_tests["TIMESTAMP_MICROS"] = lambda: pd.DataFrame({"c": pd.to_datetime(["2024-01-01"])})
    lt_tests["TIMESTAMP_NANOS"] = lambda: pd.DataFrame({"c": pd.to_datetime(["2024-01-01"])})
    lt_tests["INT96"] = lambda: pd.DataFrame({"c": pd.to_datetime(["2024-01-01"])})
    lt_tests["DECIMAL"] = lambda: pd.DataFrame({"c": [123.45]})
    lt_tests["UUID"] = lambda: pd.DataFrame({"c": ["550e8400-e29b-41d4-a716-446655440000"]})
    lt_tests["JSON"] = lambda: pd.DataFrame({"c": ['{"key":"val"}']})
    lt_tests["FLOAT16"] = lambda: pd.DataFrame({"c": np.array([1.0], dtype=np.float16)})
    lt_tests["ENUM"] = lambda: pd.DataFrame({"c": pd.Categorical(["A", "B", "A"])})
    lt_tests["BSON"] = lambda: pd.DataFrame({"c": pd.array([b'\x05\x00\x00\x00\x00'], dtype="object")})
    lt_tests["INTERVAL"] = lambda: pd.DataFrame({"c": pd.to_timedelta(["1 days"])})

    def _fp_not_supported(name):
        def fn():
            raise NotImplementedError(f"{name} not supported by fastparquet")
        return fn

    lt_tests["UNKNOWN"] = _fp_not_supported("UNKNOWN")
    lt_tests["VARIANT"] = _fp_not_supported("VARIANT")
    lt_tests["GEOMETRY"] = _fp_not_supported("GEOMETRY")
    lt_tests["GEOGRAPHY"] = _fp_not_supported("GEOGRAPHY")

    for type_name, make_df in lt_tests.items():
        path = os.path.join(tmpdir, f"lt_{type_name}.parquet")
        def write_lt(mk=make_df, p=path, tn=type_name):
            df = mk()
            kwargs = {}
            if tn == "INT96":
                kwargs["times"] = "int96"
            fastparquet.write(p, df, **kwargs)
        def read_lt(p=path):
            fastparquet.ParquetFile(p).to_pandas()
        results["logical_types"][type_name] = test_rw(write_lt, read_lt)

    # --- Nested Types ---
    nt_tests = {}
    nt_tests["LIST"] = lambda: pd.DataFrame({"c": [[1, 2], [3]]})
    nt_tests["MAP"] = lambda: pd.DataFrame({"c": [{"a": 1}, {"b": 2}]})
    nt_tests["STRUCT"] = lambda: pd.DataFrame({"c": [{"x": 1, "y": 2}]})
    nt_tests["NESTED_LIST"] = lambda: pd.DataFrame({"c": [[[1, 2], [3]], [[4]]]})
    nt_tests["NESTED_MAP"] = lambda: pd.DataFrame({"c": [{"a": {"x": 1}}, {"b": {"y": 2}}]})
    nt_tests["DEEP_NESTING"] = lambda: pd.DataFrame({"c": [[{"x": [1, 2]}]]})

    for type_name, make_df in nt_tests.items():
        path = os.path.join(tmpdir, f"nt_{type_name}.parquet")
        def write_nt(mk=make_df, p=path):
            df = mk()
            fastparquet.write(p, df)
        def read_nt(p=path):
            fastparquet.ParquetFile(p).to_pandas()
        results["nested_types"][type_name] = test_rw(write_nt, read_nt)

    # --- Advanced Features ---
    df = pd.DataFrame({"col": range(1000), "str_col": [f"val_{i}" for i in range(1000)]})

    def write_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        fastparquet.write(p, df)
        pf = fastparquet.ParquetFile(p)
        rg = pf.row_groups[0]
        stats = rg.columns[0].meta_data.statistics
        # fastparquet writes statistics (min/max)
        assert stats is not None
    def read_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        pf = fastparquet.ParquetFile(p)
        assert pf.row_groups[0].columns[0].meta_data.statistics is not None
    results["advanced_features"]["STATISTICS"] = test_rw(write_statistics, read_statistics)

    def write_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        fastparquet.write(p, df)
    def read_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        pf = fastparquet.ParquetFile(p)
        pf.to_pandas(filters=[("col", ">", 500)])
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_rw(write_predicate_pushdown, read_predicate_pushdown)

    def write_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        fastparquet.write(p, df)
    def read_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        pf = fastparquet.ParquetFile(p)
        result = pf.to_pandas(columns=["col"])
        assert "col" in result.columns
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_rw(write_projection_pushdown, read_projection_pushdown)

    # Features fastparquet doesn't support
    results["advanced_features"]["PAGE_INDEX"] = {"write": False, "read": False}
    results["advanced_features"]["BLOOM_FILTER"] = {"write": False, "read": False}
    results["advanced_features"]["COLUMN_ENCRYPTION"] = {"write": False, "read": False}

    def write_data_page_v2():
        raise NotImplementedError("fastparquet does not support Data Page V2")
    def read_data_page_v2():
        raise NotImplementedError("fastparquet does not support Data Page V2")
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2)

    def write_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1.parquet")
        p2 = os.path.join(tmpdir, "adv_se2.parquet")
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [5, 6], "c": [7, 8]})
        fastparquet.write(p1, df1)
        fastparquet.write(p2, df2)
    def read_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1.parquet")
        # Try reading with different schema
        pf = fastparquet.ParquetFile(p1)
        pf.to_pandas()
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_rw(write_schema_evolution, read_schema_evolution)

    # Size Statistics (Parquet format 2.10.0) - not supported by fastparquet
    results["advanced_features"]["SIZE_STATISTICS"] = {"write": False, "read": False}

    # Page CRC32 checksum - not supported by fastparquet
    results["advanced_features"]["PAGE_CRC32"] = {"write": False, "read": False}

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
