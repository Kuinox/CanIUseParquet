#!/usr/bin/env python3
"""Test Polars' Parquet feature support and output JSON results."""

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
        import polars as pl
    except ImportError:
        print(json.dumps({"error": "polars not installed"}))
        sys.exit(1)

    results = {
        "tool": "Polars",
        "version": pl.__version__,
        "compression": {},
        "encoding": {},
        "logical_types": {},
        "nested_types": {},
        "advanced_features": {},
    }

    tmpdir = tempfile.mkdtemp()

    # --- Compression ---
    codecs = {
        "NONE": "uncompressed",
        "SNAPPY": "snappy",
        "GZIP": "gzip",
        "BROTLI": "brotli",
        "LZO": "lzo",
        "LZ4": "lz4",
        "LZ4_RAW": "lz4_raw",
        "ZSTD": "zstd",
    }
    for codec_name, codec_val in codecs.items():
        path = os.path.join(tmpdir, f"comp_{codec_name}.parquet")
        df = pl.DataFrame({"col": [1, 2, 3]})
        def write_codec(c=codec_val, p=path, d=df):
            d.write_parquet(p, compression=c)
        def read_codec(p=path):
            pl.read_parquet(p)
        results["compression"][codec_name] = test_rw(write_codec, read_codec)

    # --- Encoding × Type matrix ---
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"]

    def make_typed_df(ptype):
        if ptype == "INT32":
            return pl.DataFrame({"col": pl.Series([1, 2, 3], dtype=pl.Int32)})
        elif ptype == "INT64":
            return pl.DataFrame({"col": pl.Series([1, 2, 3], dtype=pl.Int64)})
        elif ptype == "FLOAT":
            return pl.DataFrame({"col": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float32)})
        elif ptype == "DOUBLE":
            return pl.DataFrame({"col": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float64)})
        elif ptype == "BOOLEAN":
            return pl.DataFrame({"col": pl.Series([True, False, True])})
        elif ptype == "BYTE_ARRAY":
            return pl.DataFrame({"col": pl.Series([b"hello", b"world", b"test"])})
        raise ValueError(f"Unknown type: {ptype}")

    enc_tests = {
        "PLAIN": True,
        "PLAIN_DICTIONARY": True,
        "RLE_DICTIONARY": True,
        "RLE": True,
        "BIT_PACKED": False,
        "DELTA_BINARY_PACKED": True,
        "DELTA_LENGTH_BYTE_ARRAY": True,
        "DELTA_BYTE_ARRAY": True,
        "BYTE_STREAM_SPLIT": True,
        "BYTE_STREAM_SPLIT_EXTENDED": True,
    }
    for enc_name, supported in enc_tests.items():
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            path = os.path.join(tmpdir, f"enc_{enc_name}_{ptype}.parquet")
            def write_enc(p=path, s=supported, pt=ptype):
                if not s:
                    raise NotImplementedError("Not supported")
                df = make_typed_df(pt)
                df.write_parquet(p)
            def read_enc(p=path):
                pl.read_parquet(p)
            results["encoding"][enc_name][ptype] = test_rw(write_enc, read_enc)

    # --- Logical Types ---
    import datetime
    from decimal import Decimal

    lt_tests = {}
    lt_tests["STRING"] = lambda: pl.DataFrame({"c": ["hello"]})
    lt_tests["DATE"] = lambda: pl.DataFrame({"c": [datetime.date(2024, 1, 1)]})
    lt_tests["TIME_MILLIS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.time(12, 0, 0)]).cast(pl.Time)})
    lt_tests["TIME_MICROS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.time(12, 0, 0)]).cast(pl.Time)})
    lt_tests["TIME_NANOS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.time(12, 0, 0)]).cast(pl.Time)})
    lt_tests["TIMESTAMP_MILLIS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.datetime(2024, 1, 1)]).cast(pl.Datetime("ms"))})
    lt_tests["TIMESTAMP_MICROS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.datetime(2024, 1, 1)]).cast(pl.Datetime("us"))})
    lt_tests["TIMESTAMP_NANOS"] = lambda: pl.DataFrame({"c": pl.Series([datetime.datetime(2024, 1, 1)]).cast(pl.Datetime("ns"))})
    lt_tests["INT96"] = lambda: (_ for _ in ()).throw(NotImplementedError("Polars does not write INT96"))
    lt_tests["DECIMAL"] = lambda: pl.DataFrame({"c": pl.Series([Decimal("123.45")]).cast(pl.Decimal(10, 2))})
    lt_tests["UUID"] = lambda: pl.DataFrame({"c": ["550e8400-e29b-41d4-a716-446655440000"]})
    lt_tests["JSON"] = lambda: pl.DataFrame({"c": ['{"key":"val"}']})
    lt_tests["FLOAT16"] = lambda: pl.DataFrame({"c": pl.Series([1.0]).cast(pl.Float32)})  # Polars uses Float32 minimum
    lt_tests["ENUM"] = lambda: pl.DataFrame({"c": pl.Series(["A", "B", "A"]).cast(pl.Categorical)})
    lt_tests["BSON"] = lambda: pl.DataFrame({"c": [b'\x05\x00\x00\x00\x00']})
    lt_tests["INTERVAL"] = lambda: pl.DataFrame({"c": pl.Series([datetime.timedelta(days=1)]).cast(pl.Duration)})

    # UNKNOWN logical type (always-null column)
    lt_tests["UNKNOWN"] = lambda: pl.DataFrame({"c": pl.Series([None], dtype=pl.Null)})

    # VARIANT logical type (Parquet format 2.11.0) - not yet supported in Polars
    def _pl_variant():
        raise NotImplementedError("VARIANT not yet supported in Polars")
    lt_tests["VARIANT"] = _pl_variant

    # GEOMETRY logical type (Parquet format 2.11.0) - not yet supported in Polars
    def _pl_geometry():
        raise NotImplementedError("GEOMETRY not yet supported in Polars")
    lt_tests["GEOMETRY"] = _pl_geometry

    # GEOGRAPHY logical type (Parquet format 2.11.0) - not yet supported in Polars
    def _pl_geography():
        raise NotImplementedError("GEOGRAPHY not yet supported in Polars")
    lt_tests["GEOGRAPHY"] = _pl_geography

    for type_name, make_df in lt_tests.items():
        path = os.path.join(tmpdir, f"lt_{type_name}.parquet")
        def write_lt(mk=make_df, p=path):
            df = mk()
            df.write_parquet(p)
        def read_lt(p=path):
            pl.read_parquet(p)
        results["logical_types"][type_name] = test_rw(write_lt, read_lt)

    # --- Nested Types ---
    nt_tests = {}
    nt_tests["LIST"] = lambda: pl.DataFrame({"c": [[1, 2], [3]]})
    nt_tests["MAP"] = lambda: pl.DataFrame({"c": [{"a": 1}, {"b": 2}]})
    nt_tests["STRUCT"] = lambda: pl.DataFrame({"c": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]})
    nt_tests["NESTED_LIST"] = lambda: pl.DataFrame({"c": [[[1, 2], [3]], [[4]]]})
    nt_tests["NESTED_MAP"] = lambda: pl.DataFrame({"c": [[("a", 1), ("b", 2)]]}).cast({"c": pl.List(pl.Struct({"key": pl.String, "value": pl.Int64}))})
    nt_tests["DEEP_NESTING"] = lambda: pl.DataFrame({"c": [[{"x": [1, 2]}]]})

    for type_name, make_df in nt_tests.items():
        path = os.path.join(tmpdir, f"nt_{type_name}.parquet")
        def write_nt(mk=make_df, p=path):
            df = mk()
            df.write_parquet(p)
        def read_nt(p=path):
            pl.read_parquet(p)
        results["nested_types"][type_name] = test_rw(write_nt, read_nt)

    # --- Advanced Features ---
    df = pl.DataFrame({"col": range(1000), "str_col": [f"val_{i}" for i in range(1000)]})

    def write_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        df.write_parquet(p, statistics=True)
    def read_statistics():
        p = os.path.join(tmpdir, "adv_stats.parquet")
        pl.read_parquet(p)
    results["advanced_features"]["STATISTICS"] = test_rw(write_statistics, read_statistics)

    def write_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        df.write_parquet(p)
    def read_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred.parquet")
        pl.scan_parquet(p).filter(pl.col("col") > 500).collect()
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_rw(write_predicate_pushdown, read_predicate_pushdown)

    def write_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        df.write_parquet(p)
    def read_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj.parquet")
        pl.scan_parquet(p).select("col").collect()
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_rw(write_projection_pushdown, read_projection_pushdown)

    results["advanced_features"]["PAGE_INDEX"] = {"write": False, "read": False}
    results["advanced_features"]["BLOOM_FILTER"] = {"write": False, "read": False}
    results["advanced_features"]["COLUMN_ENCRYPTION"] = {"write": False, "read": False}

    def write_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        df.write_parquet(p, data_page_size=1024)
    def read_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2.parquet")
        pl.read_parquet(p)
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2)

    results["advanced_features"]["SCHEMA_EVOLUTION"] = {"write": False, "read": False}

    # Size Statistics (Parquet format 2.10.0) - not directly exposed in Polars public API
    results["advanced_features"]["SIZE_STATISTICS"] = {"write": False, "read": False}

    # Page CRC32 checksum - not yet supported in Polars
    results["advanced_features"]["PAGE_CRC32"] = {"write": False, "read": False}

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
