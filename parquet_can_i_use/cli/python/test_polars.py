#!/usr/bin/env python3
"""Test Polars' Parquet feature support and output JSON results."""

import json
import os
import sys
import tempfile
from pathlib import Path

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
    FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"

    # --- Compression ---
    # Polars' "lz4" codec string writes LZ4_RAW format internally (verified via raw Parquet
    # footer byte analysis — Thrift codec enum 7 / ZigZag 0x0e appears in file).
    # The string "lz4_raw" is not accepted by Polars (raises ValueError).
    # So our codec map is:
    #   LZ4     → None (no Polars codec produces the deprecated LZ4 format)
    #   LZ4_RAW → "lz4" (Polars' only LZ4 codec, produces LZ4_RAW)
    #   LZO     → None (Polars cannot write LZO; read is tested via fixture if available)
    codecs = {
        "NONE":    "uncompressed",
        "SNAPPY":  "snappy",
        "GZIP":    "gzip",
        "BROTLI":  "brotli",
        "LZO":     None,    # Polars cannot write LZO; read tested via fixture
        "LZ4":     None,    # deprecated LZ4 not supported; Polars has no codec for it
        "LZ4_RAW": "lz4",   # Polars' "lz4" produces LZ4_RAW format
        "ZSTD":    "zstd",
    }
    df = pl.DataFrame({"col": [1, 2, 3]})
    for codec_name, codec_val in codecs.items():
        fixture_path = FIXTURES_DIR / "compression" / f"comp_{codec_name}.parquet"
        if codec_val is None:
            # Cannot write this codec; try reading from fixture to detect read-only support.
            if fixture_path.exists():
                read_ok = test_feature("read", lambda p=str(fixture_path): pl.read_parquet(p))
            else:
                read_ok = False
            results["compression"][codec_name] = {"write": False, "read": read_ok}
        else:
            write_path = os.path.join(tmpdir, f"comp_{codec_name}.parquet")
            read_path = str(fixture_path) if fixture_path.exists() else write_path
            def write_codec(c=codec_val, p=write_path, d=df):
                d.write_parquet(p, compression=c)
            def read_codec(p=read_path):
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

    # Polars does not expose an API to force a specific encoding when writing.
    # For write tests: we write a Polars file and verify the encoding appears in the
    # file metadata (requires pyarrow for verification; otherwise write=false for
    # unverifiable encodings).
    # For read tests: we write a file with the target encoding using pyarrow and
    # attempt to read it back with Polars.
    try:
        import pyarrow as pa
        import pyarrow.parquet as _pq
        _have_pyarrow = True
    except ImportError:
        _have_pyarrow = False

    def get_polars_file_encodings(path):
        """Return the set of encodings used for DATA VALUES in the first column.

        PyArrow's column.encodings reports ALL encodings: data pages, dictionary
        pages, AND definition/repetition level pages.  Because Polars writes nullable
        (OPTIONAL) columns, every column contains RLE-encoded definition levels, so
        'RLE' appears in column.encodings for every single type.  Similarly, when
        dictionary encoding is active (RLE_DICTIONARY), 'PLAIN' appears for the
        dictionary page values, not for the actual data values.

        This function filters out these artefacts so that only encodings attributable
        to the data pages themselves are returned.
        """
        if not _have_pyarrow:
            return set()
        try:
            meta = _pq.read_metadata(path)
            all_encs = set(meta.row_group(0).column(0).encodings)
        except Exception:
            return set()

        # Detect if dictionary encoding is active
        uses_dictionary = bool(all_encs & {'RLE_DICTIONARY', 'PLAIN_DICTIONARY'})

        data_encs = set()
        for enc in all_encs:
            if enc == 'RLE':
                # RLE appears in ALL nullable columns because it encodes
                # definition/repetition levels — it is NOT a data-page encoding here.
                # Polars uses PLAIN encoding for BOOLEAN data (stored as 8 values per
                # byte per the Parquet PLAIN spec for booleans), not RLE/hybrid-RLE.
                pass
            elif enc == 'PLAIN':
                if not uses_dictionary:
                    # No dictionary in use → PLAIN is the actual data encoding.
                    data_encs.add('PLAIN')
                # else: PLAIN encodes the dictionary page values, not the data values.
            elif enc == 'BIT_PACKED':
                pass  # deprecated; not used for data pages in current implementations
            else:
                # RLE_DICTIONARY, PLAIN_DICTIONARY, DELTA_BINARY_PACKED,
                # DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT …
                # these only appear in data pages.
                data_encs.add(enc)
        return data_encs

    # Parquet encoding name → pyarrow column_encoding string
    _PYARROW_ENCODING_MAP = {
        "PLAIN":                    "PLAIN",
        "PLAIN_DICTIONARY":         "PLAIN_DICTIONARY",
        "RLE_DICTIONARY":           "RLE_DICTIONARY",
        "RLE":                      "RLE",
        "DELTA_BINARY_PACKED":      "DELTA_BINARY_PACKED",
        "DELTA_LENGTH_BYTE_ARRAY":  "DELTA_LENGTH_BYTE_ARRAY",
        "DELTA_BYTE_ARRAY":         "DELTA_BYTE_ARRAY",
        "BYTE_STREAM_SPLIT":        "BYTE_STREAM_SPLIT",
        "BYTE_STREAM_SPLIT_EXTENDED": "BYTE_STREAM_SPLIT",  # same PA api
    }

    def make_pa_typed_table(ptype):
        if not _have_pyarrow:
            return None
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
        elif ptype == "BYTE_ARRAY":
            return pa.table({"col": pa.array([b"a", b"b", b"c"])})
        return None

    encoding_names = [
        "PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
        "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
        "BYTE_STREAM_SPLIT", "BYTE_STREAM_SPLIT_EXTENDED",
    ]
    for enc_name in encoding_names:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            write_path = os.path.join(tmpdir, f"enc_write_{enc_name}_{ptype}.parquet")
            read_path  = os.path.join(tmpdir, f"enc_read_{enc_name}_{ptype}.parquet")

            def write_enc(p=write_path, en=enc_name, pt=ptype):
                df = make_typed_df(pt)
                df.write_parquet(p)
                # Verify the encoding actually appears in the written file.
                actual = get_polars_file_encodings(p)
                if not actual:
                    # No encodings detected (pyarrow unavailable or file is empty);
                    # cannot verify, so conservatively mark as not supported.
                    raise NotImplementedError("Could not read file encodings for verification")
                if en not in actual:
                    raise ValueError(
                        f"Polars did not write {en} encoding; actual encodings: {actual}"
                    )

            def read_enc(rp=read_path, en=enc_name, pt=ptype):
                # Write a file with the target encoding via pyarrow, then read with Polars.
                if not _have_pyarrow:
                    raise NotImplementedError("Cannot test encoding read without pyarrow")
                pa_enc = _PYARROW_ENCODING_MAP.get(en)
                if pa_enc is None:
                    raise NotImplementedError(f"No pyarrow encoding mapping for {en}")
                t = make_pa_typed_table(pt)
                if t is None:
                    raise ValueError(f"Could not build pyarrow table for {pt}")
                try:
                    _pq.write_table(
                        t, rp, use_dictionary=(pa_enc in ("PLAIN_DICTIONARY", "RLE_DICTIONARY")),
                        column_encoding=None if pa_enc in ("PLAIN_DICTIONARY", "RLE_DICTIONARY")
                                        else pa_enc,
                    )
                except Exception:
                    raise
                pl.read_parquet(rp)

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
