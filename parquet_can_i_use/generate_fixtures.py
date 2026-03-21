#!/usr/bin/env python3
"""Generate parquet fixture files for each feature category.

Fixtures are pre-generated sample parquet files committed to the repository.
Each library's test CLI uses these fixtures to test read support independently
of write support — a library can be tested for reading formats it cannot write.

Usage:
    python generate_fixtures.py

Requirements:
    - pyarrow  (for most compression codecs)
    - fastparquet + python-lzo or liblzo2.so.2 (for LZO)
"""

import ctypes
import ctypes.util
import os
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _try_lzo_via_ctypes():
    """Return (compress_fn, decompress_fn) using liblzo2 via ctypes, or None."""
    lib_name = ctypes.util.find_library("lzo2") or "liblzo2.so.2"
    try:
        lib = ctypes.CDLL(lib_name)
    except OSError:
        return None

    try:
        lib.__lzo_init_v2.restype = ctypes.c_int
        lib.__lzo_init_v2.argtypes = [ctypes.c_uint] + [ctypes.c_int] * 9
        if lib.__lzo_init_v2(9, -1, -1, -1, -1, -1, -1, -1, -1, -1) != 0:
            return None
    except AttributeError:
        return None

    LZO1X_1_MEM_COMPRESS = 65536

    lib.lzo1x_1_compress.argtypes = [
        ctypes.c_char_p, ctypes.c_ulong,
        ctypes.c_char_p, ctypes.POINTER(ctypes.c_ulong),
        ctypes.c_char_p,
    ]
    lib.lzo1x_1_compress.restype = ctypes.c_int
    lib.lzo1x_decompress_safe.argtypes = [
        ctypes.c_char_p, ctypes.c_ulong,
        ctypes.c_char_p, ctypes.POINTER(ctypes.c_ulong),
        ctypes.c_void_p,
    ]
    lib.lzo1x_decompress_safe.restype = ctypes.c_int

    def compress(data):
        src = bytes(data)
        dst_size = len(src) + len(src) // 16 + 64 + 3
        dst = ctypes.create_string_buffer(dst_size)
        dst_len = ctypes.c_ulong(dst_size)
        wrkmem = ctypes.create_string_buffer(LZO1X_1_MEM_COMPRESS)
        ret = lib.lzo1x_1_compress(src, len(src), dst, ctypes.byref(dst_len), wrkmem)
        if ret != 0:
            raise RuntimeError(f"lzo1x_1_compress failed: {ret}")
        return bytes(dst[: dst_len.value])

    def decompress(data, uncompressed_size=None):
        src = bytes(data)
        out_size = uncompressed_size if uncompressed_size else max(len(src) * 10, 64)
        out = ctypes.create_string_buffer(out_size)
        out_len = ctypes.c_ulong(out_size)
        ret = lib.lzo1x_decompress_safe(src, len(src), out, ctypes.byref(out_len), None)
        if ret != 0:
            raise RuntimeError(f"lzo1x_decompress_safe failed: {ret}")
        return bytes(out[: out_len.value])

    return compress, decompress


def _generate_lz4_deprecated_fixture(out_dir: Path) -> bool:
    """Write comp_LZ4.parquet using the deprecated LZ4 format (Thrift codec 5).

    pyarrow's "lz4" produces LZ4_RAW (codec 7), so we use fastparquet which
    has a distinct 'LZ4' codec that correctly writes the old deprecated format.
    """
    out_path = out_dir / "comp_LZ4.parquet"
    try:
        import fastparquet
        import pandas as pd
        fastparquet.write(str(out_path), pd.DataFrame({"col": [1, 2, 3]}), compression="LZ4")
        print(f"  OK: {out_path.name}  (fastparquet deprecated LZ4)")
        return True
    except ImportError:
        print(f"  SKIP: {out_path.name}  (fastparquet not available)")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")
    return False


def _generate_lzo_fixture(out_dir: Path) -> bool:
    """Write comp_LZO.parquet. Returns True on success."""
    out_path = out_dir / "comp_LZO.parquet"

    # 1) Try python-lzo (the canonical python-lzo package)
    try:
        import lzo  # noqa: F401 -- just checking availability
        import fastparquet
        import pandas as pd
        fastparquet.write(str(out_path), pd.DataFrame({"col": [1, 2, 3]}), compression="LZO")
        print(f"  OK: {out_path.name}  (python-lzo)")
        return True
    except ImportError:
        pass
    except Exception as e:
        print(f"  WARN: python-lzo available but write failed: {e}")

    # 2) Try patching fastparquet with ctypes liblzo2
    fns = _try_lzo_via_ctypes()
    if fns is not None:
        try:
            import fastparquet
            import fastparquet.compression as fc
            import pandas as pd

            lzo_compress, lzo_decompress = fns
            fc.compressions["LZO"] = lzo_compress
            fc.decompressions["LZO"] = lzo_decompress

            fastparquet.write(str(out_path), pd.DataFrame({"col": [1, 2, 3]}), compression="LZO")
            print(f"  OK: {out_path.name}  (ctypes liblzo2)")
            return True
        except Exception as e:
            print(f"  WARN: ctypes liblzo2 available but write failed: {e}")

    print(
        f"  SKIP: {out_path.name}  "
        "(requires python-lzo or liblzo2 + fastparquet; install liblzo2-dev then re-run)"
    )
    return False


def generate_compression_fixtures() -> None:
    """Generate one parquet fixture file per compression codec."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required to generate compression fixtures.")
        return

    out_dir = FIXTURES_DIR / "compression"
    _ensure_dir(out_dir)

    table = pa.table({"col": pa.array([1, 2, 3], type=pa.int32())})

    # Codecs that pyarrow supports natively.
    # Note: pyarrow's "lz4" string produces LZ4_RAW (Thrift codec 7), not deprecated LZ4
    # (Thrift codec 5).  Both LZ4 and LZ4_RAW fixtures therefore need separate handling.
    pyarrow_codecs: dict[str, str] = {
        "NONE": "none",
        "SNAPPY": "snappy",
        "GZIP": "gzip",
        "BROTLI": "brotli",
        "LZ4_RAW": "lz4",  # pyarrow's only LZ4 option; produces LZ4_RAW (Thrift codec 7)
        "ZSTD": "zstd",
    }

    for codec_name, codec_val in pyarrow_codecs.items():
        out_path = out_dir / f"comp_{codec_name}.parquet"
        try:
            pq.write_table(table, str(out_path), compression=codec_val)
            print(f"  OK: {out_path.name}")
        except Exception as e:
            print(f"  FAILED: {out_path.name}: {e}")

    # Deprecated LZ4 (Thrift codec 5) requires fastparquet which uses the legacy codec.
    # pyarrow's "lz4" always produces LZ4_RAW (codec 7), so it cannot generate this fixture.
    _generate_lz4_deprecated_fixture(out_dir)

    # LZO requires a special path
    _generate_lzo_fixture(out_dir)


def generate_logical_type_fixtures() -> None:
    """Generate one parquet fixture file per logical type using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required to generate logical type fixtures.")
        return

    import datetime
    import decimal

    out_dir = FIXTURES_DIR / "logical_types"
    _ensure_dir(out_dir)

    logical_types: dict[str, object] = {
        "STRING":            lambda: pa.table({"c": pa.array(["hello"])}),
        "DATE":              lambda: pa.table({"c": pa.array([datetime.date(2024, 1, 1)])}),
        "TIME_MILLIS":       lambda: pa.table({"c": pa.array([datetime.time(12, 0, 0)], type=pa.time32("ms"))}),
        "TIME_MICROS":       lambda: pa.table({"c": pa.array([datetime.time(12, 0, 0)], type=pa.time64("us"))}),
        "TIME_NANOS":        lambda: pa.table({"c": pa.array([datetime.time(12, 0, 0)], type=pa.time64("ns"))}),
        "TIMESTAMP_MILLIS":  lambda: pa.table({"c": pa.array([datetime.datetime(2024, 1, 1)], type=pa.timestamp("ms"))}),
        "TIMESTAMP_MICROS":  lambda: pa.table({"c": pa.array([datetime.datetime(2024, 1, 1)], type=pa.timestamp("us"))}),
        "TIMESTAMP_NANOS":   lambda: pa.table({"c": pa.array([datetime.datetime(2024, 1, 1)], type=pa.timestamp("ns"))}),
        "DECIMAL":           lambda: pa.table({"c": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2))}),
        "UUID":              lambda: pa.table({"c": pa.array(["550e8400-e29b-41d4-a716-446655440000"])}),
        "JSON":              lambda: pa.table({"c": pa.array(['{"key":"val"}'])}),
        "FLOAT16":           lambda: pa.table({"c": pa.array([1.0], type=pa.float16())}),
        "ENUM":              lambda: pa.table({"c": pa.array(["A"], type=pa.dictionary(pa.int8(), pa.string()))}),
        "BSON":              lambda: pa.table({"c": pa.array([b'\x05\x00\x00\x00\x00'], type=pa.binary())}),
        # INTERVAL: Parquet stores as FIXED_LEN_BYTE_ARRAY(12) = (months, days, millis) as uint32 LE
        "INTERVAL":          lambda: pa.table({"c": pa.array([b'\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00'], type=pa.binary(12))}),
        "UNKNOWN":           lambda: pa.table({"c": pa.array([None, None], type=pa.null())}),
    }

    for type_name, make_table in logical_types.items():
        out_path = out_dir / f"lt_{type_name}.parquet"
        try:
            t = make_table()
            if type_name == "INT96":
                pq.write_table(t, str(out_path), use_deprecated_int96_timestamps=True)
            else:
                pq.write_table(t, str(out_path))
            print(f"  OK: {out_path.name}")
        except Exception as e:
            print(f"  FAILED: {out_path.name}: {e}")

    # INT96: requires deprecated int96 timestamps flag
    out_path = out_dir / "lt_INT96.parquet"
    try:
        t = pa.table({"c": pa.array([datetime.datetime(2024, 1, 1)], type=pa.timestamp("ns"))})
        pq.write_table(t, str(out_path), use_deprecated_int96_timestamps=True)
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")


def generate_nested_type_fixtures() -> None:
    """Generate one parquet fixture file per nested type using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required to generate nested type fixtures.")
        return

    out_dir = FIXTURES_DIR / "nested_types"
    _ensure_dir(out_dir)

    nested_types: dict[str, object] = {
        "LIST":         lambda: pa.table({"c": pa.array([[1, 2], [3]])}),
        "MAP":          lambda: pa.table({"c": pa.array([[("a", 1), ("b", 2)]], type=pa.map_(pa.string(), pa.int64()))}),
        "STRUCT":       lambda: pa.table({"c": pa.array([{"x": 1, "y": 2}])}),
        "NESTED_LIST":  lambda: pa.table({"c": pa.array([[[1, 2], [3]], [[4]]])}),
        "NESTED_MAP":   lambda: pa.table({"c": pa.array([[("a", [1, 2]), ("b", [3])]], type=pa.map_(pa.string(), pa.list_(pa.int64())))}),
        "DEEP_NESTING": lambda: pa.table({"c": pa.array([[{"x": [1, 2]}]])}),
    }

    for type_name, make_table in nested_types.items():
        out_path = out_dir / f"nt_{type_name}.parquet"
        try:
            t = make_table()
            pq.write_table(t, str(out_path))
            print(f"  OK: {out_path.name}")
        except Exception as e:
            print(f"  FAILED: {out_path.name}: {e}")


def generate_encoding_fixtures() -> None:
    """Generate parquet fixture files for each encoding using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required to generate encoding fixtures.")
        return

    out_dir = FIXTURES_DIR / "encodings"
    _ensure_dir(out_dir)

    def get_column_encodings(path):
        meta = pq.read_metadata(path)
        encodings = set()
        for i in range(meta.num_row_groups):
            col = meta.row_group(i).column(0)
            encodings.update(col.encodings)
        return encodings

    encoding_types: dict[str, object] = {
        "INT32":      pa.array([1, 2, 3], type=pa.int32()),
        "INT64":      pa.array([1, 2, 3], type=pa.int64()),
        "FLOAT":      pa.array([1.0, 2.0, 3.0], type=pa.float32()),
        "DOUBLE":     pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        "BOOLEAN":    pa.array([True, False, True]),
        "BYTE_ARRAY": pa.array([b"hello", b"world", b"test"]),
    }

    encodings_to_generate: dict[str, object] = {
        "PLAIN":                    {"use_dictionary": False, "column_encoding": "PLAIN"},
        "PLAIN_DICTIONARY":         {"use_dictionary": True},
        "RLE_DICTIONARY":           {"use_dictionary": True},
        "RLE":                      {"use_dictionary": False, "column_encoding": "RLE"},
        "DELTA_BINARY_PACKED":      {"use_dictionary": False, "column_encoding": "DELTA_BINARY_PACKED"},
        "DELTA_LENGTH_BYTE_ARRAY":  {"use_dictionary": False, "column_encoding": "DELTA_LENGTH_BYTE_ARRAY"},
        "DELTA_BYTE_ARRAY":         {"use_dictionary": False, "column_encoding": "DELTA_BYTE_ARRAY"},
        "BYTE_STREAM_SPLIT":        {"use_dictionary": False, "column_encoding": "BYTE_STREAM_SPLIT"},
    }

    # Per-encoding, find a suitable type and generate a fixture
    encoding_type_map = {
        "PLAIN":                    "INT32",
        "PLAIN_DICTIONARY":         "INT32",
        "RLE_DICTIONARY":           "INT32",
        "RLE":                      "BOOLEAN",
        "DELTA_BINARY_PACKED":      "INT32",
        "DELTA_LENGTH_BYTE_ARRAY":  "BYTE_ARRAY",
        "DELTA_BYTE_ARRAY":         "BYTE_ARRAY",
        "BYTE_STREAM_SPLIT":        "FLOAT",
        "BYTE_STREAM_SPLIT_EXTENDED": "FLOAT",
    }

    for enc_name, ptype_name in encoding_type_map.items():
        out_path = out_dir / f"enc_{enc_name}.parquet"
        arr = encoding_types.get(ptype_name)
        if arr is None:
            print(f"  SKIP: {out_path.name} (no array for type {ptype_name})")
            continue
        table = pa.table({"col": arr})
        try:
            if enc_name in ("PLAIN_DICTIONARY", "RLE_DICTIONARY"):
                pq.write_table(table, str(out_path), use_dictionary=True)
            elif enc_name == "BYTE_STREAM_SPLIT_EXTENDED":
                pq.write_table(table, str(out_path), use_dictionary=False, column_encoding="BYTE_STREAM_SPLIT")
            else:
                write_kwargs = encodings_to_generate.get(enc_name, {})
                pq.write_table(table, str(out_path), **write_kwargs)
            print(f"  OK: {out_path.name}")
        except Exception as e:
            print(f"  FAILED: {out_path.name}: {e}")

    # BIT_PACKED is deprecated; pyarrow does not write it for data pages, skip
    print(f"  SKIP: enc_BIT_PACKED.parquet  (deprecated, pyarrow does not write it for data pages)")


def generate_advanced_feature_fixtures() -> None:
    """Generate parquet fixture files for advanced features using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required to generate advanced feature fixtures.")
        return

    out_dir = FIXTURES_DIR / "advanced_features"
    _ensure_dir(out_dir)

    table = pa.table({"col": pa.array(range(100)), "str_col": pa.array([f"val_{i}" for i in range(100)])})

    # STATISTICS
    out_path = out_dir / "adv_STATISTICS.parquet"
    try:
        pq.write_table(table, str(out_path), write_statistics=True)
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    # PAGE_INDEX
    out_path = out_dir / "adv_PAGE_INDEX.parquet"
    try:
        pq.write_table(table, str(out_path), write_page_index=True)
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    # DATA_PAGE_V2
    out_path = out_dir / "adv_DATA_PAGE_V2.parquet"
    try:
        pq.write_table(table, str(out_path), data_page_version="2.0")
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    # SIZE_STATISTICS
    out_path = out_dir / "adv_SIZE_STATISTICS.parquet"
    try:
        pq.write_table(table, str(out_path), write_statistics=True)
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    # PAGE_CRC32
    out_path = out_dir / "adv_PAGE_CRC32.parquet"
    try:
        pq.write_table(table, str(out_path), write_page_checksum=True)
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    # PREDICATE_PUSHDOWN / PROJECTION_PUSHDOWN: just a plain file (read-side feature)
    out_path = out_dir / "adv_PREDICATE_PUSHDOWN.parquet"
    try:
        pq.write_table(table, str(out_path))
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")

    out_path = out_dir / "adv_PROJECTION_PUSHDOWN.parquet"
    try:
        pq.write_table(table, str(out_path))
        print(f"  OK: {out_path.name}")
    except Exception as e:
        print(f"  FAILED: {out_path.name}: {e}")


if __name__ == "__main__":
    print("Generating parquet fixtures...")
    print()
    print("Compression codecs:")
    generate_compression_fixtures()
    print()
    print("Logical types:")
    generate_logical_type_fixtures()
    print()
    print("Nested types:")
    generate_nested_type_fixtures()
    print()
    print("Encodings:")
    generate_encoding_fixtures()
    print()
    print("Advanced features:")
    generate_advanced_feature_fixtures()
    print()
    print(f"Fixtures written to: {FIXTURES_DIR}")
