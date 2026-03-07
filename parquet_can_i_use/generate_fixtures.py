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


if __name__ == "__main__":
    print("Generating parquet fixtures...")
    print()
    print("Compression codecs:")
    generate_compression_fixtures()
    print()
    print(f"Fixtures written to: {FIXTURES_DIR}")
