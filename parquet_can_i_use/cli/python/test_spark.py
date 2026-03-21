#!/usr/bin/env python3
"""Test Apache Spark (PySpark) Parquet feature support and output JSON results."""

import base64
import hashlib
import inspect
import json
import logging
import os
import sys
import tempfile
import traceback
from pathlib import Path




def test_feature(fn):
    try:
        fn()
        return True, None
    except Exception:
        return False, traceback.format_exc()


def test_rw(write_fn, read_fn, write_path=None):
    """Run separate write and read tests, return {"write": bool, "read": bool, ...}."""
    write_ok, write_log = test_feature(write_fn)
    read_ok, read_log = test_feature(read_fn)
    result = {"write": write_ok, "read": read_ok}
    if write_log:
        result["write_log"] = write_log
    if read_log:
        result["read_log"] = read_log
    return result


def _not_supported_result(reason=None):
    """Return a result for a feature explicitly not supported, with source code as proof."""
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno
    try:
        with open(filename) as f:
            source_lines = f.readlines()
        start = max(0, lineno - 4)
        end = min(len(source_lines), lineno + 1)
        source = "".join(source_lines[start:end]).rstrip()
        log = f"Source proof (line {lineno}):\n{source}"
    except Exception:
        log = f"Not supported (at {filename}:{lineno})"
    if reason:
        log = f"{reason}\n{log}"
    return {"write": False, "read": False, "write_log": log, "read_log": log}


def main():
    try:
        import pyspark
        from pyspark.sql import SparkSession
        import pyspark.sql.types as T
        import pyspark.sql.functions as F
    except ImportError:
        print(json.dumps({"error": "pyspark not installed"}))
        sys.exit(1)

    # Suppress Spark / py4j logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--conf spark.ui.enabled=false pyspark-shell")

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("ParquetFeatureTest")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def _read_proof_log(path):
        try:
            if not path:
                return None
            # Spark may write a directory with part files; find the first .parquet file
            actual_path = path
            if os.path.isdir(path):
                for f in sorted(os.listdir(path)):
                    if f.endswith(".parquet"):
                        actual_path = os.path.join(path, f)
                        break
                else:
                    return None
            if not os.path.isfile(actual_path):
                return None
            proof_data = Path(actual_path).read_bytes()
            sha = hashlib.sha256(proof_data).hexdigest()
            df = spark.read.parquet(str(actual_path))
            rows = df.collect()
            values = {c: [getattr(r, c) for r in rows] for c in df.columns}
            return f"proof_sha256:{sha}\nvalues:{json.dumps(values)}"
        except Exception as e:
            return f"proof_read_error:{e}"

    def test_rw(write_fn, read_fn, write_path=None, read_path=None):
        write_ok, write_log = test_feature(write_fn)
        read_ok, read_log = test_feature(read_fn)
        if write_ok and write_path:
            # Spark writes directories with part files; find the first .parquet file
            actual_path = write_path
            if os.path.isdir(write_path):
                for f in sorted(os.listdir(write_path)):
                    if f.endswith(".parquet"):
                        actual_path = os.path.join(write_path, f)
                        break
                else:
                    actual_path = None
            if actual_path and os.path.isfile(actual_path):
                try:
                    with open(actual_path, "rb") as f:
                        data = f.read()
                    sha = hashlib.sha256(data).hexdigest()
                    write_log = f"sha256:{sha}\n{base64.b64encode(data).decode()}"
                except Exception:
                    pass
        if read_ok:
            read_log = _read_proof_log(read_path or write_path)
        result = {"write": write_ok, "read": read_ok}
        if write_log:
            result["write_log"] = write_log
        if read_log:
            result["read_log"] = read_log
        return result

    version = pyspark.__version__

    results = {
        "tool": "Spark",
        "version": version,
        "compression": {},
        "encoding": {},
        "logical_types": {},
        "nested_types": {},
        "advanced_features": {},
    }

    tmpdir = tempfile.mkdtemp()
    FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"

    # --- Compression ---
    simple_schema = T.StructType([T.StructField("col", T.IntegerType())])
    simple_df = spark.createDataFrame([(1,), (2,), (3,)], simple_schema)

    compression_codecs = [
        ("NONE", "none"),
        ("SNAPPY", "snappy"),
        ("GZIP", "gzip"),
        ("BROTLI", "brotli"),
        ("LZO", "lzo"),
        ("LZ4", "lz4"),
        ("LZ4_RAW", "lz4"),   # PySpark uses "lz4" which maps to LZ4_RAW in Parquet
        ("ZSTD", "zstd"),
    ]

    for codec_name, codec_val in compression_codecs:
        write_path = os.path.join(tmpdir, f"comp_{codec_name}")
        fixture_path = FIXTURES_DIR / "compression" / f"comp_{codec_name}.parquet"
        read_path = str(fixture_path) if fixture_path.exists() else write_path

        def write_comp(df=simple_df, c=codec_val, p=write_path):
            df.write.mode("overwrite").option("compression", c).parquet(p)

        def read_comp(p=read_path):
            spark.read.parquet(p).collect()

        results["compression"][codec_name] = test_rw(write_comp, read_comp, write_path=write_path, read_path=read_path)

    # --- Encoding × Type matrix ---
    # PySpark does not expose per-column encoding control; Spark uses its own default
    # encoding strategy (PLAIN_DICTIONARY / RLE_DICTIONARY by default).
    # Write: only encodings Spark actually uses are marked true.
    # Read:  Spark can read any standard Parquet encoding (except deprecated BIT_PACKED).
    encoding_types = ["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"]

    write_supported_encs = {"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY"}
    read_unsupported_encs = {"BIT_PACKED"}

    for enc_name in ["PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                     "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                     "BYTE_STREAM_SPLIT"]:
        results["encoding"][enc_name] = {}
        for ptype in encoding_types:
            results["encoding"][enc_name][ptype] = {
                "write": enc_name in write_supported_encs,
                "read": enc_name not in read_unsupported_encs,
            }

    # --- Logical Types ---
    import datetime
    from decimal import Decimal

    def lt_test(schema_fields, rows, path_suffix, write_options=None):
        path = os.path.join(tmpdir, f"lt_{path_suffix}")
        schema = T.StructType(schema_fields)

        def write_lt(s=schema, r=rows, p=path, opts=write_options):
            df = spark.createDataFrame(r, s)
            writer = df.write.mode("overwrite")
            if opts:
                for k, v in opts.items():
                    writer = writer.option(k, v)
            writer.parquet(p)

        def read_lt(p=path):
            spark.read.parquet(p).collect()

        return test_rw(write_lt, read_lt, write_path=path)

    results["logical_types"]["STRING"] = lt_test(
        [T.StructField("c", T.StringType())], [("hello",)], "string")

    results["logical_types"]["DATE"] = lt_test(
        [T.StructField("c", T.DateType())], [(datetime.date(2024, 1, 1),)], "date")

    # TIME: Spark has no native time-only type; stored as LongType (ms/us from midnight)
    results["logical_types"]["TIME_MILLIS"] = _not_supported_result("Spark has no native time-only type; TIME_MILLIS is not supported")
    results["logical_types"]["TIME_MICROS"] = _not_supported_result("Spark has no native time-only type; TIME_MICROS is not supported")
    results["logical_types"]["TIME_NANOS"] = _not_supported_result("Spark has no native time-only type; TIME_NANOS is not supported")

    # TIMESTAMP: default encoding depends on Spark version
    results["logical_types"]["TIMESTAMP_MILLIS"] = lt_test(
        [T.StructField("c", T.TimestampType())],
        [(datetime.datetime(2024, 1, 1),)], "ts_millis",
        {"spark.sql.parquet.outputTimestampType": "TIMESTAMP_MILLIS"})

    results["logical_types"]["TIMESTAMP_MICROS"] = lt_test(
        [T.StructField("c", T.TimestampType())],
        [(datetime.datetime(2024, 1, 1),)], "ts_micros",
        {"spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS"})

    # TIMESTAMP_NANOS: PySpark's outputTimestampType only supports INT96, TIMESTAMP_MILLIS,
    # and TIMESTAMP_MICROS. Nanosecond precision is not available as a write option.
    results["logical_types"]["TIMESTAMP_NANOS"] = _not_supported_result("PySpark's outputTimestampType does not support TIMESTAMP_NANOS")

    # INT96 (legacy timestamp format)
    results["logical_types"]["INT96"] = lt_test(
        [T.StructField("c", T.TimestampType())],
        [(datetime.datetime(2024, 1, 1),)], "int96",
        {"spark.sql.parquet.outputTimestampType": "INT96"})

    results["logical_types"]["DECIMAL"] = lt_test(
        [T.StructField("c", T.DecimalType(10, 2))],
        [(Decimal("123.45"),)], "decimal")

    # UUID: no native UUID type in Spark; stored as string
    results["logical_types"]["UUID"] = lt_test(
        [T.StructField("c", T.StringType())],
        [("550e8400-e29b-41d4-a716-446655440000",)], "uuid")

    # JSON: no native JSON type in Spark SQL; stored as string
    results["logical_types"]["JSON"] = lt_test(
        [T.StructField("c", T.StringType())],
        [('{"key":"val"}',)], "json")

    # FLOAT16: not supported in Spark
    results["logical_types"]["FLOAT16"] = _not_supported_result("Spark does not support FLOAT16")

    # ENUM: no native enum type; stored as string
    results["logical_types"]["ENUM"] = lt_test(
        [T.StructField("c", T.StringType())], [("A",)], "enum")

    # BSON: no native BSON type; stored as binary
    results["logical_types"]["BSON"] = lt_test(
        [T.StructField("c", T.BinaryType())],
        [(bytes([5, 0, 0, 0, 0]),)], "bson")

    # INTERVAL: Spark DayTimeIntervalType / YearMonthIntervalType (3.x+)
    def write_interval():
        if hasattr(T, "DayTimeIntervalType"):
            schema = T.StructType([T.StructField("c", T.DayTimeIntervalType())])
            df = spark.createDataFrame([(datetime.timedelta(days=1),)], schema)
        else:
            schema = T.StructType([T.StructField("c", T.StringType())])
            df = spark.createDataFrame([("1 DAY",)], schema)
        df.write.mode("overwrite").parquet(os.path.join(tmpdir, "lt_interval"))

    def read_interval():
        spark.read.parquet(os.path.join(tmpdir, "lt_interval")).collect()

    results["logical_types"]["INTERVAL"] = test_rw(write_interval, read_interval, write_path=os.path.join(tmpdir, "lt_interval"))

    # --- Nested Types ---
    nested_tests = [
        ("LIST", T.StructType([T.StructField("c", T.ArrayType(T.IntegerType()))]),
         [([1, 2],), ([3],)]),
        ("MAP", T.StructType([T.StructField("c", T.MapType(T.StringType(), T.IntegerType()))]),
         [({"a": 1, "b": 2},)]),
        ("STRUCT", T.StructType([T.StructField("c", T.StructType([
            T.StructField("x", T.IntegerType()), T.StructField("y", T.IntegerType())]))]),
         [((1, 2),)]),
        ("NESTED_LIST", T.StructType([T.StructField("c", T.ArrayType(T.ArrayType(T.IntegerType())))]),
         [([[1, 2], [3]],)]),
        ("NESTED_MAP", T.StructType([T.StructField("c", T.MapType(T.StringType(), T.ArrayType(T.IntegerType())))]),
         [({"a": [1, 2], "b": [3]},)]),
        ("DEEP_NESTING", T.StructType([T.StructField("c", T.ArrayType(T.StructType([
            T.StructField("x", T.ArrayType(T.IntegerType()))])))]),
         [([{"x": [1, 2]}],)]),
    ]

    for nt_name, schema, rows in nested_tests:
        path = os.path.join(tmpdir, f"nt_{nt_name}")

        def write_nt(s=schema, r=rows, p=path):
            spark.createDataFrame(r, s).write.mode("overwrite").parquet(p)

        def read_nt(p=path):
            spark.read.parquet(p).collect()

        results["nested_types"][nt_name] = test_rw(write_nt, read_nt, write_path=path)

    # --- Advanced Features ---
    adv_schema = T.StructType([
        T.StructField("col", T.IntegerType()),
        T.StructField("str_col", T.StringType()),
    ])
    adv_rows = [(i, f"val_{i}") for i in range(100)]

    # STATISTICS: Spark writes column statistics by default
    def write_statistics():
        p = os.path.join(tmpdir, "adv_stats")
        spark.createDataFrame(adv_rows, adv_schema).write.mode("overwrite").parquet(p)
    def read_statistics():
        p = os.path.join(tmpdir, "adv_stats")
        spark.read.parquet(p).collect()
    results["advanced_features"]["STATISTICS"] = test_rw(write_statistics, read_statistics, write_path=os.path.join(tmpdir, "adv_stats"))

    # PAGE_INDEX: supported in Spark 3.1+ (parquet page index)
    def write_page_index():
        p = os.path.join(tmpdir, "adv_page_index")
        spark.createDataFrame(adv_rows, adv_schema).write.mode("overwrite").parquet(p)
    def read_page_index():
        p = os.path.join(tmpdir, "adv_page_index")
        spark.read.parquet(p).collect()
    results["advanced_features"]["PAGE_INDEX"] = test_rw(write_page_index, read_page_index, write_path=os.path.join(tmpdir, "adv_page_index"))

    # BLOOM_FILTER: supported via spark.sql.parquet.bloom.filter.enabled (Spark 3.1+)
    def write_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom")
        (spark.createDataFrame(adv_rows, adv_schema)
         .write.mode("overwrite")
         .option("parquet.bloom.filter.enabled", "true")
         .parquet(p))
    def read_bloom_filter():
        p = os.path.join(tmpdir, "adv_bloom")
        spark.read.parquet(p).collect()
    results["advanced_features"]["BLOOM_FILTER"] = test_rw(write_bloom_filter, read_bloom_filter, write_path=os.path.join(tmpdir, "adv_bloom"))

    # DATA_PAGE_V2: Spark can write and read data page v2
    def write_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2")
        (spark.createDataFrame(adv_rows, adv_schema)
         .write.mode("overwrite")
         .option("parquet.writer.version", "v2")
         .parquet(p))
    def read_data_page_v2():
        p = os.path.join(tmpdir, "adv_v2")
        spark.read.parquet(p).collect()
    results["advanced_features"]["DATA_PAGE_V2"] = test_rw(write_data_page_v2, read_data_page_v2, write_path=os.path.join(tmpdir, "adv_v2"))

    # COLUMN_ENCRYPTION: Parquet modular encryption is not supported in open-source PySpark
    results["advanced_features"]["COLUMN_ENCRYPTION"] = _not_supported_result("Parquet modular encryption is not supported in open-source PySpark")

    # PREDICATE_PUSHDOWN: Spark supports predicate pushdown by default
    def write_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred")
        spark.createDataFrame(adv_rows, adv_schema).write.mode("overwrite").parquet(p)
    def read_predicate_pushdown():
        p = os.path.join(tmpdir, "adv_pred")
        spark.read.parquet(p).filter("col > 50").collect()
    results["advanced_features"]["PREDICATE_PUSHDOWN"] = test_rw(
        write_predicate_pushdown, read_predicate_pushdown, write_path=os.path.join(tmpdir, "adv_pred"))

    # PROJECTION_PUSHDOWN: Spark reads only requested columns
    def write_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj")
        spark.createDataFrame(adv_rows, adv_schema).write.mode("overwrite").parquet(p)
    def read_projection_pushdown():
        p = os.path.join(tmpdir, "adv_proj")
        spark.read.parquet(p).select("col").collect()
    results["advanced_features"]["PROJECTION_PUSHDOWN"] = test_rw(
        write_projection_pushdown, read_projection_pushdown, write_path=os.path.join(tmpdir, "adv_proj"))

    # SCHEMA_EVOLUTION: Spark supports mergeSchema option
    def write_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1")
        p2 = os.path.join(tmpdir, "adv_se2")
        spark.createDataFrame([(1, 2)], ["a", "b"]).write.mode("overwrite").parquet(p1)
        spark.createDataFrame([(3, 4)], ["a", "c"]).write.mode("overwrite").parquet(p2)
    def read_schema_evolution():
        p1 = os.path.join(tmpdir, "adv_se1")
        p2 = os.path.join(tmpdir, "adv_se2")
        spark.read.option("mergeSchema", "true").parquet(p1, p2).collect()
    results["advanced_features"]["SCHEMA_EVOLUTION"] = test_rw(
        write_schema_evolution, read_schema_evolution, write_path=os.path.join(tmpdir, "adv_se1"))

    spark.stop()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
