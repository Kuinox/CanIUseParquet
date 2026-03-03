# Can I Use: Parquet Format Support Matrix

A comprehensive compatibility reference for Apache Parquet features across query engines, libraries, and tools.

> **Legend:** ✅ = Supported | ⚠️ = Partial / Read-only | ❌ = Not supported | ➖ = N/A

---

## Tools & Libraries Covered

| Category | Tool | Language / Platform | Description |
|---|---|---|---|
| **Reference Impl.** | parquet-java | Java | Apache reference implementation (formerly parquet-mr) |
| **Libraries** | Apache Arrow (C++) | C++ | Core engine behind PyArrow and many tools |
| **Libraries** | PyArrow | Python (C++ bindings) | Python bindings for Apache Arrow |
| **Libraries** | fastparquet | Python | Pure Python Parquet implementation |
| **Libraries** | parquet-rs (arrow-rs) | Rust | Rust-native Parquet implementation |
| **Libraries** | parquet-dotnet | C# / .NET | Fully managed .NET Parquet library |
| **Libraries** | parquet-go | Go | Go Parquet implementation (parquet-go/parquet-go) |
| **Query Engines** | Apache Spark | JVM | Distributed data processing engine |
| **Query Engines** | DuckDB | C++ | In-process analytical database |
| **Query Engines** | Polars | Rust / Python | DataFrame library |
| **Query Engines** | ClickHouse | C++ | Column-oriented OLAP DBMS |
| **Query Engines** | Apache Hive | JVM | Data warehouse on Hadoop |
| **Query Engines** | Trino (Presto) | Java | Distributed SQL query engine |
| **Query Engines** | Apache Flink | JVM | Stream & batch processing |
| **Query Engines** | Apache Drill | Java | Schema-free SQL query engine |
| **Cloud Services** | BigQuery | Managed | Google Cloud data warehouse |
| **Cloud Services** | Snowflake | Managed | Cloud data platform |
| **Cloud Services** | AWS Athena | Managed | Serverless query service on S3 |
| **Cloud Services** | Databricks | Managed | Unified analytics platform (Spark-based) |

---

## Compression Codecs

| Codec | parquet-java | PyArrow | fastparquet | parquet-rs | parquet-dotnet | parquet-go | Spark | DuckDB | Polars | ClickHouse | Hive | Trino | Flink | Drill | BigQuery | Snowflake | Athena | Databricks |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| UNCOMPRESSED | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SNAPPY | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| GZIP | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BROTLI | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ⚠️ |
| LZO | ✅ | ❌ | ⚠️ | ✅ | ⚠️ | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ⚠️ | ✅ |
| LZ4 (deprecated) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ |
| LZ4_RAW | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ |
| ZSTD | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ✅ |

**Notes:**
- **LZ4 vs LZ4_RAW:** The original `LZ4` codec is deprecated due to Hadoop-specific framing inconsistencies. `LZ4_RAW` uses the standard LZ4 block format and is the preferred replacement.
- **SNAPPY** is the most universally supported codec and is the default in many tools (e.g., Spark).
- **ZSTD** offers an excellent balance of compression ratio and speed, and has become widely supported.
- **parquet-go:** Brotli and LZO are not natively supported; no pure-Go implementations are available for these codecs.
- Some codecs (Brotli, LZO) may require optional dependencies to be installed in certain libraries.

---

## Encoding Types

| Encoding | parquet-java | PyArrow | fastparquet | parquet-rs | parquet-dotnet | parquet-go | Spark | DuckDB | Polars | ClickHouse |
|---|---|---|---|---|---|---|---|---|---|---|
| PLAIN | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PLAIN_DICTIONARY (deprecated) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ✅ |
| RLE_DICTIONARY | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| RLE | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BIT_PACKED (deprecated) | ✅ | ✅ | ⚠️ | ❌ | ✅ | ⚠️ | ✅ | ✅ | ❌ | ✅ |
| DELTA_BINARY_PACKED | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| DELTA_LENGTH_BYTE_ARRAY | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| DELTA_BYTE_ARRAY | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| BYTE_STREAM_SPLIT | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ⚠️ |

**Notes:**
- **PLAIN_DICTIONARY** is deprecated in favor of **RLE_DICTIONARY**. Most modern implementations support both for backward compatibility.
- **BIT_PACKED** is deprecated in favor of the **RLE** hybrid encoding. parquet-rs and Polars have dropped support for it.
- **DELTA** encodings are efficient for sorted integer sequences and variable-length strings with common prefixes. fastparquet does not support them.
- **BYTE_STREAM_SPLIT** is optimized for floating-point and fixed-size binary types.
- Query engines like Hive, Trino, Flink, BigQuery, and Snowflake delegate encoding to the underlying Parquet library and support all standard encodings transparently.

---

## Logical Types

| Logical Type | parquet-java | PyArrow | fastparquet | parquet-rs | parquet-dotnet | parquet-go | Spark | DuckDB | Polars | ClickHouse |
|---|---|---|---|---|---|---|---|---|---|---|
| STRING (UTF8) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ENUM | ✅ | ❌ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ❌ |
| UUID | ✅ | ✅ | ❌ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ⚠️ |
| DATE | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TIME (millis) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TIME (micros) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TIME (nanos) | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| TIMESTAMP (millis) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TIMESTAMP (micros) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| TIMESTAMP (nanos) | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| INT96 (legacy timestamps) | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ |
| DECIMAL | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| JSON | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |
| BSON | ✅ | ✅ | ❌ | ⚠️ | ❌ | ❌ | ⚠️ | ❌ | ❌ | ❌ |
| INTERVAL | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ❌ | ⚠️ | ⚠️ | ✅ | ⚠️ |
| FLOAT16 | ✅ | ✅ | ❌ | ⚠️ | ❌ | ❌ | ⚠️ | ⚠️ | ⚠️ | ✅ |
| VARIANT (new) | ✅ | ⚠️ | ❌ | ⚠️ | ❌ | ❌ | ✅ | ⚠️ | ❌ | ❌ |

**Notes:**
- **INT96** is a legacy type used for timestamps (primarily by Apache Hive/Spark). It is deprecated in the spec but widely supported for backward compatibility.
- **DECIMAL** can be stored on INT32, INT64, FIXED_LEN_BYTE_ARRAY, or BYTE_ARRAY physical types. Support depth varies by implementation.
- **FLOAT16** (half-precision float) is a newer addition to the spec with growing support.
- **VARIANT** is the newest logical type for semi-structured data (announced February 2026). Support is emerging across the ecosystem.
- **ENUM** is read as STRING by PyArrow; it does not write ENUM annotations. ClickHouse reads ENUM as STRING.
- Cloud services (BigQuery, Snowflake, Athena, Databricks) support standard logical types and map them to native types transparently.

---

## Nested & Complex Types

| Type | parquet-java | PyArrow | fastparquet | parquet-rs | parquet-dotnet | parquet-go | Spark | DuckDB | Polars | ClickHouse |
|---|---|---|---|---|---|---|---|---|---|---|
| LIST (Array) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| MAP | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| STRUCT (Group) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Nested LIST | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |
| Nested MAP | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |
| Deep nesting (3+ levels) | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |

**Notes:**
- Parquet uses the Dremel encoding model with **repetition** and **definition levels** to represent nested data.
- Deep nesting support varies; most engines handle 2-3 levels well, but very deeply nested schemas may encounter edge cases.
- **Snowflake** reads nested types as `VARIANT`, requiring explicit casting in queries.
- **Athena** and **Redshift Spectrum** support nested types with `ROW`, `ARRAY`, and `MAP` syntax.

---

## Advanced Features

| Feature | parquet-java | PyArrow | fastparquet | parquet-rs | parquet-dotnet | parquet-go | Spark | DuckDB | Polars | ClickHouse |
|---|---|---|---|---|---|---|---|---|---|---|
| **Page Index (Column Index)** | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ |
| **Bloom Filters** | ✅ | ⚠️ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ⚠️ |
| **Column Encryption** | ✅ | ✅ | ❌ | ✅ | ❌ | ⚠️ | ✅ | ⚠️ | ❌ | ❌ |
| **Data Page V2** | ✅ | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| **Statistics (min/max)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Predicate Pushdown** | ✅ | ✅ | ⚠️ | ✅ | ❌ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| **Projection Pushdown** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Schema Evolution** | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ |

**Notes:**
- **Page Index** enables page-level min/max statistics for finer-grained predicate pushdown. Available from Parquet format 2.6+.
- **Bloom Filters** enable efficient negative filtering on high-cardinality columns. parquet-rs and parquet-go now have full read/write support. PyArrow has write support.
- **Column Encryption** (Parquet Modular Encryption) allows encrypting individual columns. DuckDB supports footer-level encryption only (single key). parquet-go encryption is emerging/planned.
- **Data Page V2** provides more efficient storage with separate handling of repetition/definition levels.
- **Predicate Pushdown** uses row group and page-level statistics to skip irrelevant data during reads.
- **Schema Evolution** allows reading files with columns added/removed/reordered across different schema versions.

---

## Read/Write Capability

| Tool | Read | Write |
|---|---|---|
| parquet-java | ✅ | ✅ |
| PyArrow | ✅ | ✅ |
| fastparquet | ✅ | ✅ |
| parquet-rs (arrow-rs) | ✅ | ✅ |
| parquet-dotnet | ✅ | ✅ |
| parquet-go | ✅ | ✅ |
| Pandas (via PyArrow/fastparquet) | ✅ | ✅ |
| Apache Spark | ✅ | ✅ |
| DuckDB | ✅ | ✅ |
| Polars | ✅ | ✅ |
| ClickHouse | ✅ | ✅ |
| Apache Hive | ✅ | ✅ |
| Trino (Presto) | ✅ | ✅ |
| Apache Flink | ✅ | ✅ |
| Apache Drill | ✅ | ✅ |
| BigQuery | ✅ | ⚠️ (export) |
| Snowflake | ✅ | ✅ (external tables) |
| AWS Athena | ✅ | ✅ (CTAS) |
| Databricks | ✅ | ✅ |

---

## Recommendations

### Best Interoperability
Use **SNAPPY** or **ZSTD** compression, **RLE_DICTIONARY** encoding, and stick to well-supported logical types (STRING, DATE, TIMESTAMP with micros, DECIMAL). Avoid INT96, LZO, and BSON for maximum cross-tool compatibility.

### Best Compression Ratio
Use **ZSTD** for the best compression-to-speed ratio. **GZIP** provides slightly better compression but is slower. **BROTLI** can also achieve high ratios but has more limited support.

### Best Read Performance
Use **SNAPPY** or **LZ4_RAW** for fastest decompression. Enable **Page Index** and **Bloom Filters** where supported for analytical query workloads.

### Recommended Parquet Version
Use **Parquet format version 2.6+** for modern logical types (nanosecond timestamps, UUID) and advanced features (page index). Use version 1.0 only when maximum backward compatibility is required.

---

## Sources

- [Apache Parquet Format Specification](https://github.com/apache/parquet-format)
- [Apache Parquet Implementation Status](https://parquet.apache.org/docs/file-format/implementationstatus/)
- [Apache Parquet Compression Documentation](https://parquet.apache.org/docs/file-format/data-pages/compression/)
- [Apache Parquet Encoding Documentation](https://parquet.apache.org/docs/file-format/data-pages/encodings/)
- [Apache Parquet Types Documentation](https://parquet.apache.org/docs/file-format/types/)
- [PyArrow Parquet Documentation](https://arrow.apache.org/docs/python/parquet.html)
- [DuckDB Parquet Documentation](https://duckdb.org/docs/data/parquet/overview)
- [DuckDB Bloom Filter Support](https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb)
- [DuckDB Parquet Encryption](https://duckdb.org/docs/stable/data/parquet/encryption)
- [Apache Spark Parquet Documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Polars Parquet Documentation](https://docs.pola.rs/user-guide/io/parquet/)
- [ClickHouse Parquet Documentation](https://clickhouse.com/docs/interfaces/formats/Parquet)
- [parquet-rs (arrow-rs) Documentation](https://docs.rs/parquet/latest/parquet/)
- [parquet-dotnet Documentation](https://github.com/aloneguid/parquet-dotnet)
- [fastparquet Documentation](https://fastparquet.readthedocs.io/)
- [parquet-go Documentation](https://github.com/parquet-go/parquet-go)
- [AWS Athena Compression Formats](https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html)
- [Databricks Data Format Options](https://docs.databricks.com/aws/en/query/formats/)

*Last updated: March 2026. Feature support may change with new releases. Always consult the [official implementation status page](https://parquet.apache.org/docs/file-format/implementationstatus/) for the most current information.*
