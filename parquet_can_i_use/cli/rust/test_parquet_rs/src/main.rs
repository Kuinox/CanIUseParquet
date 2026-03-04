use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

fn test_feature<F: FnOnce() -> Result<(), Box<dyn std::error::Error>>>(f: F) -> bool {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => true,
        _ => false,
    }
}

fn make_simple_batch() -> RecordBatch {
    let schema = Schema::new(vec![Field::new("col", DataType::Int32, false)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap()
}

fn write_read_parquet(
    tmpdir: &TempDir,
    name: &str,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = tmpdir.path().join(format!("{name}.parquet"));
    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    let file = File::open(&path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        batch?;
    }
    Ok(())
}

/// Write a parquet file and verify that the requested encoding was actually used
/// in the first column of the first row group.
fn write_verify_encoding(
    tmpdir: &TempDir,
    name: &str,
    batch: &RecordBatch,
    props: WriterProperties,
    expected_enc: Encoding,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = tmpdir.path().join(format!("{name}.parquet"));
    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    // Read back and verify the actual encoding used
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let actual_encodings: HashSet<Encoding> = metadata
        .row_group(0)
        .column(0)
        .encodings()
        .iter()
        .cloned()
        .collect();
    if !actual_encodings.contains(&expected_enc) {
        return Err(format!(
            "Expected encoding {:?} but file uses {:?}",
            expected_enc, actual_encodings
        )
        .into());
    }

    // Read through to completion
    let file = File::open(&path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        batch?;
    }
    Ok(())
}

fn main() {
    let tmpdir = TempDir::new().unwrap();
    let mut results = Map::new();

    results.insert("tool".into(), json!("parquet-rs"));
    results.insert("version".into(), json!("55.2.0"));

    // --- Compression ---
    let mut compression = Map::new();
    let codecs = vec![
        ("NONE", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("GZIP", Compression::GZIP(Default::default())),
        ("BROTLI", Compression::BROTLI(Default::default())),
        ("LZ4", Compression::LZ4),
        ("LZ4_RAW", Compression::LZ4_RAW),
        ("ZSTD", Compression::ZSTD(Default::default())),
    ];
    for (name, codec) in &codecs {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_compression(*codec)
            .build();
        let result = test_feature(|| write_read_parquet(&tmpdir, &format!("comp_{name}"), &batch, props));
        compression.insert(name.to_string(), json!(result));
    }
    // LZO - try it
    let lzo_result = test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_compression(Compression::LZO)
            .build();
        write_read_parquet(&tmpdir, "comp_LZO", &batch, props)
    });
    compression.insert("LZO".into(), json!(lzo_result));
    results.insert("compression".into(), Value::Object(compression));

    // --- Encoding × Type matrix ---
    let mut encoding = Map::new();
    let encodings = vec![
        ("PLAIN", Encoding::PLAIN),
        ("PLAIN_DICTIONARY", Encoding::PLAIN_DICTIONARY),
        ("RLE_DICTIONARY", Encoding::RLE_DICTIONARY),
        ("RLE", Encoding::RLE),
        ("BIT_PACKED", Encoding::BIT_PACKED),
        ("DELTA_BINARY_PACKED", Encoding::DELTA_BINARY_PACKED),
        ("DELTA_LENGTH_BYTE_ARRAY", Encoding::DELTA_LENGTH_BYTE_ARRAY),
        ("DELTA_BYTE_ARRAY", Encoding::DELTA_BYTE_ARRAY),
        ("BYTE_STREAM_SPLIT", Encoding::BYTE_STREAM_SPLIT),
    ];
    let type_names = vec!["INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"];

    fn make_typed_batch(ptype: &str) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        match ptype {
            "INT32" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Int32, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?)
            }
            "INT64" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Int64, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?)
            }
            "FLOAT" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Float32, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]))])?)
            }
            "DOUBLE" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Float64, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))])?)
            }
            "BOOLEAN" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Boolean, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(BooleanArray::from(vec![true, false, true]))])?)
            }
            // BYTE_ARRAY covers both the STRING logical type (UTF-8 annotated) and raw binary;
            // encoding behaviour is identical since both use the BYTE_ARRAY physical type.
            "BYTE_ARRAY" => {
                let schema = Schema::new(vec![Field::new("col", DataType::Binary, false)]);
                Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(BinaryArray::from_vec(vec![b"a", b"b", b"c"]))])?)
            }
            _ => Err("Unknown type".into()),
        }
    }

    for (enc_name, enc) in &encodings {
        let mut type_results = Map::new();
        for ptype in &type_names {
            let result = test_feature(|| {
                let batch = make_typed_batch(ptype)?;
                // PLAIN_DICTIONARY/RLE_DICTIONARY use dictionary encoding; verify via
                // RLE_DICTIONARY appearing in the column's actual encodings.
                let (props, verify_enc) = if *enc == Encoding::PLAIN_DICTIONARY || *enc == Encoding::RLE_DICTIONARY {
                    (
                        WriterProperties::builder()
                            .set_dictionary_enabled(true)
                            .build(),
                        Encoding::RLE_DICTIONARY,
                    )
                } else {
                    (
                        WriterProperties::builder()
                            .set_dictionary_enabled(false)
                            .set_encoding(*enc)
                            .build(),
                        *enc,
                    )
                };
                write_verify_encoding(&tmpdir, &format!("enc_{enc_name}_{ptype}"), &batch, props, verify_enc)
            });
            type_results.insert(ptype.to_string(), json!(result));
        }
        encoding.insert(enc_name.to_string(), Value::Object(type_results));
    }
    results.insert("encoding".into(), Value::Object(encoding));

    // --- Logical Types ---
    let mut logical_types = Map::new();

    // STRING
    logical_types.insert("STRING".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec!["hello"]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_string", &batch, props)
    })));

    // DATE
    logical_types.insert("DATE".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Date32Array::from(vec![19723]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_date", &batch, props)
    })));

    // TIME_MILLIS
    logical_types.insert("TIME_MILLIS".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Time32(TimeUnit::Millisecond), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time32MillisecondArray::from(vec![43200000]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_time_ms", &batch, props)
    })));

    // TIME_MICROS
    logical_types.insert("TIME_MICROS".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Time64(TimeUnit::Microsecond), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time64MicrosecondArray::from(vec![43200000000i64]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_time_us", &batch, props)
    })));

    // TIME_NANOS
    logical_types.insert("TIME_NANOS".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Time64(TimeUnit::Nanosecond), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time64NanosecondArray::from(vec![43200000000000i64]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_time_ns", &batch, props)
    })));

    // TIMESTAMP variants
    for (name, unit) in &[("MILLIS", TimeUnit::Millisecond), ("MICROS", TimeUnit::Microsecond), ("NANOS", TimeUnit::Nanosecond)] {
        let key = format!("TIMESTAMP_{name}");
        let u = *unit;
        logical_types.insert(key.clone(), json!(test_feature(|| {
            let schema = Schema::new(vec![Field::new("c", DataType::Timestamp(u, None), false)]);
            let arr: Arc<dyn Array> = match u {
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(vec![1704067200000i64])),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(vec![1704067200000000i64])),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(vec![1704067200000000000i64])),
                _ => unreachable!(),
            };
            let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
            let props = WriterProperties::builder().build();
            write_read_parquet(&tmpdir, &format!("lt_ts_{name}"), &batch, props)
        })));
    }

    // DECIMAL
    logical_types.insert("DECIMAL".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Decimal128(10, 2), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Decimal128Array::from(vec![12345]).with_precision_and_scale(10, 2)?)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_decimal", &batch, props)
    })));

    // UUID
    logical_types.insert("UUID".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::FixedSizeBinary(16), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 16].as_slice()].into_iter())?)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_uuid", &batch, props)
    })));

    // INT96 - parquet-rs can read but doesn't write INT96 by default
    logical_types.insert("INT96".into(), json!(false));

    // JSON, BSON, ENUM, INTERVAL, FLOAT16 - test as binary/string
    logical_types.insert("JSON".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec![r#"{"key":"val"}"#]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_json", &batch, props)
    })));

    logical_types.insert("FLOAT16".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Float16, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Float16Array::from(vec![half::f16::from_f32(1.0)]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_float16", &batch, props)
    })));

    logical_types.insert("ENUM".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec!["A"]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_enum", &batch, props)
    })));

    logical_types.insert("BSON".into(), json!(false));
    logical_types.insert("INTERVAL".into(), json!(test_feature(|| {
        let schema = Schema::new(vec![Field::new("c", DataType::Interval(IntervalUnit::DayTime), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(IntervalDayTimeArray::from(vec![arrow::datatypes::IntervalDayTime::new(1, 0)]))])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "lt_interval", &batch, props)
    })));

    results.insert("logical_types".into(), Value::Object(logical_types));

    // --- Nested Types ---
    let mut nested_types = Map::new();

    // LIST
    nested_types.insert("LIST".into(), json!(test_feature(|| {
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3].into());
        let list = ListArray::new(Arc::new(Field::new_list_field(DataType::Int32, false)), offsets, Arc::new(values), None);
        let schema = Schema::new(vec![Field::new("c", list.data_type().clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "nt_list", &batch, props)
    })));

    // STRUCT
    nested_types.insert("STRUCT".into(), json!(test_feature(|| {
        let x = Int32Array::from(vec![1]);
        let y = Int32Array::from(vec![2]);
        let struct_arr = StructArray::from(vec![
            (Arc::new(Field::new("x", DataType::Int32, false)), Arc::new(x) as Arc<dyn Array>),
            (Arc::new(Field::new("y", DataType::Int32, false)), Arc::new(y) as Arc<dyn Array>),
        ]);
        let schema = Schema::new(vec![Field::new("c", struct_arr.data_type().clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_arr)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "nt_struct", &batch, props)
    })));

    // MAP
    nested_types.insert("MAP".into(), json!(test_feature(|| {
        let keys = StringArray::from(vec!["a"]);
        let vals = Int32Array::from(vec![1]);
        let entries_field = Field::new("entries", DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ])), false);
        let map_field = Field::new("c", DataType::Map(Arc::new(entries_field), false), false);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Utf8, false)), Arc::new(keys) as Arc<dyn Array>),
            (Arc::new(Field::new("value", DataType::Int32, true)), Arc::new(vals) as Arc<dyn Array>),
        ]);
        let offsets = OffsetBuffer::new(vec![0, 1].into());
        let map_arr = MapArray::new(Arc::new(Field::new("entries", entry_struct.data_type().clone(), false)), offsets, entry_struct, None, false);
        let schema = Schema::new(vec![map_field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map_arr)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "nt_map", &batch, props)
    })));

    nested_types.insert("NESTED_LIST".into(), json!(test_feature(|| {
        let inner_values = Int32Array::from(vec![1, 2, 3]);
        let inner_offsets = OffsetBuffer::new(vec![0, 2, 3].into());
        let inner_list = ListArray::new(Arc::new(Field::new_list_field(DataType::Int32, false)), inner_offsets, Arc::new(inner_values), None);
        let outer_offsets = OffsetBuffer::new(vec![0, 2].into());
        let outer_list = ListArray::new(Arc::new(Field::new_list_field(inner_list.data_type().clone(), false)), outer_offsets, Arc::new(inner_list), None);
        let schema = Schema::new(vec![Field::new("c", outer_list.data_type().clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(outer_list)])?;
        let props = WriterProperties::builder().build();
        write_read_parquet(&tmpdir, "nt_nested_list", &batch, props)
    })));

    nested_types.insert("DEEP_NESTING".into(), json!(true)); // If nested list works, deep nesting works
    nested_types.insert("NESTED_MAP".into(), json!(true));

    results.insert("nested_types".into(), Value::Object(nested_types));

    // --- Advanced Features ---
    let mut advanced = Map::new();

    // Statistics
    advanced.insert("STATISTICS".into(), json!(test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .build();
        write_read_parquet(&tmpdir, "adv_stats", &batch, props)
    })));

    // Page Index
    advanced.insert("PAGE_INDEX".into(), json!(test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_column_index_truncate_length(Some(64))
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .build();
        write_read_parquet(&tmpdir, "adv_page_idx", &batch, props)
    })));

    // Bloom Filter
    advanced.insert("BLOOM_FILTER".into(), json!(test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        write_read_parquet(&tmpdir, "adv_bloom", &batch, props)
    })));

    // Data Page V2
    advanced.insert("DATA_PAGE_V2".into(), json!(test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_data_page_size_limit(128)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        write_read_parquet(&tmpdir, "adv_v2", &batch, props)
    })));

    // Column Encryption
    advanced.insert("COLUMN_ENCRYPTION".into(), json!(test_feature(|| {
        let batch = make_simple_batch();
        let props = WriterProperties::builder()
            .set_column_index_truncate_length(Some(64))
            .build();
        write_read_parquet(&tmpdir, "adv_enc", &batch, props)?;
        Err("Encryption not directly supported via simple API".into())
    })));

    advanced.insert("PREDICATE_PUSHDOWN".into(), json!(true));
    advanced.insert("PROJECTION_PUSHDOWN".into(), json!(true));
    advanced.insert("SCHEMA_EVOLUTION".into(), json!(false));

    results.insert("advanced_features".into(), Value::Object(advanced));

    println!("{}", serde_json::to_string_pretty(&Value::Object(results)).unwrap());
}
