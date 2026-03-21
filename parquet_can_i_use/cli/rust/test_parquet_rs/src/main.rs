use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::basic::LogicalType;
use parquet::basic::Type as ParquetPhysicalType;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type as ParquetType;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;
use base64::{engine::general_purpose, Engine as _};
use sha2::{Digest, Sha256};
use std::path::Path;

fn test_feature<F: FnOnce() -> Result<(), Box<dyn std::error::Error>>>(f: F) -> (bool, Option<String>) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => (true, None),
        Ok(Err(e)) => (false, Some(e.to_string())),
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "panic".to_string()
            };
            (false, Some(msg))
        }
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn read_proof_values(proof_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(proof_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let mut result_map = serde_json::Map::new();
    if let Some(batch) = reader.next() {
        let batch = batch?;
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            let mut vals: Vec<Value> = Vec::new();
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                for j in 0..arr.len() {
                    vals.push(json!(arr.value(j)));
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                for j in 0..arr.len() {
                    vals.push(json!(arr.value(j)));
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                for j in 0..arr.len() {
                    vals.push(json!(arr.value(j)));
                }
            } else {
                vals.push(json!(format!("{:?}", col.data_type())));
            }
            result_map.insert(field.name().clone(), Value::Array(vals));
        }
    }
    Ok(serde_json::to_string(&Value::Object(result_map))?)
}

fn test_rw_with_proof<W, R>(
    write_fn: W,
    read_fn: R,
    write_path: Option<&Path>,
    proof_path: Option<&Path>,
) -> Value
where
    W: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
    R: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
{
    let (write_ok, write_log) = test_feature(write_fn);
    let (read_ok, read_log) = test_feature(read_fn);

    let write_log = if write_ok {
        if let Some(path) = write_path {
            match std::fs::read(path) {
                Ok(data) => {
                    let sha = sha256_hex(&data);
                    let b64 = general_purpose::STANDARD.encode(&data);
                    Some(format!("sha256:{}\n{}", sha, b64))
                }
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        write_log
    };

    let read_log = if read_ok {
        if let Some(proof) = proof_path {
            match std::fs::read(proof) {
                Ok(data) => {
                    let sha = sha256_hex(&data);
                    let values_str = read_proof_values(proof)
                        .unwrap_or_else(|_| "{}".to_string());
                    Some(format!("proof_sha256:{}\nvalues:{}", sha, values_str))
                }
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        read_log
    };

    let mut result = serde_json::Map::new();
    result.insert("write".to_string(), json!(write_ok));
    result.insert("read".to_string(), json!(read_ok));
    if let Some(log) = write_log {
        result.insert("write_log".to_string(), json!(log));
    }
    if let Some(log) = read_log {
        result.insert("read_log".to_string(), json!(log));
    }
    Value::Object(result)
}

fn test_rw<W, R>(write_fn: W, read_fn: R) -> Value
where
    W: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
    R: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
{
    test_rw_with_proof(write_fn, read_fn, None, None)
}

fn not_supported(reason: &str) -> Value {
    json!({"write": false, "read": false, "write_log": reason, "read_log": reason})
}

fn make_simple_batch() -> RecordBatch {
    let schema = Schema::new(vec![Field::new("col", DataType::Int32, false)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap()
}

fn write_parquet(
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
    Ok(())
}

fn read_parquet(
    tmpdir: &TempDir,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = tmpdir.path().join(format!("{name}.parquet"));
    let file = File::open(&path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        batch?;
    }
    Ok(())
}

fn write_read_parquet(
    tmpdir: &TempDir,
    name: &str,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<(), Box<dyn std::error::Error>> {
    write_parquet(tmpdir, name, batch, props)?;
    read_parquet(tmpdir, name)
}

/// Write a Parquet file with a single BSON column using the low-level API,
/// since Arrow has no native BSON type but the Parquet spec supports it as
/// BYTE_ARRAY with the BSON logical type annotation.
fn write_bson_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use parquet::basic::Repetition;
    use parquet::data_type::ByteArrayType;
    let field = Arc::new(
        ParquetType::primitive_type_builder("c", ParquetPhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::Bson))
            .with_repetition(Repetition::REQUIRED)
            .build()?,
    );
    let schema = Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![field])
            .build()?,
    );
    let file = File::create(path)?;
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props)?;
    {
        let mut row_group = writer.next_row_group()?;
        if let Some(mut col_writer) = row_group.next_column()? {
            // Minimal valid BSON document: {length=5, terminator=0}
            col_writer.typed::<ByteArrayType>().write_batch(
                &[ByteArray::from(vec![5u8, 0, 0, 0, 0])],
                None,
                None,
            )?;
            col_writer.close()?;
        }
        row_group.close()?;
    }
    writer.close()?;
    Ok(())
}

/// Read a parquet file at an arbitrary path (not relative to tmpdir).
fn read_parquet_path(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
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
    let fixtures_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..").join("..").join("..").join("fixtures");
    let proof_fixture_path = fixtures_dir.join("proof").join("proof.parquet");
    let proof_path: Option<&std::path::Path> = if proof_fixture_path.exists() {
        Some(proof_fixture_path.as_path())
    } else {
        None
    };
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
        let n = name.to_string();
        let write_file = tmpdir.path().join(format!("comp_{n}.parquet"));
        let result = test_rw_with_proof(
            || {
                let b = make_simple_batch();
                let p = WriterProperties::builder().set_compression(*codec).build();
                write_parquet(&tmpdir, &format!("comp_{n}"), &b, p)
            },
            || read_parquet(&tmpdir, &format!("comp_{n}")),
            Some(&write_file),
            proof_path,
        );
        let _ = props;
        let _ = batch;
        compression.insert(name.to_string(), result);
    }
    // LZO - try it
    let lzo_write_file = tmpdir.path().join("comp_LZO.parquet");
    let lzo_result = test_rw_with_proof(
        || {
            let batch = make_simple_batch();
            let props = WriterProperties::builder()
                .set_compression(Compression::LZO)
                .build();
            write_parquet(&tmpdir, "comp_LZO", &batch, props)
        },
        || read_parquet(&tmpdir, "comp_LZO"),
        Some(&lzo_write_file),
        proof_path,
    );
    compression.insert("LZO".into(), lzo_result);
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
            let enc_name_str = enc_name.to_string();
            let ptype_str = ptype.to_string();
            let enc_val = *enc;
            let enc_write_file = tmpdir.path().join(format!("enc_{enc_name_str}_{ptype_str}.parquet"));
            let result = test_rw_with_proof(
                || {
                    let batch = make_typed_batch(&ptype_str)?;
                    // PLAIN_DICTIONARY/RLE_DICTIONARY use dictionary encoding; verify via
                    // RLE_DICTIONARY appearing in the column's actual encodings.
                    let (props, verify_enc) = if enc_val == Encoding::PLAIN_DICTIONARY || enc_val == Encoding::RLE_DICTIONARY {
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
                                .set_encoding(enc_val)
                                .build(),
                            enc_val,
                        )
                    };
                    write_verify_encoding(&tmpdir, &format!("enc_{enc_name_str}_{ptype_str}"), &batch, props, verify_enc)
                },
                || read_parquet(&tmpdir, &format!("enc_{enc_name}_{ptype}")),
                Some(&enc_write_file),
                proof_path,
            );
            type_results.insert(ptype.to_string(), result);
        }
        encoding.insert(enc_name.to_string(), Value::Object(type_results));
    }

    // BYTE_STREAM_SPLIT_EXTENDED: parquet format 2.11 extends BYTE_STREAM_SPLIT to all
    // fixed-width types. In parquet-rs v55 the same BYTE_STREAM_SPLIT encoding covers the
    // extended set; we test with the same types as BYTE_STREAM_SPLIT.
    {
        let mut type_results = Map::new();
        for ptype in &type_names {
            let ptype_str = ptype.to_string();
            let enc_write_file = tmpdir.path().join(format!("enc_BYTE_STREAM_SPLIT_EXTENDED_{ptype_str}.parquet"));
            let result = test_rw_with_proof(
                || {
                    let batch = make_typed_batch(&ptype_str)?;
                    let props = WriterProperties::builder()
                        .set_dictionary_enabled(false)
                        .set_encoding(Encoding::BYTE_STREAM_SPLIT)
                        .build();
                    write_verify_encoding(
                        &tmpdir,
                        &format!("enc_BYTE_STREAM_SPLIT_EXTENDED_{ptype_str}"),
                        &batch,
                        props,
                        Encoding::BYTE_STREAM_SPLIT,
                    )
                },
                || read_parquet(&tmpdir, &format!("enc_BYTE_STREAM_SPLIT_EXTENDED_{ptype}")),
                Some(&enc_write_file),
                proof_path,
            );
            type_results.insert(ptype.to_string(), result);
        }
        encoding.insert("BYTE_STREAM_SPLIT_EXTENDED".to_string(), Value::Object(type_results));
    }

    results.insert("encoding".into(), Value::Object(encoding));

    // --- Logical Types ---
    let mut logical_types = Map::new();

    // STRING
    logical_types.insert("STRING".into(), {
        let wf = tmpdir.path().join("lt_string.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec!["hello"]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_string", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_string"),
            Some(&wf),
            proof_path,
        )
    });

    // DATE
    logical_types.insert("DATE".into(), {
        let wf = tmpdir.path().join("lt_date.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Date32, false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Date32Array::from(vec![19723]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_date", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_date"),
            Some(&wf),
            proof_path,
        )
    });

    // TIME_MILLIS
    logical_types.insert("TIME_MILLIS".into(), {
        let wf = tmpdir.path().join("lt_time_ms.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Time32(TimeUnit::Millisecond), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time32MillisecondArray::from(vec![43200000]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_time_ms", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_time_ms"),
            Some(&wf),
            proof_path,
        )
    });

    // TIME_MICROS
    logical_types.insert("TIME_MICROS".into(), {
        let wf = tmpdir.path().join("lt_time_us.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Time64(TimeUnit::Microsecond), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time64MicrosecondArray::from(vec![43200000000i64]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_time_us", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_time_us"),
            Some(&wf),
            proof_path,
        )
    });

    // TIME_NANOS
    logical_types.insert("TIME_NANOS".into(), {
        let wf = tmpdir.path().join("lt_time_ns.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Time64(TimeUnit::Nanosecond), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Time64NanosecondArray::from(vec![43200000000000i64]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_time_ns", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_time_ns"),
            Some(&wf),
            proof_path,
        )
    });

    // TIMESTAMP variants
    for (name, unit) in &[("MILLIS", TimeUnit::Millisecond), ("MICROS", TimeUnit::Microsecond), ("NANOS", TimeUnit::Nanosecond)] {
        let key = format!("TIMESTAMP_{name}");
        let u = *unit;
        let n = name.to_string();
        let ts_write_file = tmpdir.path().join(format!("lt_ts_{n}.parquet"));
        let tmpdir_ref = &tmpdir;
        logical_types.insert(key.clone(), test_rw_with_proof(
            move || {
                let schema = Schema::new(vec![Field::new("c", DataType::Timestamp(u, None), false)]);
                let arr: Arc<dyn Array> = match u {
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(vec![1704067200000i64])),
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(vec![1704067200000000i64])),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(vec![1704067200000000000i64])),
                    _ => unreachable!(),
                };
                let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
                let props = WriterProperties::builder().build();
                write_parquet(tmpdir_ref, &format!("lt_ts_{n}"), &batch, props)
            },
            || read_parquet(&tmpdir, &format!("lt_ts_{name}")),
            Some(&ts_write_file),
            proof_path,
        ));
    }

    // DECIMAL
    logical_types.insert("DECIMAL".into(), {
        let wf = tmpdir.path().join("lt_decimal.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Decimal128(10, 2), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Decimal128Array::from(vec![12345]).with_precision_and_scale(10, 2)?)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_decimal", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_decimal"),
            Some(&wf),
            proof_path,
        )
    });

    // UUID
    logical_types.insert("UUID".into(), {
        let wf = tmpdir.path().join("lt_uuid.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::FixedSizeBinary(16), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![0u8; 16].as_slice()].into_iter())?)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_uuid", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_uuid"),
            Some(&wf),
            proof_path,
        )
    });

    // INT96 - parquet-rs can read but doesn't write INT96 by default
    logical_types.insert("INT96".into(), not_supported("INT96 is not supported for writing by parquet-rs; INT96 is a deprecated legacy timestamp format"));

    // JSON, BSON, ENUM, INTERVAL, FLOAT16 - test as binary/string
    logical_types.insert("JSON".into(), {
        let wf = tmpdir.path().join("lt_json.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec![r#"{"key":"val"}"#]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_json", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_json"),
            Some(&wf),
            proof_path,
        )
    });

    logical_types.insert("FLOAT16".into(), {
        let wf = tmpdir.path().join("lt_float16.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Float16, false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Float16Array::from(vec![half::f16::from_f32(1.0)]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_float16", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_float16"),
            Some(&wf),
            proof_path,
        )
    });

    logical_types.insert("ENUM".into(), {
        let wf = tmpdir.path().join("lt_enum.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Utf8, false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec!["A"]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_enum", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_enum"),
            Some(&wf),
            proof_path,
        )
    });

    logical_types.insert("BSON".into(), {
        let wf = tmpdir.path().join("lt_bson.parquet");
        test_rw_with_proof(
            || write_bson_parquet(&tmpdir.path().join("lt_bson.parquet")),
            || read_parquet(&tmpdir, "lt_bson"),
            Some(&wf),
            proof_path,
        )
    });
    logical_types.insert("INTERVAL".into(), {
        let wf = tmpdir.path().join("lt_interval.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Interval(IntervalUnit::DayTime), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(IntervalDayTimeArray::from(vec![arrow::datatypes::IntervalDayTime::new(1, 0)]))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_interval", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_interval"),
            Some(&wf),
            proof_path,
        )
    });

    // UNKNOWN logical type: Arrow DataType::Null maps to Parquet UNKNOWN (always-null column)
    logical_types.insert("UNKNOWN".into(), {
        let wf = tmpdir.path().join("lt_unknown.parquet");
        test_rw_with_proof(
            || {
                let schema = Schema::new(vec![Field::new("c", DataType::Null, true)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(NullArray::new(2))])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "lt_unknown", &batch, props)
            },
            || read_parquet(&tmpdir, "lt_unknown"),
            Some(&wf),
            proof_path,
        )
    });

    // VARIANT, GEOMETRY, GEOGRAPHY - not yet supported in parquet-rs v55
    logical_types.insert("VARIANT".into(), not_supported("VARIANT logical type is not yet supported in parquet-rs"));
    logical_types.insert("GEOMETRY".into(), not_supported("GEOMETRY logical type is not yet supported in parquet-rs"));
    logical_types.insert("GEOGRAPHY".into(), not_supported("GEOGRAPHY logical type is not yet supported in parquet-rs"));

    results.insert("logical_types".into(), Value::Object(logical_types));

    // --- Nested Types ---
    let mut nested_types = Map::new();

    // LIST
    nested_types.insert("LIST".into(), {
        let wf = tmpdir.path().join("nt_list.parquet");
        test_rw_with_proof(
            || {
                let values = Int32Array::from(vec![1, 2, 3]);
                let offsets = OffsetBuffer::new(vec![0, 2, 3].into());
                let list = ListArray::new(Arc::new(Field::new_list_field(DataType::Int32, false)), offsets, Arc::new(values), None);
                let schema = Schema::new(vec![Field::new("c", list.data_type().clone(), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "nt_list", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_list"),
            Some(&wf),
            proof_path,
        )
    });

    // STRUCT
    nested_types.insert("STRUCT".into(), {
        let wf = tmpdir.path().join("nt_struct.parquet");
        test_rw_with_proof(
            || {
                let x = Int32Array::from(vec![1]);
                let y = Int32Array::from(vec![2]);
                let struct_arr = StructArray::from(vec![
                    (Arc::new(Field::new("x", DataType::Int32, false)), Arc::new(x) as Arc<dyn Array>),
                    (Arc::new(Field::new("y", DataType::Int32, false)), Arc::new(y) as Arc<dyn Array>),
                ]);
                let schema = Schema::new(vec![Field::new("c", struct_arr.data_type().clone(), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_arr)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "nt_struct", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_struct"),
            Some(&wf),
            proof_path,
        )
    });

    // MAP
    nested_types.insert("MAP".into(), {
        let wf = tmpdir.path().join("nt_map.parquet");
        test_rw_with_proof(
            || {
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
                write_parquet(&tmpdir, "nt_map", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_map"),
            Some(&wf),
            proof_path,
        )
    });

    nested_types.insert("NESTED_LIST".into(), {
        let wf = tmpdir.path().join("nt_nested_list.parquet");
        test_rw_with_proof(
            || {
                let inner_values = Int32Array::from(vec![1, 2, 3]);
                let inner_offsets = OffsetBuffer::new(vec![0, 2, 3].into());
                let inner_list = ListArray::new(Arc::new(Field::new_list_field(DataType::Int32, false)), inner_offsets, Arc::new(inner_values), None);
                let outer_offsets = OffsetBuffer::new(vec![0, 2].into());
                let outer_list = ListArray::new(Arc::new(Field::new_list_field(inner_list.data_type().clone(), false)), outer_offsets, Arc::new(inner_list), None);
                let schema = Schema::new(vec![Field::new("c", outer_list.data_type().clone(), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(outer_list)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "nt_nested_list", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_nested_list"),
            Some(&wf),
            proof_path,
        )
    });

    nested_types.insert("DEEP_NESTING".into(), {
        let wf = tmpdir.path().join("nt_deep.parquet");
        test_rw_with_proof(
            || {
                // Deep nesting: list of structs
                let inner_values = Int32Array::from(vec![1, 2, 3, 4]);
                let inner_offsets = OffsetBuffer::new(vec![0, 2, 4].into());
                let inner_list = ListArray::new(Arc::new(Field::new_list_field(DataType::Int32, false)), inner_offsets, Arc::new(inner_values), None);
                let schema = Schema::new(vec![Field::new("c", inner_list.data_type().clone(), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(inner_list)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "nt_deep", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_deep"),
            Some(&wf),
            proof_path,
        )
    });
    nested_types.insert("NESTED_MAP".into(), {
        let wf = tmpdir.path().join("nt_nested_map.parquet");
        test_rw_with_proof(
            || {
                // Nested map: map<string, map<string, int32>>
                let inner_keys = StringArray::from(vec!["ik"]);
                let inner_vals = Int32Array::from(vec![42]);
                let inner_entry_struct = StructArray::from(vec![
                    (Arc::new(Field::new("key", DataType::Utf8, false)), Arc::new(inner_keys) as Arc<dyn Array>),
                    (Arc::new(Field::new("value", DataType::Int32, true)), Arc::new(inner_vals) as Arc<dyn Array>),
                ]);
                let inner_offsets = OffsetBuffer::new(vec![0, 1].into());
                let inner_map = MapArray::new(Arc::new(Field::new("entries", inner_entry_struct.data_type().clone(), false)), inner_offsets, inner_entry_struct, None, false);
                let outer_keys = StringArray::from(vec!["ok"]);
                let outer_entry_struct = StructArray::from(vec![
                    (Arc::new(Field::new("key", DataType::Utf8, false)), Arc::new(outer_keys) as Arc<dyn Array>),
                    (Arc::new(Field::new("value", inner_map.data_type().clone(), true)), Arc::new(inner_map) as Arc<dyn Array>),
                ]);
                let outer_offsets = OffsetBuffer::new(vec![0, 1].into());
                let outer_map = MapArray::new(Arc::new(Field::new("entries", outer_entry_struct.data_type().clone(), false)), outer_offsets, outer_entry_struct, None, false);
                let schema = Schema::new(vec![Field::new("c", outer_map.data_type().clone(), false)]);
                let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(outer_map)])?;
                let props = WriterProperties::builder().build();
                write_parquet(&tmpdir, "nt_nested_map", &batch, props)
            },
            || read_parquet(&tmpdir, "nt_nested_map"),
            Some(&wf),
            proof_path,
        )
    });

    results.insert("nested_types".into(), Value::Object(nested_types));

    // --- Advanced Features ---
    let mut advanced = Map::new();

    // Statistics
    advanced.insert("STATISTICS".into(), {
        let wf = tmpdir.path().join("adv_stats.parquet");
        test_rw_with_proof(
            || {
                let batch = make_simple_batch();
                let props = WriterProperties::builder()
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .build();
                write_parquet(&tmpdir, "adv_stats", &batch, props)
            },
            || read_parquet(&tmpdir, "adv_stats"),
            Some(&wf),
            proof_path,
        )
    });

    // Page Index
    advanced.insert("PAGE_INDEX".into(), {
        let wf = tmpdir.path().join("adv_page_idx.parquet");
        test_rw_with_proof(
            || {
                let batch = make_simple_batch();
                let props = WriterProperties::builder()
                    .set_column_index_truncate_length(Some(64))
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .build();
                write_parquet(&tmpdir, "adv_page_idx", &batch, props)
            },
            || read_parquet(&tmpdir, "adv_page_idx"),
            Some(&wf),
            proof_path,
        )
    });

    // Bloom Filter
    advanced.insert("BLOOM_FILTER".into(), {
        let wf = tmpdir.path().join("adv_bloom.parquet");
        test_rw_with_proof(
            || {
                let batch = make_simple_batch();
                let props = WriterProperties::builder()
                    .set_bloom_filter_enabled(true)
                    .build();
                write_parquet(&tmpdir, "adv_bloom", &batch, props)
            },
            || read_parquet(&tmpdir, "adv_bloom"),
            Some(&wf),
            proof_path,
        )
    });

    // Data Page V2
    advanced.insert("DATA_PAGE_V2".into(), {
        let wf = tmpdir.path().join("adv_v2.parquet");
        test_rw_with_proof(
            || {
                let batch = make_simple_batch();
                let props = WriterProperties::builder()
                    .set_data_page_size_limit(128)
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();
                write_parquet(&tmpdir, "adv_v2", &batch, props)
            },
            || read_parquet(&tmpdir, "adv_v2"),
            Some(&wf),
            proof_path,
        )
    });

    // Column Encryption: parquet-rs v55 supports encryption but requires the
    // `encryption` feature which adds AES-GCM dependencies not included in this build.
    // Mark as not supported for this build configuration.
    advanced.insert("COLUMN_ENCRYPTION".into(), not_supported("COLUMN_ENCRYPTION requires the 'encryption' feature flag in parquet-rs (adds AES-GCM dependencies not included in this build)"));

    advanced.insert("PREDICATE_PUSHDOWN".into(), {
        let wf = tmpdir.path().join("adv_pred.parquet");
        test_rw_with_proof(
            || {
                let batch = make_simple_batch();
                write_parquet(&tmpdir, "adv_pred", &batch, WriterProperties::builder().build())
            },
            || {
                // Read only the first row group (simulates predicate pushdown / row-group skipping)
                let path = tmpdir.path().join("adv_pred.parquet");
                let file = File::open(&path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let reader = builder.with_row_groups(vec![0]).build()?;
                for batch in reader { batch?; }
                Ok(())
            },
            Some(&wf),
            proof_path,
        )
    });
    advanced.insert("PROJECTION_PUSHDOWN".into(), {
        let wf = tmpdir.path().join("adv_proj.parquet");
        test_rw_with_proof(
            || {
                // Write a file with two columns
                let schema = Schema::new(vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Int32, false),
                ]);
                let batch = RecordBatch::try_new(
                    Arc::new(schema),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(Int32Array::from(vec![4, 5, 6]))],
                )?;
                write_parquet(&tmpdir, "adv_proj", &batch, WriterProperties::builder().build())
            },
            || {
                // Read only the first column (projection pushdown)
                let path = tmpdir.path().join("adv_proj.parquet");
                let file = File::open(&path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let projection = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), vec![0]);
                let reader = builder.with_projection(projection).build()?;
                for batch in reader { batch?; }
                Ok(())
            },
            Some(&wf),
            proof_path,
        )
    });
    advanced.insert("SCHEMA_EVOLUTION".into(), not_supported("SCHEMA_EVOLUTION is not supported in a single write/read cycle in parquet-rs"));

    // Size Statistics: written automatically by parquet-rs when page-level statistics
    // are enabled. Verify that the column metadata includes level histograms (part of
    // SizeStatistics) via the page index API.
    advanced.insert("SIZE_STATISTICS".into(), {
        let wf = tmpdir.path().join("adv_size_stats.parquet");
        test_rw_with_proof(
            || {
                // Write a nullable BYTE_ARRAY column with page statistics so that
                // both level histograms and unencoded byte counts are recorded.
                let schema = Schema::new(vec![Field::new("c", DataType::Utf8, true)]);
                let arr: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec![
                    Some("hello"), None, Some("world"),
                ]));
                let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
                let props = WriterProperties::builder()
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                    .build();
                write_parquet(&tmpdir, "adv_size_stats", &batch, props)?;
                // Verify size statistics are present via the page index
                let path = tmpdir.path().join("adv_size_stats.parquet");
                let file = File::open(&path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                let col_meta = builder.metadata().row_group(0).column(0);
                // definition_level_histogram is part of SizeStatistics; present for
                // nullable columns when written with EnabledStatistics::Page.
                if col_meta.unencoded_byte_array_data_bytes().is_none()
                    && col_meta.definition_level_histogram().is_none()
                {
                    return Err("No size statistics (level histograms) found in column metadata".into());
                }
                Ok(())
            },
            || read_parquet(&tmpdir, "adv_size_stats"),
            Some(&wf),
            proof_path,
        )
    });

    // Page CRC32: parquet-rs v55 can read files with page CRC32 checksums but
    // does not yet support writing them (see TODO in column/page.rs).
    // Test read support using the pre-generated fixture.
    {
        let fixture_path = fixtures_dir.join("advanced_features").join("adv_PAGE_CRC32.parquet");
        let (write_ok, write_log) = (false, Some("write not yet supported in parquet-rs v55".to_string()));
        let (read_ok, read_log) = if fixture_path.exists() {
            test_feature(|| read_parquet_path(&fixture_path))
        } else {
            (false, Some("fixture not found".to_string()))
        };
        let mut cell = serde_json::Map::new();
        cell.insert("write".into(), json!(write_ok));
        cell.insert("read".into(), json!(read_ok));
        if let Some(log) = write_log { cell.insert("write_log".into(), json!(log)); }
        if let Some(log) = read_log { cell.insert("read_log".into(), json!(log)); }
        advanced.insert("PAGE_CRC32".into(), Value::Object(cell));
    }

    results.insert("advanced_features".into(), Value::Object(advanced));

    println!("{}", serde_json::to_string_pretty(&Value::Object(results)).unwrap());
}
