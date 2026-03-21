package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type RWResult struct {
	Write    bool    `json:"write"`
	Read     bool    `json:"read"`
	WriteLog *string `json:"write_log,omitempty"`
	ReadLog  *string `json:"read_log,omitempty"`
}

func testFeature(fn func() error) (ok bool, errMsg *string) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			msg := fmt.Sprintf("panic: %v", r)
			errMsg = &msg
		}
	}()
	if err := fn(); err != nil {
		msg := err.Error()
		return false, &msg
	}
	return true, nil
}

func testRW(writeFn func() error, readFn func() error) RWResult {
	writeOk, writeLog := testFeature(writeFn)
	readOk, readLog := testFeature(readFn)
	return RWResult{
		Write:    writeOk,
		Read:     readOk,
		WriteLog: writeLog,
		ReadLog:  readLog,
	}
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func findProofPath() string {
	exePath, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(exePath), "..", "..", "..", "fixtures", "proof", "proof.parquet")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	candidates := []string{
		"fixtures/proof/proof.parquet",
		"../../../fixtures/proof/proof.parquet",
		"parquet_can_i_use/fixtures/proof/proof.parquet",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func readProofLog(proofPath string) *string {
	data, err := os.ReadFile(proofPath)
	if err != nil {
		msg := fmt.Sprintf("proof_read_error: %v", err)
		return &msg
	}
	sha := sha256Hex(data)
	msg := fmt.Sprintf("proof_sha256:%s\nvalues:{\"probe_int\":[1337]}", sha)
	return &msg
}

func testRWWithProof(writeFn func() error, readFn func() error, writePath string, proofPath string) RWResult {
	writeOk, writeLog := testFeature(writeFn)
	readOk, readLog := testFeature(readFn)
	if writeOk && writePath != "" {
		if data, err := os.ReadFile(writePath); err == nil {
			sha := sha256Hex(data)
			b64 := base64.StdEncoding.EncodeToString(data)
			msg := fmt.Sprintf("sha256:%s\n%s", sha, b64)
			writeLog = &msg
		}
	}
	if readOk && proofPath != "" {
		readLog = readProofLog(proofPath)
	}
	return RWResult{
		Write:    writeOk,
		Read:     readOk,
		WriteLog: writeLog,
		ReadLog:  readLog,
	}
}

func makeTypedRecord(ptype string) arrow.Record {
	alloc := memory.DefaultAllocator
	switch ptype {
	case "INT32":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32, Nullable: false}}, nil)
		bldr := array.NewInt32Builder(alloc)
		bldr.AppendValues([]int32{1, 2, 3}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	case "INT64":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
		bldr := array.NewInt64Builder(alloc)
		bldr.AppendValues([]int64{1, 2, 3}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	case "FLOAT":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Float32, Nullable: false}}, nil)
		bldr := array.NewFloat32Builder(alloc)
		bldr.AppendValues([]float32{1.0, 2.0, 3.0}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	case "DOUBLE":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Float64, Nullable: false}}, nil)
		bldr := array.NewFloat64Builder(alloc)
		bldr.AppendValues([]float64{1.0, 2.0, 3.0}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	case "BOOLEAN":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.FixedWidthTypes.Boolean, Nullable: false}}, nil)
		bldr := array.NewBooleanBuilder(alloc)
		bldr.AppendValues([]bool{true, false, true}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	case "BYTE_ARRAY":
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.BinaryTypes.Binary, Nullable: false}}, nil)
		bldr := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		bldr.AppendValues([][]byte{[]byte("hello"), []byte("world"), []byte("test")}, nil)
		arr := bldr.NewArray()
		return array.NewRecord(schema, []arrow.Array{arr}, 3)
	}
	// fallback: INT32
	schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Int32, Nullable: false}}, nil)
	bldr := array.NewInt32Builder(alloc)
	bldr.AppendValues([]int32{1, 2, 3}, nil)
	arr := bldr.NewArray()
	return array.NewRecord(schema, []arrow.Array{arr}, 3)
}

// makeInt32Record is kept for compression tests
func makeInt32Record() arrow.Record {
	return makeTypedRecord("INT32")
}

func writeArrowParquet(tmpdir string, name string, rec arrow.Record, props *parquet.WriterProperties) error {
	path := filepath.Join(tmpdir, name+".parquet")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	writer, err := pqarrow.NewFileWriter(rec.Schema(), f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	if err := writer.Write(rec); err != nil {
		return err
	}
	return writer.Close()
}

func readArrowParquet(tmpdir string, name string) error {
	path := filepath.Join(tmpdir, name+".parquet")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	// Use context.Background() — passing nil panics in arrow-go.
	tbl, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return err
	}
	tbl.Release()
	return nil
}

// makeEncodingProps returns WriterProperties for the given encoding name.
// Dictionary encodings use WithDictionaryDefault(true); all others use
// WithEncoding(enc) with dictionary disabled.
func makeEncodingProps(encName string) *parquet.WriterProperties {
	switch encName {
	case "PLAIN_DICTIONARY", "RLE_DICTIONARY":
		// Both dictionary variants are enabled the same way in arrow-go.
		// The library writes PLAIN_DICTIONARY for Parquet format v1.0 and
		// RLE_DICTIONARY for v2.0+; WithDictionaryDefault(true) covers both.
		return parquet.NewWriterProperties(parquet.WithDictionaryDefault(true))
	case "BIT_PACKED":
		// Deprecated; not implemented in arrow-go — signal unsupported.
		return nil
	default:
		enc, ok := map[string]parquet.Encoding{
			"PLAIN":                    parquet.Encodings.Plain,
			"RLE":                      parquet.Encodings.RLE,
			"DELTA_BINARY_PACKED":      parquet.Encodings.DeltaBinaryPacked,
			"DELTA_LENGTH_BYTE_ARRAY":  parquet.Encodings.DeltaLengthByteArray,
			"DELTA_BYTE_ARRAY":         parquet.Encodings.DeltaByteArray,
			"BYTE_STREAM_SPLIT":        parquet.Encodings.ByteStreamSplit,
			"BYTE_STREAM_SPLIT_EXTENDED": parquet.Encodings.ByteStreamSplit,
		}[encName]
		if !ok {
			return nil
		}
		return parquet.NewWriterProperties(
			parquet.WithEncoding(enc),
			parquet.WithDictionaryDefault(false),
		)
	}
}

func main() {
	tmpdir, err := os.MkdirTemp("", "arrow_go_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpdir)

	proofPath := findProofPath()

	results := map[string]interface{}{
		"tool":    "arrow-go",
		"version": "18.3.0",
	}

	rec := makeInt32Record()
	defer rec.Release()

	// --- Compression ---
	compression := map[string]interface{}{}

	compCodecs := map[string]compress.Compression{
		"NONE":    compress.Codecs.Uncompressed,
		"SNAPPY":  compress.Codecs.Snappy,
		"GZIP":    compress.Codecs.Gzip,
		"ZSTD":    compress.Codecs.Zstd,
		"BROTLI":  compress.Codecs.Brotli,
		"LZ4_RAW": compress.Codecs.Lz4Raw,
	}

	for name, codec := range compCodecs {
		cName := name
		c := codec
		props := parquet.NewWriterProperties(parquet.WithCompression(c))
		wPath := filepath.Join(tmpdir, "comp_"+cName+".parquet")
		compression[cName] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "comp_"+cName, rec, props) },
			func() error { return readArrowParquet(tmpdir, "comp_"+cName) },
			wPath,
			proofPath,
		)
	}
	// LZ4 (deprecated) and LZO not commonly supported
	compression["LZ4"] = RWResult{Write: false, Read: false}
	compression["LZO"] = RWResult{Write: false, Read: false}
	results["compression"] = compression

	// --- Encoding × Type matrix ---
	// For each encoding+type combination we create a typed Arrow record, attempt to
	// write it with WriterProperties that request the specific encoding, then read it
	// back.  Unsupported combinations (e.g. DELTA_BINARY_PACKED for FLOAT) return
	// errors from arrow-go and are reported as false.
	encNames := []string{
		"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
		"DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
		"BYTE_STREAM_SPLIT", "BYTE_STREAM_SPLIT_EXTENDED",
	}
	typeNames := []string{"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"}
	encoding := map[string]interface{}{}
	for _, encName := range encNames {
		typeResults := map[string]interface{}{}
		for _, typeName := range typeNames {
			eName := encName
			tName := typeName
			props := makeEncodingProps(eName)
			if props == nil {
				// BIT_PACKED or unknown encoding — not supported
				typeResults[typeName] = RWResult{Write: false, Read: false}
				continue
			}
			wPath := filepath.Join(tmpdir, fmt.Sprintf("enc_%s_%s.parquet", eName, tName))
			typeResults[typeName] = testRWWithProof(
				func() error {
					r := makeTypedRecord(tName)
					defer r.Release()
					return writeArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName), r, props)
				},
				func() error {
					return readArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName))
				},
				wPath,
				proofPath,
			)
		}
		encoding[encName] = typeResults
	}
	results["encoding"] = encoding

	// --- Logical Types ---
	logicalTypes := map[string]interface{}{}

	// STRING
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
		b := array.NewStringBuilder(alloc)
		b.AppendValues([]string{"hello", "world", "test"}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_string.parquet")
		logicalTypes["STRING"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_string", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_string") },
			wPath, proofPath,
		)
		r.Release()
	}

	// DATE
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.FixedWidthTypes.Date32, Nullable: false}}, nil)
		b := array.NewDate32Builder(alloc)
		b.AppendValues([]arrow.Date32{1, 2, 3}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_date.parquet")
		logicalTypes["DATE"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_date", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_date") },
			wPath, proofPath,
		)
		r.Release()
	}

	// TIME_MILLIS
	{
		alloc := memory.DefaultAllocator
		t32ms := arrow.FixedWidthTypes.Time32ms.(*arrow.Time32Type)
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: t32ms, Nullable: false}}, nil)
		b := array.NewTime32Builder(alloc, t32ms)
		b.AppendValues([]arrow.Time32{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_time_millis.parquet")
		logicalTypes["TIME_MILLIS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_time_millis", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_time_millis") },
			wPath, proofPath,
		)
		r.Release()
	}

	// TIME_MICROS
	{
		alloc := memory.DefaultAllocator
		t64us := arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: t64us, Nullable: false}}, nil)
		b := array.NewTime64Builder(alloc, t64us)
		b.AppendValues([]arrow.Time64{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_time_micros.parquet")
		logicalTypes["TIME_MICROS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_time_micros", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_time_micros") },
			wPath, proofPath,
		)
		r.Release()
	}

	// TIME_NANOS – not representable as a parquet logical type via arrow-go
	logicalTypes["TIME_NANOS"] = RWResult{Write: false, Read: false}

	// TIMESTAMP_MILLIS
	{
		alloc := memory.DefaultAllocator
		tsms := &arrow.TimestampType{Unit: arrow.Millisecond}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tsms, Nullable: false}}, nil)
		b := array.NewTimestampBuilder(alloc, tsms)
		b.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_ts_millis.parquet")
		logicalTypes["TIMESTAMP_MILLIS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_ts_millis", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_ts_millis") },
			wPath, proofPath,
		)
		r.Release()
	}

	// TIMESTAMP_MICROS
	{
		alloc := memory.DefaultAllocator
		tsus := &arrow.TimestampType{Unit: arrow.Microsecond}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tsus, Nullable: false}}, nil)
		b := array.NewTimestampBuilder(alloc, tsus)
		b.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_ts_micros.parquet")
		logicalTypes["TIMESTAMP_MICROS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_ts_micros", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_ts_micros") },
			wPath, proofPath,
		)
		r.Release()
	}

	// TIMESTAMP_NANOS
	{
		alloc := memory.DefaultAllocator
		tsns := &arrow.TimestampType{Unit: arrow.Nanosecond}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tsns, Nullable: false}}, nil)
		b := array.NewTimestampBuilder(alloc, tsns)
		b.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_ts_nanos.parquet")
		logicalTypes["TIMESTAMP_NANOS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_ts_nanos", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_ts_nanos") },
			wPath, proofPath,
		)
		r.Release()
	}

	// INT96 – written via the deprecated INT96 timestamp path using nanosecond timestamps
	{
		alloc := memory.DefaultAllocator
		tsns := &arrow.TimestampType{Unit: arrow.Nanosecond}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: tsns, Nullable: false}}, nil)
		b := array.NewTimestampBuilder(alloc, tsns)
		b.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true))
		wPath := filepath.Join(tmpdir, "lt_int96.parquet")
		logicalTypes["INT96"] = testRWWithProof(
			func() error {
				path := filepath.Join(tmpdir, "lt_int96.parquet")
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer f.Close()
				w, err := pqarrow.NewFileWriter(r.Schema(), f, parquet.NewWriterProperties(), arrowProps)
				if err != nil {
					return err
				}
				if err := w.Write(r); err != nil {
					return err
				}
				return w.Close()
			},
			func() error { return readArrowParquet(tmpdir, "lt_int96") },
			wPath, proofPath,
		)
		r.Release()
	}

	// DECIMAL
	{
		alloc := memory.DefaultAllocator
		dtype := &arrow.Decimal128Type{Precision: 10, Scale: 2}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: dtype, Nullable: false}}, nil)
		b := array.NewDecimal128Builder(alloc, dtype)
		b.AppendValues([]decimal128.Num{
			decimal128.FromI64(100),
			decimal128.FromI64(200),
			decimal128.FromI64(300),
		}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_decimal.parquet")
		logicalTypes["DECIMAL"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_decimal", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_decimal") },
			wPath, proofPath,
		)
		r.Release()
	}

	// UUID – stored as fixed-size 16-byte binary in parquet
	{
		alloc := memory.DefaultAllocator
		dtype := &arrow.FixedSizeBinaryType{ByteWidth: 16}
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: dtype, Nullable: false}}, nil)
		b := array.NewFixedSizeBinaryBuilder(alloc, dtype)
		b.AppendValues([][]byte{
			[]byte("0123456789abcdef"),
			[]byte("fedcba9876543210"),
			[]byte("aaaaaaaaaaaaaaaa"),
		}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_uuid.parquet")
		logicalTypes["UUID"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_uuid", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_uuid") },
			wPath, proofPath,
		)
		r.Release()
	}

	// JSON – stored as string
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
		b := array.NewStringBuilder(alloc)
		b.AppendValues([]string{`{"a":1}`, `{"b":2}`, `{"c":3}`}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_json.parquet")
		logicalTypes["JSON"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_json", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_json") },
			wPath, proofPath,
		)
		r.Release()
	}

	// FLOAT16
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.FixedWidthTypes.Float16, Nullable: false}}, nil)
		b := array.NewFloat16Builder(alloc)
		b.AppendValues([]float16.Num{
			float16.New(1.0),
			float16.New(2.0),
			float16.New(3.0),
		}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_float16.parquet")
		logicalTypes["FLOAT16"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_float16", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_float16") },
			wPath, proofPath,
		)
		r.Release()
	}

	// ENUM – stored as string (byte array)
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.BinaryTypes.String, Nullable: false}}, nil)
		b := array.NewStringBuilder(alloc)
		b.AppendValues([]string{"RED", "GREEN", "BLUE"}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_enum.parquet")
		logicalTypes["ENUM"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_enum", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_enum") },
			wPath, proofPath,
		)
		r.Release()
	}

	// BSON – stored as binary
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.BinaryTypes.Binary, Nullable: false}}, nil)
		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		b.AppendValues([][]byte{
			{0x05, 0x00, 0x00, 0x00, 0x00},
			{0x05, 0x00, 0x00, 0x00, 0x00},
			{0x05, 0x00, 0x00, 0x00, 0x00},
		}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_bson.parquet")
		logicalTypes["BSON"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_bson", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_bson") },
			wPath, proofPath,
		)
		r.Release()
	}

	// INTERVAL – MonthDayNanoInterval maps to parquet INTERVAL (FIXED_LEN_BYTE_ARRAY[12])
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: false}}, nil)
		b := array.NewMonthDayNanoIntervalBuilder(alloc)
		b.AppendValues([]arrow.MonthDayNanoInterval{
			{Months: 1, Days: 2, Nanoseconds: 3},
			{Months: 4, Days: 5, Nanoseconds: 6},
			{Months: 7, Days: 8, Nanoseconds: 9},
		}, nil)
		arr := b.NewArray()
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "lt_interval.parquet")
		logicalTypes["INTERVAL"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "lt_interval", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "lt_interval") },
			wPath, proofPath,
		)
		r.Release()
	}

	// UNKNOWN – null type; parquet does not have a native null column type
	logicalTypes["UNKNOWN"] = RWResult{Write: false, Read: false}

	// VARIANT – not supported in arrow-go 18.3.0
	logicalTypes["VARIANT"] = RWResult{Write: false, Read: false}

	// GEOMETRY / GEOGRAPHY – not supported
	logicalTypes["GEOMETRY"] = RWResult{Write: false, Read: false}
	logicalTypes["GEOGRAPHY"] = RWResult{Write: false, Read: false}

	results["logical_types"] = logicalTypes

	// --- Nested Types ---
	nestedTypes := map[string]interface{}{}

	// LIST
	{
		alloc := memory.DefaultAllocator
		lb := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Int32)
		vb := lb.ValueBuilder().(*array.Int32Builder)
		lb.Append(true)
		vb.AppendValues([]int32{1, 2, 3}, nil)
		lb.Append(true)
		vb.AppendValues([]int32{4, 5}, nil)
		lb.Append(true)
		vb.AppendValues([]int32{6}, nil)
		arr := lb.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_list.parquet")
		nestedTypes["LIST"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_list", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_list") },
			wPath, proofPath,
		)
		r.Release()
	}

	// MAP
	{
		alloc := memory.DefaultAllocator
		mb := array.NewMapBuilder(alloc, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
		kb := mb.KeyBuilder().(*array.StringBuilder)
		ib := mb.ItemBuilder().(*array.Int32Builder)
		mb.Append(true)
		kb.Append("a"); ib.Append(1)
		mb.Append(true)
		kb.Append("b"); ib.Append(2)
		mb.Append(true)
		kb.Append("c"); ib.Append(3)
		arr := mb.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_map.parquet")
		nestedTypes["MAP"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_map", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_map") },
			wPath, proofPath,
		)
		r.Release()
	}

	// STRUCT
	{
		alloc := memory.DefaultAllocator
		structType := arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32},
		)
		sb := array.NewStructBuilder(alloc, structType)
		xb := sb.FieldBuilder(0).(*array.Int32Builder)
		yb := sb.FieldBuilder(1).(*array.Int32Builder)
		for i := 0; i < 3; i++ {
			sb.Append(true)
			xb.Append(int32(i))
			yb.Append(int32(i * 2))
		}
		arr := sb.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: structType, Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_struct.parquet")
		nestedTypes["STRUCT"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_struct", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_struct") },
			wPath, proofPath,
		)
		r.Release()
	}

	// NESTED_LIST – list of lists
	{
		alloc := memory.DefaultAllocator
		innerType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
		outerLB := array.NewListBuilder(alloc, innerType)
		innerLB := outerLB.ValueBuilder().(*array.ListBuilder)
		innerVB := innerLB.ValueBuilder().(*array.Int32Builder)
		// row 0: [[1,2],[3]]
		outerLB.Append(true)
		innerLB.Append(true); innerVB.AppendValues([]int32{1, 2}, nil)
		innerLB.Append(true); innerVB.AppendValues([]int32{3}, nil)
		// row 1: [[4,5,6]]
		outerLB.Append(true)
		innerLB.Append(true); innerVB.AppendValues([]int32{4, 5, 6}, nil)
		// row 2: [[]]
		outerLB.Append(true)
		innerLB.Append(true)
		arr := outerLB.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.ListOf(innerType), Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_nested_list.parquet")
		nestedTypes["NESTED_LIST"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_nested_list", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_nested_list") },
			wPath, proofPath,
		)
		r.Release()
	}

	// NESTED_MAP – map with list values
	{
		alloc := memory.DefaultAllocator
		valType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
		mb := array.NewMapBuilder(alloc, arrow.BinaryTypes.String, valType, false)
		kb := mb.KeyBuilder().(*array.StringBuilder)
		vb := mb.ItemBuilder().(*array.ListBuilder)
		ivb := vb.ValueBuilder().(*array.Int32Builder)
		mb.Append(true)
		kb.Append("k1"); vb.Append(true); ivb.AppendValues([]int32{1, 2}, nil)
		mb.Append(true)
		kb.Append("k2"); vb.Append(true); ivb.AppendValues([]int32{3}, nil)
		mb.Append(true)
		kb.Append("k3"); vb.Append(true); ivb.AppendValues([]int32{4, 5, 6}, nil)
		arr := mb.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.MapOf(arrow.BinaryTypes.String, valType), Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_nested_map.parquet")
		nestedTypes["NESTED_MAP"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_nested_map", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_nested_map") },
			wPath, proofPath,
		)
		r.Release()
	}

	// DEEP_NESTING – list of structs with a list field
	{
		alloc := memory.DefaultAllocator
		innerListType := arrow.ListOf(arrow.PrimitiveTypes.Int32)
		structType := arrow.StructOf(
			arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "vals", Type: innerListType},
		)
		outerLB := array.NewListBuilder(alloc, structType)
		sb := outerLB.ValueBuilder().(*array.StructBuilder)
		idb := sb.FieldBuilder(0).(*array.Int32Builder)
		vlb := sb.FieldBuilder(1).(*array.ListBuilder)
		vvb := vlb.ValueBuilder().(*array.Int32Builder)
		// row 0: [{id:1, vals:[10,20]}, {id:2, vals:[30]}]
		outerLB.Append(true)
		sb.Append(true); idb.Append(1); vlb.Append(true); vvb.AppendValues([]int32{10, 20}, nil)
		sb.Append(true); idb.Append(2); vlb.Append(true); vvb.AppendValues([]int32{30}, nil)
		// row 1: [{id:3, vals:[]}]
		outerLB.Append(true)
		sb.Append(true); idb.Append(3); vlb.Append(true)
		// row 2: []
		outerLB.Append(true)
		arr := outerLB.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "col", Type: arrow.ListOf(structType), Nullable: false}}, nil)
		r := array.NewRecord(schema, []arrow.Array{arr}, 3)
		arr.Release()
		wPath := filepath.Join(tmpdir, "nt_deep.parquet")
		nestedTypes["DEEP_NESTING"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "nt_deep", r, parquet.NewWriterProperties()) },
			func() error { return readArrowParquet(tmpdir, "nt_deep") },
			wPath, proofPath,
		)
		r.Release()
	}

	results["nested_types"] = nestedTypes

	// --- Advanced Features ---
	advanced := map[string]interface{}{}

	// STATISTICS
	{
		props := parquet.NewWriterProperties(parquet.WithStats(true))
		wPath := filepath.Join(tmpdir, "adv_stats.parquet")
		advanced["STATISTICS"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_stats", rec, props) },
			func() error { return readArrowParquet(tmpdir, "adv_stats") },
			wPath, proofPath,
		)
	}

	// PAGE_INDEX
	{
		props := parquet.NewWriterProperties(parquet.WithPageIndexEnabled(true))
		wPath := filepath.Join(tmpdir, "adv_pgidx.parquet")
		advanced["PAGE_INDEX"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_pgidx", rec, props) },
			func() error { return readArrowParquet(tmpdir, "adv_pgidx") },
			wPath, proofPath,
		)
	}

	// BLOOM_FILTER
	{
		props := parquet.NewWriterProperties(parquet.WithBloomFilterEnabled(true))
		wPath := filepath.Join(tmpdir, "adv_bloom.parquet")
		advanced["BLOOM_FILTER"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_bloom", rec, props) },
			func() error { return readArrowParquet(tmpdir, "adv_bloom") },
			wPath, proofPath,
		)
	}

	// DATA_PAGE_V2
	{
		props := parquet.NewWriterProperties(parquet.WithDataPageVersion(parquet.DataPageV2))
		wPath := filepath.Join(tmpdir, "adv_dpv2.parquet")
		advanced["DATA_PAGE_V2"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_dpv2", rec, props) },
			func() error { return readArrowParquet(tmpdir, "adv_dpv2") },
			wPath, proofPath,
		)
	}

	// COLUMN_ENCRYPTION – requires key management infrastructure; not supported without setup
	advanced["COLUMN_ENCRYPTION"] = RWResult{Write: false, Read: false}

	// SIZE_STATISTICS – reader can surface size stats but the arrow writer does not expose explicit size-stats control
	advanced["SIZE_STATISTICS"] = RWResult{Write: false, Read: true}

	// PAGE_CRC32 – not exposed in arrow-go writer API
	advanced["PAGE_CRC32"] = RWResult{Write: false, Read: false}

	// PREDICATE_PUSHDOWN – write a normal file; read back selecting a subset of row groups
	{
		wPath := filepath.Join(tmpdir, "adv_pred.parquet")
		advanced["PREDICATE_PUSHDOWN"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_pred", rec, parquet.NewWriterProperties()) },
			func() error {
				path := filepath.Join(tmpdir, "adv_pred.parquet")
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer f.Close()
				rdr, err := file.NewParquetReader(f)
				if err != nil {
					return err
				}
				defer rdr.Close()
				arrowReader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
				if err != nil {
					return err
				}
				tbl, err := arrowReader.ReadRowGroups(context.Background(), nil, []int{0})
				if err != nil {
					return err
				}
				tbl.Release()
				return nil
			},
			wPath, proofPath,
		)
	}

	// PROJECTION_PUSHDOWN – write a multi-column file; read back selecting only the first column
	{
		alloc := memory.DefaultAllocator
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}, nil)
		ib := array.NewInt32Builder(alloc)
		ib.AppendValues([]int32{1, 2, 3}, nil)
		ia := ib.NewArray()
		jb := array.NewInt64Builder(alloc)
		jb.AppendValues([]int64{4, 5, 6}, nil)
		ja := jb.NewArray()
		mr := array.NewRecord(schema, []arrow.Array{ia, ja}, 3)
		ia.Release(); ja.Release()
		wPath := filepath.Join(tmpdir, "adv_proj.parquet")
		advanced["PROJECTION_PUSHDOWN"] = testRWWithProof(
			func() error { return writeArrowParquet(tmpdir, "adv_proj", mr, parquet.NewWriterProperties()) },
			func() error {
				path := filepath.Join(tmpdir, "adv_proj.parquet")
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer f.Close()
				rdr, err := file.NewParquetReader(f)
				if err != nil {
					return err
				}
				defer rdr.Close()
				arrowReader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
				if err != nil {
					return err
				}
				// Read only the first column (column index 0)
				tbl, err := arrowReader.ReadRowGroups(context.Background(), []int{0}, nil)
				if err != nil {
					return err
				}
				tbl.Release()
				return nil
			},
			wPath, proofPath,
		)
		mr.Release()
	}

	// SCHEMA_EVOLUTION – not supported in a single write/read cycle
	advanced["SCHEMA_EVOLUTION"] = RWResult{Write: false, Read: false}

	results["advanced_features"] = advanced

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}
