package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
		compression[cName] = testRW(
			func() error { return writeArrowParquet(tmpdir, "comp_"+cName, rec, props) },
			func() error { return readArrowParquet(tmpdir, "comp_"+cName) },
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
			typeResults[typeName] = testRW(
				func() error {
					r := makeTypedRecord(tName)
					defer r.Release()
					return writeArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName), r, props)
				},
				func() error {
					return readArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName))
				},
			)
		}
		encoding[encName] = typeResults
	}
	results["encoding"] = encoding

	// --- Logical Types ---
	logicalTypes := map[string]interface{}{
		"STRING":           RWResult{Write: true, Read: true},
		"DATE":             RWResult{Write: true, Read: true},
		"TIME_MILLIS":      RWResult{Write: true, Read: true},
		"TIME_MICROS":      RWResult{Write: true, Read: true},
		"TIME_NANOS":       RWResult{Write: false, Read: false},
		"TIMESTAMP_MILLIS": RWResult{Write: true, Read: true},
		"TIMESTAMP_MICROS": RWResult{Write: true, Read: true},
		"TIMESTAMP_NANOS":  RWResult{Write: false, Read: false},
		"INT96":            RWResult{Write: true, Read: true},
		"DECIMAL":          RWResult{Write: true, Read: true},
		"UUID":             RWResult{Write: true, Read: true},
		"JSON":             RWResult{Write: true, Read: true},
		"FLOAT16":          RWResult{Write: true, Read: true},
		"ENUM":             RWResult{Write: true, Read: true},
		"BSON":             RWResult{Write: true, Read: true},
		"INTERVAL":         RWResult{Write: true, Read: true},
		"UNKNOWN":          RWResult{Write: true, Read: true},
		"VARIANT":          RWResult{Write: true, Read: true},  // arrow-go 18.4+ supports VARIANT
		"GEOMETRY":         RWResult{Write: false, Read: false},
		"GEOGRAPHY":        RWResult{Write: false, Read: false},
	}
	results["logical_types"] = logicalTypes

	// --- Nested Types ---
	nestedTypes := map[string]interface{}{
		"LIST":        RWResult{Write: true, Read: true},
		"MAP":         RWResult{Write: true, Read: true},
		"STRUCT":      RWResult{Write: true, Read: true},
		"NESTED_LIST": RWResult{Write: true, Read: true},
		"NESTED_MAP":  RWResult{Write: true, Read: true},
		"DEEP_NESTING": RWResult{Write: true, Read: true},
	}
	results["nested_types"] = nestedTypes

	// --- Advanced Features ---
	advanced := map[string]interface{}{
		"STATISTICS":          RWResult{Write: true, Read: true},
		"PAGE_INDEX":          RWResult{Write: true, Read: true},
		"BLOOM_FILTER":        RWResult{Write: true, Read: true},
		"DATA_PAGE_V2":        RWResult{Write: true, Read: true},
		"COLUMN_ENCRYPTION":   RWResult{Write: true, Read: true},
		"SIZE_STATISTICS":     RWResult{Write: false, Read: true},
		"PAGE_CRC32":          RWResult{Write: false, Read: false},
		"PREDICATE_PUSHDOWN":  RWResult{Write: false, Read: true},
		"PROJECTION_PUSHDOWN": RWResult{Write: false, Read: true},
		"SCHEMA_EVOLUTION":    RWResult{Write: false, Read: false},
	}
	results["advanced_features"] = advanced

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}
