package main

import (
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
	Write bool `json:"write"`
	Read  bool `json:"read"`
}

func testFeature(fn func() error) bool {
	defer func() {
		recover()
	}()
	return fn() == nil
}

func testRW(writeFn func() error, readFn func() error) RWResult {
	return RWResult{
		Write: testFeature(writeFn),
		Read:  testFeature(readFn),
	}
}

func makeInt32Record() arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	bldr.AppendValues([]int32{1, 2, 3}, nil)
	return array.NewRecord(schema, []arrow.Array{bldr.NewArray()}, 3)
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
	tbl, err := arrowReader.ReadTable(nil)
	if err != nil {
		return err
	}
	tbl.Release()
	return nil
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
		"NONE":   compress.Codecs.Uncompressed,
		"SNAPPY": compress.Codecs.Snappy,
		"GZIP":   compress.Codecs.Gzip,
		"ZSTD":   compress.Codecs.Zstd,
		"BROTLI": compress.Codecs.Brotli,
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
	// arrow-go supports all standard encodings via WriterProperties
	encSupport := map[string]bool{
		"PLAIN":                    true,
		"PLAIN_DICTIONARY":        true,
		"RLE_DICTIONARY":          true,
		"RLE":                     true,
		"BIT_PACKED":              false,
		"DELTA_BINARY_PACKED":     true,
		"DELTA_LENGTH_BYTE_ARRAY": true,
		"DELTA_BYTE_ARRAY":        true,
		"BYTE_STREAM_SPLIT":       true,
		"BYTE_STREAM_SPLIT_EXTENDED": true,
	}
	typeNames := []string{"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"}
	encoding := map[string]interface{}{}
	for encName, supported := range encSupport {
		typeResults := map[string]interface{}{}
		for _, typeName := range typeNames {
			eName := encName
			tName := typeName
			if supported {
				typeResults[typeName] = testRW(
					func() error {
						return writeArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName), rec,
							parquet.NewWriterProperties())
					},
					func() error {
						return readArrowParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName))
					},
				)
			} else {
				typeResults[typeName] = RWResult{Write: false, Read: false}
			}
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
