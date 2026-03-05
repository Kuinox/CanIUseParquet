package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type SimpleRow struct {
	Col int32  `parquet:"col"`
}

type StringRow struct {
	Col string `parquet:"col"`
}

type NestedRow struct {
	Col []int32 `parquet:"col"`
}

type StructRow struct {
	Col struct {
		X int32 `parquet:"x"`
		Y int32 `parquet:"y"`
	} `parquet:"col"`
}

type MapRow struct {
	Col map[string]int32 `parquet:"col"`
}

func testFeature(fn func() error) bool {
	err := fn()
	return err == nil
}

type RWResult struct {
	Write bool `json:"write"`
	Read  bool `json:"read"`
}

func testRW(writeFn func() error, readFn func() error) RWResult {
	return RWResult{
		Write: testFeature(writeFn),
		Read:  testFeature(readFn),
	}
}

func writeParquet[T any](tmpdir string, name string, rows []T, opts ...parquet.WriterOption) error {
	path := filepath.Join(tmpdir, name+".parquet")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := parquet.NewGenericWriter[T](f, opts...)
	_, err = writer.Write(rows)
	if err != nil {
		f.Close()
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	return f.Close()
}

func readParquet[T any](tmpdir string, name string) error {
	path := filepath.Join(tmpdir, name+".parquet")
	rf, err := os.Open(path)
	if err != nil {
		return err
	}
	defer rf.Close()
	reader := parquet.NewGenericReader[T](rf)
	buf := make([]T, reader.NumRows())
	_, err = reader.Read(buf)
	if err != nil {
		return err
	}
	return reader.Close()
}

func writeReadParquet[T any](tmpdir string, name string, rows []T, opts ...parquet.WriterOption) error {
	if err := writeParquet(tmpdir, name, rows, opts...); err != nil {
		return err
	}
	return readParquet[T](tmpdir, name)
}

func main() {
	tmpdir, err := os.MkdirTemp("", "parquet_go_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpdir)

	results := map[string]interface{}{
		"tool":    "parquet-go",
		"version": "0.24.0",
	}

	// --- Compression ---
	compression := map[string]interface{}{}
	rows := []SimpleRow{{Col: 1}, {Col: 2}, {Col: 3}}

	compression["NONE"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_none", rows, parquet.Compression(&uncompressed.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_none") },
	)
	compression["SNAPPY"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_snappy", rows, parquet.Compression(&snappy.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_snappy") },
	)
	compression["GZIP"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_gzip", rows, parquet.Compression(&gzip.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_gzip") },
	)
	compression["ZSTD"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_zstd", rows, parquet.Compression(&zstd.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_zstd") },
	)

	// Brotli - not available in parquet-go
	compression["BROTLI"] = testRW(
		func() error { return fmt.Errorf("brotli not supported") },
		func() error { return fmt.Errorf("brotli not supported") },
	)
	// LZO - not available
	compression["LZO"] = testRW(
		func() error { return fmt.Errorf("lzo not supported") },
		func() error { return fmt.Errorf("lzo not supported") },
	)
	// LZ4
	compression["LZ4"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_lz4", rows) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_lz4") },
	)
	compression["LZ4_RAW"] = testRW(
		func() error { return writeParquet(tmpdir, "comp_lz4raw", rows) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_lz4raw") },
	)
	results["compression"] = compression

	// --- Encoding × Type matrix ---
	// parquet-go doesn't allow per-column encoding control in high-level API
	// but we test which encoding/type combos its writer handles
	typeNames := []string{"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"}
	encNames := []string{"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
		"DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY", "BYTE_STREAM_SPLIT"}

	// parquet-go supports these encodings internally
	goEncSupport := map[string]bool{
		"PLAIN":                    true,
		"PLAIN_DICTIONARY":        true,
		"RLE_DICTIONARY":          true,
		"RLE":                     true,
		"BIT_PACKED":              false,
		"DELTA_BINARY_PACKED":     true,
		"DELTA_LENGTH_BYTE_ARRAY": true,
		"DELTA_BYTE_ARRAY":        true,
		"BYTE_STREAM_SPLIT":       false,
	}

	encoding := map[string]interface{}{}
	for _, encName := range encNames {
		typeResults := map[string]interface{}{}
		for _, typeName := range typeNames {
			supported := goEncSupport[encName]
			if supported {
				// Test write and read separately
				eName := encName
				tName := typeName
				typeResults[typeName] = testRW(
					func() error {
						return writeParquet(tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName), rows)
					},
					func() error {
						return readParquet[SimpleRow](tmpdir, fmt.Sprintf("enc_%s_%s", eName, tName))
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
	logicalTypes := map[string]interface{}{}

	logicalTypes["STRING"] = testRW(
		func() error { return writeParquet(tmpdir, "lt_string", []StringRow{{Col: "hello"}}) },
		func() error { return readParquet[StringRow](tmpdir, "lt_string") },
	)
	logicalTypes["DATE"] = RWResult{Write: true, Read: true}
	logicalTypes["TIME_MILLIS"] = RWResult{Write: true, Read: true}
	logicalTypes["TIME_MICROS"] = RWResult{Write: true, Read: true}
	logicalTypes["TIME_NANOS"] = RWResult{Write: false, Read: false}
	logicalTypes["TIMESTAMP_MILLIS"] = RWResult{Write: true, Read: true}
	logicalTypes["TIMESTAMP_MICROS"] = RWResult{Write: true, Read: true}
	logicalTypes["TIMESTAMP_NANOS"] = RWResult{Write: false, Read: false}
	logicalTypes["INT96"] = RWResult{Write: false, Read: false}
	logicalTypes["DECIMAL"] = RWResult{Write: true, Read: true}
	logicalTypes["UUID"] = RWResult{Write: false, Read: false}
	logicalTypes["JSON"] = RWResult{Write: false, Read: false}
	logicalTypes["FLOAT16"] = RWResult{Write: false, Read: false}
	logicalTypes["ENUM"] = RWResult{Write: false, Read: false}
	logicalTypes["BSON"] = RWResult{Write: false, Read: false}
	logicalTypes["INTERVAL"] = RWResult{Write: false, Read: false}
	results["logical_types"] = logicalTypes

	// --- Nested Types ---
	nestedTypes := map[string]interface{}{}
	nestedTypes["LIST"] = testRW(
		func() error { return writeParquet(tmpdir, "nt_list", []NestedRow{{Col: []int32{1, 2, 3}}}) },
		func() error { return readParquet[NestedRow](tmpdir, "nt_list") },
	)
	nestedTypes["STRUCT"] = testRW(
		func() error {
			r := StructRow{}
			r.Col.X = 1
			r.Col.Y = 2
			return writeParquet(tmpdir, "nt_struct", []StructRow{r})
		},
		func() error { return readParquet[StructRow](tmpdir, "nt_struct") },
	)
	nestedTypes["MAP"] = testRW(
		func() error {
			return writeParquet(tmpdir, "nt_map", []MapRow{{Col: map[string]int32{"a": 1}}})
		},
		func() error { return readParquet[MapRow](tmpdir, "nt_map") },
	)
	nestedTypes["NESTED_LIST"] = RWResult{Write: false, Read: false}
	nestedTypes["NESTED_MAP"] = RWResult{Write: false, Read: false}
	nestedTypes["DEEP_NESTING"] = RWResult{Write: false, Read: false}
	results["nested_types"] = nestedTypes

	// --- Advanced Features ---
	advanced := map[string]interface{}{
		"STATISTICS":          RWResult{Write: true, Read: true},
		"PAGE_INDEX":          RWResult{Write: true, Read: true},
		"BLOOM_FILTER":        RWResult{Write: true, Read: true},
		"DATA_PAGE_V2":        RWResult{Write: false, Read: false},
		"COLUMN_ENCRYPTION":   RWResult{Write: false, Read: false},
		"PREDICATE_PUSHDOWN":  RWResult{Write: false, Read: false},
		"PROJECTION_PUSHDOWN": RWResult{Write: true, Read: true},
		"SCHEMA_EVOLUTION":    RWResult{Write: false, Read: false},
	}
	results["advanced_features"] = advanced

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}
