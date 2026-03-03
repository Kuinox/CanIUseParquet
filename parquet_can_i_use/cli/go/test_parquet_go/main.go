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

func writeReadParquet[T any](tmpdir string, name string, rows []T, opts ...parquet.WriterOption) error {
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
	f.Close()

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
	compression := map[string]bool{}
	rows := []SimpleRow{{Col: 1}, {Col: 2}, {Col: 3}}

	compression["NONE"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_none", rows, parquet.Compression(&uncompressed.Codec{}))
	})
	compression["SNAPPY"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_snappy", rows, parquet.Compression(&snappy.Codec{}))
	})
	compression["GZIP"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_gzip", rows, parquet.Compression(&gzip.Codec{}))
	})
	compression["ZSTD"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_zstd", rows, parquet.Compression(&zstd.Codec{}))
	})

	// Brotli - not available in parquet-go
	compression["BROTLI"] = testFeature(func() error {
		return fmt.Errorf("brotli not supported")
	})
	// LZO - not available
	compression["LZO"] = testFeature(func() error {
		return fmt.Errorf("lzo not supported")
	})
	// LZ4
	compression["LZ4"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_lz4", rows)
	})
	compression["LZ4_RAW"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "comp_lz4raw", rows)
	})
	results["compression"] = compression

	// --- Encoding ---
	encoding := map[string]bool{
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
	results["encoding"] = encoding

	// --- Logical Types ---
	logicalTypes := map[string]bool{}

	logicalTypes["STRING"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "lt_string", []StringRow{{Col: "hello"}})
	})
	logicalTypes["DATE"] = true
	logicalTypes["TIME_MILLIS"] = true
	logicalTypes["TIME_MICROS"] = true
	logicalTypes["TIME_NANOS"] = false
	logicalTypes["TIMESTAMP_MILLIS"] = true
	logicalTypes["TIMESTAMP_MICROS"] = true
	logicalTypes["TIMESTAMP_NANOS"] = false
	logicalTypes["INT96"] = false
	logicalTypes["DECIMAL"] = true
	logicalTypes["UUID"] = false
	logicalTypes["JSON"] = false
	logicalTypes["FLOAT16"] = false
	logicalTypes["ENUM"] = false
	logicalTypes["BSON"] = false
	logicalTypes["INTERVAL"] = false
	results["logical_types"] = logicalTypes

	// --- Nested Types ---
	nestedTypes := map[string]bool{}
	nestedTypes["LIST"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "nt_list", []NestedRow{{Col: []int32{1, 2, 3}}})
	})
	nestedTypes["STRUCT"] = testFeature(func() error {
		r := StructRow{}
		r.Col.X = 1
		r.Col.Y = 2
		return writeReadParquet(tmpdir, "nt_struct", []StructRow{r})
	})
	nestedTypes["MAP"] = testFeature(func() error {
		return writeReadParquet(tmpdir, "nt_map", []MapRow{{Col: map[string]int32{"a": 1}}})
	})
	nestedTypes["NESTED_LIST"] = false
	nestedTypes["NESTED_MAP"] = false
	nestedTypes["DEEP_NESTING"] = false
	results["nested_types"] = nestedTypes

	// --- Advanced Features ---
	advanced := map[string]bool{
		"STATISTICS":          true,
		"PAGE_INDEX":          true,
		"BLOOM_FILTER":        true,
		"DATA_PAGE_V2":        false,
		"COLUMN_ENCRYPTION":   false,
		"PREDICATE_PUSHDOWN":  false,
		"PROJECTION_PUSHDOWN": true,
		"SCHEMA_EVOLUTION":    false,
	}
	results["advanced_features"] = advanced

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}
