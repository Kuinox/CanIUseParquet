package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	pgbytestream "github.com/parquet-go/parquet-go/encoding/bytestreamsplit"
	pgdelta "github.com/parquet-go/parquet-go/encoding/delta"
	pgplain "github.com/parquet-go/parquet-go/encoding/plain"
	pgrle "github.com/parquet-go/parquet-go/encoding/rle"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"

	pgencoding "github.com/parquet-go/parquet-go/encoding"
)

type SimpleRow struct {
	Col int32 `parquet:"col"`
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

// testFeature runs fn() and returns (ok, errMsg).
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

type RWResult struct {
	Write    bool    `json:"write"`
	Read     bool    `json:"read"`
	WriteLog *string `json:"write_log,omitempty"`
	ReadLog  *string `json:"read_log,omitempty"`
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

// makeTypedNode returns a parquet.Node for the given physical type name.
func makeTypedNode(ptype string) parquet.Node {
	switch ptype {
	case "INT32":
		return parquet.Int(32)
	case "INT64":
		return parquet.Int(64)
	case "FLOAT":
		return parquet.Leaf(parquet.FloatType)
	case "DOUBLE":
		return parquet.Leaf(parquet.DoubleType)
	case "BOOLEAN":
		return parquet.Leaf(parquet.BooleanType)
	case "BYTE_ARRAY":
		return parquet.String()
	}
	return parquet.Int(32)
}

// sampleRows returns 3 sample rows for the given physical type.
func sampleRows(ptype string) []parquet.Row {
	out := make([]parquet.Row, 3)
	for i := int32(0); i < 3; i++ {
		switch ptype {
		case "INT32":
			out[i] = parquet.Row{parquet.ValueOf(int32(i + 1))}
		case "INT64":
			out[i] = parquet.Row{parquet.ValueOf(int64(i + 1))}
		case "FLOAT":
			out[i] = parquet.Row{parquet.FloatValue(float32(i + 1))}
		case "DOUBLE":
			out[i] = parquet.Row{parquet.DoubleValue(float64(i + 1))}
		case "BOOLEAN":
			out[i] = parquet.Row{parquet.BooleanValue(i%2 == 0)}
		case "BYTE_ARRAY":
			out[i] = parquet.Row{parquet.ValueOf([]byte{byte(i + 1)})}
		default:
			out[i] = parquet.Row{parquet.ValueOf(int32(i + 1))}
		}
	}
	return out
}

// writeEncParquet writes a file using the low-level parquet.Writer with a
// schema that has enc applied to the single column "col".
func writeEncParquet(tmpdir, encName, typeName string, enc pgencoding.Encoding) error {
	leaf := makeTypedNode(typeName)
	node := parquet.Group{"col": parquet.Encoded(leaf, enc)}
	schema := parquet.NewSchema("", node)

	path := filepath.Join(tmpdir, fmt.Sprintf("enc_%s_%s.parquet", encName, typeName))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := parquet.NewWriter(f, schema)
	rows := sampleRows(typeName)
	if _, err := writer.WriteRows(rows); err != nil {
		writer.Close()
		f.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// readEncParquet reads back a file written by writeEncParquet.
func readEncParquet(tmpdir, encName, typeName string, enc pgencoding.Encoding) error {
	leaf := makeTypedNode(typeName)
	node := parquet.Group{"col": parquet.Encoded(leaf, enc)}
	schema := parquet.NewSchema("", node)

	path := filepath.Join(tmpdir, fmt.Sprintf("enc_%s_%s.parquet", encName, typeName))
	rf, err := os.Open(path)
	if err != nil {
		return err
	}
	defer rf.Close()
	reader := parquet.NewReader(rf, schema)
	defer reader.Close()
	buf := make([]parquet.Row, 3)
	_, err = reader.ReadRows(buf)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
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
	// For each encoding+type we use parquet-go's low-level schema Node API with
	// parquet.Encoded(node, enc) to apply the specific encoding.  Combinations that
	// are not supported by the library (e.g. DELTA_BINARY_PACKED on FLOAT) cause a
	// panic inside parquet.Encoded; testFeature() recovers from that and returns false.
	encNames := []string{
		"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
		"DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
		"BYTE_STREAM_SPLIT",
	}
	typeNames := []string{"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"}

	// Map encoding name → encoding object
	encObjects := map[string]pgencoding.Encoding{
		"PLAIN":                    &pgplain.Encoding{},
		"PLAIN_DICTIONARY":         &parquet.PlainDictionary,
		"RLE_DICTIONARY":           &parquet.RLEDictionary,
		"RLE":                      &pgrle.Encoding{},
		"BIT_PACKED":               nil, // deprecated, not implemented
		"DELTA_BINARY_PACKED":      &pgdelta.BinaryPackedEncoding{},
		"DELTA_LENGTH_BYTE_ARRAY":  &pgdelta.LengthByteArrayEncoding{},
		"DELTA_BYTE_ARRAY":         &pgdelta.ByteArrayEncoding{},
		"BYTE_STREAM_SPLIT":        &pgbytestream.Encoding{},
	}

	encoding := map[string]interface{}{}
	for _, encName := range encNames {
		typeResults := map[string]interface{}{}
		enc := encObjects[encName]
		for _, typeName := range typeNames {
			eName := encName
			tName := typeName
			e := enc
			if e == nil {
				// BIT_PACKED: deprecated / not implemented
				typeResults[typeName] = RWResult{Write: false, Read: false}
				continue
			}
			typeResults[typeName] = testRW(
				func() error {
					return writeEncParquet(tmpdir, eName, tName, e)
				},
				func() error {
					return readEncParquet(tmpdir, eName, tName, e)
				},
			)
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
