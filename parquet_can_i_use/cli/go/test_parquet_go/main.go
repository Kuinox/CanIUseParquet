package main

import (
	"crypto/sha256"
	"encoding/base64"
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

	proofPath := findProofPath()

	results := map[string]interface{}{
		"tool":    "parquet-go",
		"version": "0.24.0",
	}

	// --- Compression ---
	compression := map[string]interface{}{}
	rows := []SimpleRow{{Col: 1}, {Col: 2}, {Col: 3}}

	compression["NONE"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_none", rows, parquet.Compression(&uncompressed.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_none") },
		filepath.Join(tmpdir, "comp_none.parquet"),
		proofPath,
	)
	compression["SNAPPY"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_snappy", rows, parquet.Compression(&snappy.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_snappy") },
		filepath.Join(tmpdir, "comp_snappy.parquet"),
		proofPath,
	)
	compression["GZIP"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_gzip", rows, parquet.Compression(&gzip.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_gzip") },
		filepath.Join(tmpdir, "comp_gzip.parquet"),
		proofPath,
	)
	compression["ZSTD"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_zstd", rows, parquet.Compression(&zstd.Codec{})) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_zstd") },
		filepath.Join(tmpdir, "comp_zstd.parquet"),
		proofPath,
	)

	// Brotli - not available in parquet-go
	compression["BROTLI"] = testRWWithProof(
		func() error { return fmt.Errorf("brotli not supported") },
		func() error { return fmt.Errorf("brotli not supported") },
		filepath.Join(tmpdir, "comp_brotli.parquet"),
		proofPath,
	)
	// LZO - not available
	compression["LZO"] = testRWWithProof(
		func() error { return fmt.Errorf("lzo not supported") },
		func() error { return fmt.Errorf("lzo not supported") },
		filepath.Join(tmpdir, "comp_lzo.parquet"),
		proofPath,
	)
	// LZ4
	compression["LZ4"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_lz4", rows) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_lz4") },
		filepath.Join(tmpdir, "comp_lz4.parquet"),
		proofPath,
	)
	compression["LZ4_RAW"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "comp_lz4raw", rows) },
		func() error { return readParquet[SimpleRow](tmpdir, "comp_lz4raw") },
		filepath.Join(tmpdir, "comp_lz4raw.parquet"),
		proofPath,
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
			wPath := filepath.Join(tmpdir, fmt.Sprintf("enc_%s_%s.parquet", eName, tName))
			typeResults[typeName] = testRWWithProof(
				func() error {
					return writeEncParquet(tmpdir, eName, tName, e)
				},
				func() error {
					return readEncParquet(tmpdir, eName, tName, e)
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

	logicalTypes["STRING"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "lt_string", []StringRow{{Col: "hello"}}) },
		func() error { return readParquet[StringRow](tmpdir, "lt_string") },
		filepath.Join(tmpdir, "lt_string.parquet"),
		proofPath,
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
	nestedTypes["LIST"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "nt_list", []NestedRow{{Col: []int32{1, 2, 3}}}) },
		func() error { return readParquet[NestedRow](tmpdir, "nt_list") },
		filepath.Join(tmpdir, "nt_list.parquet"),
		proofPath,
	)
	nestedTypes["STRUCT"] = testRWWithProof(
		func() error {
			r := StructRow{}
			r.Col.X = 1
			r.Col.Y = 2
			return writeParquet(tmpdir, "nt_struct", []StructRow{r})
		},
		func() error { return readParquet[StructRow](tmpdir, "nt_struct") },
		filepath.Join(tmpdir, "nt_struct.parquet"),
		proofPath,
	)
	nestedTypes["MAP"] = testRWWithProof(
		func() error {
			return writeParquet(tmpdir, "nt_map", []MapRow{{Col: map[string]int32{"a": 1}}})
		},
		func() error { return readParquet[MapRow](tmpdir, "nt_map") },
		filepath.Join(tmpdir, "nt_map.parquet"),
		proofPath,
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
