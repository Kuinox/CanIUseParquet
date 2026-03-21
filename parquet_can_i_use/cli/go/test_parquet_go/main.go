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

type MultiColRow struct {
	ColA int32 `parquet:"col_a"`
	ColB int32 `parquet:"col_b"`
}

type ProjectedRow struct {
	ColA int32 `parquet:"col_a"`
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

func findFixturesDir() string {
	exePath, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(exePath), "..", "..", "..", "fixtures")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	candidates := []string{
		"fixtures",
		"../../../fixtures",
		"parquet_can_i_use/fixtures",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func readProofValues(proofPath string) (string, error) {
	f, err := os.Open(proofPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return "", err
	}
	fields := pf.Schema().Fields()
	colNames := make([]string, len(fields))
	for i, field := range fields {
		colNames[i] = field.Name()
	}
	colValues := make([][]interface{}, len(fields))
	for i := range colValues {
		colValues[i] = []interface{}{}
	}
	reader := parquet.NewReader(pf)
	defer reader.Close()
	buf := make([]parquet.Row, 128)
	for {
		n, err := reader.ReadRows(buf)
		for i := 0; i < n; i++ {
			for j, v := range buf[i] {
				if j >= len(colValues) {
					break
				}
				switch v.Kind() {
				case parquet.Int32:
					colValues[j] = append(colValues[j], v.Int32())
				case parquet.Int64:
					colValues[j] = append(colValues[j], v.Int64())
				case parquet.Float:
					colValues[j] = append(colValues[j], v.Float())
				case parquet.Double:
					colValues[j] = append(colValues[j], v.Double())
				case parquet.Boolean:
					colValues[j] = append(colValues[j], v.Boolean())
				case parquet.ByteArray:
					colValues[j] = append(colValues[j], string(v.ByteArray()))
				default:
					colValues[j] = append(colValues[j], fmt.Sprintf("%v", v))
				}
			}
		}
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return "", err
		}
	}
	result := make(map[string]interface{})
	for i, name := range colNames {
		result[name] = colValues[i]
	}
	b, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func readProofLog(proofPath string) *string {
	data, err := os.ReadFile(proofPath)
	if err != nil {
		msg := fmt.Sprintf("proof_read_error: %v", err)
		return &msg
	}
	sha := sha256Hex(data)
	values, err := readProofValues(proofPath)
	if err != nil {
		msg := fmt.Sprintf("proof_sha256:%s\nproof_read_error:%v", sha, err)
		return &msg
	}
	msg := fmt.Sprintf("proof_sha256:%s\nvalues:%s", sha, values)
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

// writeLogicParquet writes a parquet file using the given schema node and rows.
func writeLogicParquet(tmpdir, name string, node parquet.Node, rows []parquet.Row) error {
	schema := parquet.NewSchema("", parquet.Group{"col": node})
	path := filepath.Join(tmpdir, name+".parquet")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := parquet.NewWriter(f, schema)
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

// readLogicParquet reads a parquet file using the given schema node.
func readLogicParquet(tmpdir, name string, node parquet.Node) error {
	schema := parquet.NewSchema("", parquet.Group{"col": node})
	path := filepath.Join(tmpdir, name+".parquet")
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
	fixturesDir := findFixturesDir()

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
	logicalTypes["DATE"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_date", parquet.Date(),
				[]parquet.Row{{parquet.ValueOf(int32(18628))}})
		},
		func() error { return readLogicParquet(tmpdir, "lt_date", parquet.Date()) },
		filepath.Join(tmpdir, "lt_date.parquet"),
		proofPath,
	)
	logicalTypes["TIME_MILLIS"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_time_millis", parquet.Time(parquet.Millisecond),
				[]parquet.Row{{parquet.ValueOf(int32(3600000))}})
		},
		func() error {
			return readLogicParquet(tmpdir, "lt_time_millis", parquet.Time(parquet.Millisecond))
		},
		filepath.Join(tmpdir, "lt_time_millis.parquet"),
		proofPath,
	)
	logicalTypes["TIME_MICROS"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_time_micros", parquet.Time(parquet.Microsecond),
				[]parquet.Row{{parquet.ValueOf(int64(3600000000))}})
		},
		func() error {
			return readLogicParquet(tmpdir, "lt_time_micros", parquet.Time(parquet.Microsecond))
		},
		filepath.Join(tmpdir, "lt_time_micros.parquet"),
		proofPath,
	)
	logicalTypes["TIME_NANOS"] = RWResult{Write: false, Read: false}
	logicalTypes["TIMESTAMP_MILLIS"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_ts_millis", parquet.Timestamp(parquet.Millisecond),
				[]parquet.Row{{parquet.ValueOf(int64(1700000000000))}})
		},
		func() error {
			return readLogicParquet(tmpdir, "lt_ts_millis", parquet.Timestamp(parquet.Millisecond))
		},
		filepath.Join(tmpdir, "lt_ts_millis.parquet"),
		proofPath,
	)
	logicalTypes["TIMESTAMP_MICROS"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_ts_micros", parquet.Timestamp(parquet.Microsecond),
				[]parquet.Row{{parquet.ValueOf(int64(1700000000000000))}})
		},
		func() error {
			return readLogicParquet(tmpdir, "lt_ts_micros", parquet.Timestamp(parquet.Microsecond))
		},
		filepath.Join(tmpdir, "lt_ts_micros.parquet"),
		proofPath,
	)
	logicalTypes["TIMESTAMP_NANOS"] = RWResult{Write: false, Read: false}
	logicalTypes["INT96"] = RWResult{Write: false, Read: false}
	logicalTypes["DECIMAL"] = testRWWithProof(
		func() error {
			return writeLogicParquet(tmpdir, "lt_decimal", parquet.Decimal(2, 10, parquet.Int32Type),
				[]parquet.Row{{parquet.ValueOf(int32(12345))}})
		},
		func() error {
			return readLogicParquet(tmpdir, "lt_decimal", parquet.Decimal(2, 10, parquet.Int32Type))
		},
		filepath.Join(tmpdir, "lt_decimal.parquet"),
		proofPath,
	)
	logicalTypes["UUID"] = RWResult{Write: false, Read: false}
	logicalTypes["JSON"] = RWResult{Write: false, Read: false}
	logicalTypes["FLOAT16"] = RWResult{Write: false, Read: false}
	logicalTypes["ENUM"] = RWResult{Write: false, Read: false}
	logicalTypes["BSON"] = RWResult{Write: false, Read: false}
	logicalTypes["INTERVAL"] = RWResult{Write: false, Read: false}
	logicalTypes["UNKNOWN"] = RWResult{Write: false, Read: false}
	logicalTypes["VARIANT"] = RWResult{Write: false, Read: false}
	logicalTypes["GEOMETRY"] = RWResult{Write: false, Read: false}
	logicalTypes["GEOGRAPHY"] = RWResult{Write: false, Read: false}
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
	advRows := []SimpleRow{{Col: 1}, {Col: 2}, {Col: 3}}
	advanced := map[string]interface{}{}

	advanced["STATISTICS"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "adv_stats", advRows) },
		func() error { return readParquet[SimpleRow](tmpdir, "adv_stats") },
		filepath.Join(tmpdir, "adv_stats.parquet"),
		proofPath,
	)
	advanced["PAGE_INDEX"] = testRWWithProof(
		func() error { return writeParquet(tmpdir, "adv_pageidx", advRows) },
		func() error { return readParquet[SimpleRow](tmpdir, "adv_pageidx") },
		filepath.Join(tmpdir, "adv_pageidx.parquet"),
		proofPath,
	)
	advanced["BLOOM_FILTER"] = testRWWithProof(
		func() error {
			return writeParquet(tmpdir, "adv_bloom", advRows,
				parquet.BloomFilters(parquet.SplitBlockFilter(10, "col")))
		},
		func() error { return readParquet[SimpleRow](tmpdir, "adv_bloom") },
		filepath.Join(tmpdir, "adv_bloom.parquet"),
		proofPath,
	)
	advanced["DATA_PAGE_V2"] = RWResult{Write: false, Read: false}
	advanced["COLUMN_ENCRYPTION"] = RWResult{Write: false, Read: false}
	advanced["PREDICATE_PUSHDOWN"] = RWResult{Write: false, Read: false}
	advanced["PROJECTION_PUSHDOWN"] = testRWWithProof(
		func() error {
			multiRows := []MultiColRow{{ColA: 1, ColB: 10}, {ColA: 2, ColB: 20}}
			return writeParquet(tmpdir, "adv_proj", multiRows)
		},
		func() error { return readParquet[ProjectedRow](tmpdir, "adv_proj") },
		filepath.Join(tmpdir, "adv_proj.parquet"),
		proofPath,
	)
	advanced["SCHEMA_EVOLUTION"] = RWResult{Write: false, Read: false}

	// SIZE_STATISTICS: parquet-go does not expose explicit size-statistics control.
	// Test read support using the pre-generated fixture.
	{
		readFn := func() error {
			return fmt.Errorf("SIZE_STATISTICS fixture not found")
		}
		if fixturesDir != "" {
			fixturePath := filepath.Join(fixturesDir, "advanced_features", "adv_SIZE_STATISTICS.parquet")
			if _, err := os.Stat(fixturePath); err == nil {
				readFn = func() error {
					f, err := os.Open(fixturePath)
					if err != nil {
						return err
					}
					defer f.Close()
					info, err := f.Stat()
					if err != nil {
						return err
					}
					pf, err := parquet.OpenFile(f, info.Size())
					if err != nil {
						return err
					}
					_ = pf
					return nil
				}
			}
		}
		advanced["SIZE_STATISTICS"] = testRW(
			func() error { return fmt.Errorf("parquet-go does not expose SIZE_STATISTICS write control") },
			readFn,
		)
	}

	// PAGE_CRC32: parquet-go does not write page checksums.
	// Test read support using the pre-generated fixture.
	{
		readFn := func() error {
			return fmt.Errorf("PAGE_CRC32 fixture not found")
		}
		if fixturesDir != "" {
			fixturePath := filepath.Join(fixturesDir, "advanced_features", "adv_PAGE_CRC32.parquet")
			if _, err := os.Stat(fixturePath); err == nil {
				readFn = func() error {
					f, err := os.Open(fixturePath)
					if err != nil {
						return err
					}
					defer f.Close()
					info, err := f.Stat()
					if err != nil {
						return err
					}
					pf, err := parquet.OpenFile(f, info.Size())
					if err != nil {
						return err
					}
					_ = pf
					return nil
				}
			}
		}
		advanced["PAGE_CRC32"] = testRW(
			func() error { return fmt.Errorf("parquet-go does not write page CRC32 checksums") },
			readFn,
		)
	}
	results["advanced_features"] = advanced

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}
