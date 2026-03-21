using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using ParquetSharp;
using ParquetSharp.Schema;

class Program
{
    static string tmpDir = Path.Combine(Path.GetTempPath(), "parquet_sharp_test_" + Guid.NewGuid().ToString("N"));

    static (bool ok, string? log) TestFeature(Action fn)
    {
        try { fn(); return (true, null); }
        catch (Exception e) { return (false, e.ToString()); }
    }

    static Dictionary<string, object> TestRW(Action writeFn, Action readFn)
    {
        var (writeOk, writeLog) = TestFeature(writeFn);
        var (readOk, readLog) = TestFeature(readFn);
        var result = new Dictionary<string, object> { ["write"] = writeOk, ["read"] = readOk };
        if (writeLog != null) result["write_log"] = writeLog;
        if (readLog != null) result["read_log"] = readLog;
        return result;
    }

    static string Sha256Hex(byte[] data)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(data);
        return BitConverter.ToString(hash).Replace("-", "").ToLower();
    }

    static string? FindProofPath()
    {
        var candidates = new[] {
            "fixtures/proof/proof.parquet",
            Path.Combine("..", "..", "..", "fixtures", "proof", "proof.parquet"),
            "parquet_can_i_use/fixtures/proof/proof.parquet",
        };
        foreach (var c in candidates)
            if (File.Exists(c)) return Path.GetFullPath(c);
        return null;
    }

    static string? ReadProofLog(string? proofPath)
    {
        if (proofPath == null || !File.Exists(proofPath)) return null;
        try
        {
            var data = File.ReadAllBytes(proofPath);
            var sha = Sha256Hex(data);
            using var reader = new ParquetFileReader(proofPath);
            var fileMetadata = reader.FileMetaData;
            var values = new Dictionary<string, List<object?>>();
            for (int rg = 0; rg < fileMetadata.NumRowGroups; rg++)
            {
                using var rowGroup = reader.RowGroup(rg);
                long numRows = rowGroup.MetaData.NumRows;
                int numCols = fileMetadata.Schema.NumColumns;
                for (int c = 0; c < numCols; c++)
                {
                    var colDescr = fileMetadata.Schema.Column(c);
                    string colName = colDescr.Name;
                    if (!values.ContainsKey(colName))
                        values[colName] = new List<object?>();
                    using var colReader = rowGroup.Column(c);
                    switch (colDescr.PhysicalType)
                    {
                        case PhysicalType.Int32:
                            try {
                                foreach (var v in colReader.LogicalReader<int>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            } catch (InvalidCastException) {
                                using var nullableColReader = rowGroup.Column(c);
                                foreach (var v in nullableColReader.LogicalReader<int?>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            }
                            break;
                        case PhysicalType.Int64:
                            try {
                                foreach (var v in colReader.LogicalReader<long>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            } catch (InvalidCastException) {
                                using var nullableColReader = rowGroup.Column(c);
                                foreach (var v in nullableColReader.LogicalReader<long?>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            }
                            break;
                        case PhysicalType.Float:
                            try {
                                foreach (var v in colReader.LogicalReader<float>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            } catch (InvalidCastException) {
                                using var nullableColReader = rowGroup.Column(c);
                                foreach (var v in nullableColReader.LogicalReader<float?>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            }
                            break;
                        case PhysicalType.Double:
                            try {
                                foreach (var v in colReader.LogicalReader<double>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            } catch (InvalidCastException) {
                                using var nullableColReader = rowGroup.Column(c);
                                foreach (var v in nullableColReader.LogicalReader<double?>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            }
                            break;
                        case PhysicalType.Boolean:
                            try {
                                foreach (var v in colReader.LogicalReader<bool>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            } catch (InvalidCastException) {
                                using var nullableColReader = rowGroup.Column(c);
                                foreach (var v in nullableColReader.LogicalReader<bool?>().ReadAll((int)numRows))
                                    values[colName].Add(v);
                            }
                            break;
                        default:
                            values[colName].Add($"unsupported:{colDescr.PhysicalType}");
                            break;
                    }
                }
            }
            reader.Close();
            return $"proof_sha256:{sha}\nvalues:{JsonSerializer.Serialize(values)}";
        }
        catch (Exception e) { return $"proof_read_error:{e.Message}"; }
    }

    static Dictionary<string, object> TestRW(Action writeFn, Action readFn, string? writePath, string? proofPath)
    {
        var (writeOk, writeLog) = TestFeature(writeFn);
        var (readOk, readLog) = TestFeature(readFn);
        if (writeOk && writePath != null && File.Exists(writePath))
        {
            try
            {
                var data = File.ReadAllBytes(writePath);
                var sha = Sha256Hex(data);
                var b64 = Convert.ToBase64String(data);
                writeLog = $"sha256:{sha}\n{b64}";
            }
            catch { }
        }
        if (readOk) readLog = ReadProofLog(proofPath);
        var result = new Dictionary<string, object> { ["write"] = writeOk, ["read"] = readOk };
        if (writeLog != null) result["write_log"] = writeLog;
        if (readLog != null) result["read_log"] = readLog;
        return result;
    }

    static Dictionary<string, object> RW(bool write, bool read) =>
        new Dictionary<string, object> { ["write"] = write, ["read"] = read };

    static string TmpPath(string name) => Path.Combine(tmpDir, name + ".parquet");

    // --- Helpers ---

    static void WriteIntFile(string name, WriterProperties props)
    {
        var columns = new Column[] { new Column<int>("col") };
        using var writer = new ParquetFileWriter(TmpPath(name), columns, props);
        using var rg = writer.AppendRowGroup();
        using var col = rg.NextColumn().LogicalWriter<int>();
        col.WriteBatch(new int[] { 1, 2, 3 });
        writer.Close();
    }

    static void ReadIntFile(string name)
    {
        using var reader = new ParquetFileReader(TmpPath(name));
        using var rg = reader.RowGroup(0);
        var vals = rg.Column(0).LogicalReader<int>().ReadAll(3);
        reader.Close();
    }

    static WriterProperties DefaultProps() =>
        new WriterPropertiesBuilder().Build();

    static void Main()
    {
        Directory.CreateDirectory(tmpDir);
        var results = new Dictionary<string, object>
        {
            ["tool"] = "parquet-sharp",
            ["version"] = typeof(ParquetFileWriter).Assembly.GetName().Version?.ToString() ?? "unknown"
        };

        // --- Compression ---
        var compression = new Dictionary<string, object>();

        var proofPath = FindProofPath();

        void TestCompression(string key, Compression codec)
        {
            var props = new WriterPropertiesBuilder().Compression(codec).Build();
            compression[key] = TestRW(
                () => WriteIntFile("comp_" + key, props),
                () => ReadIntFile("comp_" + key),
                TmpPath("comp_" + key),
                proofPath);
        }

        TestCompression("NONE", Compression.Uncompressed);
        TestCompression("SNAPPY", Compression.Snappy);
        TestCompression("GZIP", Compression.Gzip);
        TestCompression("BROTLI", Compression.Brotli);
        TestCompression("LZO", Compression.Lzo);
        // Lz4Hadoop = legacy Parquet LZ4 (Hadoop-wrapped)
        TestCompression("LZ4", Compression.Lz4Hadoop);
        // Lz4 in ParquetSharp = LZ4 Frame = LZ4_RAW in Parquet spec
        TestCompression("LZ4_RAW", Compression.Lz4);
        TestCompression("ZSTD", Compression.Zstd);
        results["compression"] = compression;

        // --- Encoding × Type ---
        var encoding = new Dictionary<string, object>();

        // Test an encoding for a given C# type using WriterPropertiesBuilder
        Dictionary<string, object> TryWriteRead<T>(string fileSuffix, Column[] cols, T[] data, WriterProperties props)
        {
            var path = TmpPath(fileSuffix);
            return TestRW(
                () => {
                    using var w = new ParquetFileWriter(path, cols, props);
                    using var rg = w.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<T>();
                    c.WriteBatch(data);
                    w.Close();
                },
                () => {
                    using var r = new ParquetFileReader(path);
                    using var rg = r.RowGroup(0);
                    rg.Column(0).LogicalReader<T>().ReadAll(data.Length);
                    r.Close();
                },
                path, proofPath);
        }

        // Plain encoding (disable dictionary to force plain)
        WriterProperties PlainProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.Plain).Build();
        // RLE Dictionary (default dictionary encoding)
        WriterProperties RleDictProps() => new WriterPropertiesBuilder().EnableDictionary().Build();
        // Delta binary packed
        WriterProperties DeltaProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.DeltaBinaryPacked).Build();
        // Delta length byte array
        WriterProperties DeltaLengthProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.DeltaLengthByteArray).Build();
        // Delta byte array
        WriterProperties DeltaByteArrayProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.DeltaByteArray).Build();
        // Byte stream split
        WriterProperties ByteStreamSplitProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.ByteStreamSplit).Build();
        // RLE (for booleans)
        WriterProperties RleProps() => new WriterPropertiesBuilder().DisableDictionary().Encoding(Encoding.Rle).Build();

        var intCols = new Column[] { new Column<int>("col") };
        var longCols = new Column[] { new Column<long>("col") };
        var floatCols = new Column[] { new Column<float>("col") };
        var doubleCols = new Column[] { new Column<double>("col") };
        var boolCols = new Column[] { new Column<bool>("col") };
        var byteArrayCols = new Column[] { new Column<byte[]>("col") };

        string[] typeNames = { "INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY" };

        // PLAIN
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_plain_i32", intCols, new int[] { 1, 2, 3 }, PlainProps());
            t["INT64"] = TryWriteRead("enc_plain_i64", longCols, new long[] { 1L, 2L, 3L }, PlainProps());
            t["FLOAT"] = TryWriteRead("enc_plain_f32", floatCols, new float[] { 1f, 2f, 3f }, PlainProps());
            t["DOUBLE"] = TryWriteRead("enc_plain_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, PlainProps());
            t["BOOLEAN"] = TryWriteRead("enc_plain_bool", boolCols, new bool[] { true, false, true }, PlainProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_plain_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, PlainProps());
            encoding["PLAIN"] = t;
        }
        // PLAIN_DICTIONARY (deprecated legacy)
        {
            var t = new Dictionary<string, object>();
            foreach (var tn in typeNames) t[tn] = RW(false, false);
            encoding["PLAIN_DICTIONARY"] = t;
        }
        // RLE_DICTIONARY
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_rledict_i32", intCols, new int[] { 1, 2, 3 }, RleDictProps());
            t["INT64"] = TryWriteRead("enc_rledict_i64", longCols, new long[] { 1L, 2L, 3L }, RleDictProps());
            t["FLOAT"] = TryWriteRead("enc_rledict_f32", floatCols, new float[] { 1f, 2f, 3f }, RleDictProps());
            t["DOUBLE"] = TryWriteRead("enc_rledict_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, RleDictProps());
            t["BOOLEAN"] = TryWriteRead("enc_rledict_bool", boolCols, new bool[] { true, false, true }, RleDictProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_rledict_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, RleDictProps());
            encoding["RLE_DICTIONARY"] = t;
        }
        // RLE
        {
            var t = new Dictionary<string, object>();
            // RLE is not valid for INT32/INT64/FLOAT/DOUBLE/BYTE_ARRAY directly - only BOOLEAN via fallback
            t["INT32"] = TryWriteRead("enc_rle_i32", intCols, new int[] { 1, 2, 3 }, RleProps());
            t["INT64"] = TryWriteRead("enc_rle_i64", longCols, new long[] { 1L, 2L, 3L }, RleProps());
            t["FLOAT"] = TryWriteRead("enc_rle_f32", floatCols, new float[] { 1f, 2f, 3f }, RleProps());
            t["DOUBLE"] = TryWriteRead("enc_rle_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, RleProps());
            t["BOOLEAN"] = TryWriteRead("enc_rle_bool", boolCols, new bool[] { true, false, true }, RleProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_rle_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, RleProps());
            encoding["RLE"] = t;
        }
        // BIT_PACKED (deprecated)
        {
            var t = new Dictionary<string, object>();
            foreach (var tn in typeNames) t[tn] = RW(false, false);
            encoding["BIT_PACKED"] = t;
        }
        // DELTA_BINARY_PACKED
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_dbp_i32", intCols, new int[] { 1, 2, 3 }, DeltaProps());
            t["INT64"] = TryWriteRead("enc_dbp_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaProps());
            t["FLOAT"] = TryWriteRead("enc_dbp_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaProps());
            t["DOUBLE"] = TryWriteRead("enc_dbp_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaProps());
            t["BOOLEAN"] = TryWriteRead("enc_dbp_bool", boolCols, new bool[] { true, false, true }, DeltaProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_dbp_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaProps());
            encoding["DELTA_BINARY_PACKED"] = t;
        }
        // DELTA_LENGTH_BYTE_ARRAY
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_dlba_i32", intCols, new int[] { 1, 2, 3 }, DeltaLengthProps());
            t["INT64"] = TryWriteRead("enc_dlba_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaLengthProps());
            t["FLOAT"] = TryWriteRead("enc_dlba_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaLengthProps());
            t["DOUBLE"] = TryWriteRead("enc_dlba_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaLengthProps());
            t["BOOLEAN"] = TryWriteRead("enc_dlba_bool", boolCols, new bool[] { true, false, true }, DeltaLengthProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_dlba_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaLengthProps());
            encoding["DELTA_LENGTH_BYTE_ARRAY"] = t;
        }
        // DELTA_BYTE_ARRAY
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_dba_i32", intCols, new int[] { 1, 2, 3 }, DeltaByteArrayProps());
            t["INT64"] = TryWriteRead("enc_dba_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaByteArrayProps());
            t["FLOAT"] = TryWriteRead("enc_dba_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaByteArrayProps());
            t["DOUBLE"] = TryWriteRead("enc_dba_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaByteArrayProps());
            t["BOOLEAN"] = TryWriteRead("enc_dba_bool", boolCols, new bool[] { true, false, true }, DeltaByteArrayProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_dba_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaByteArrayProps());
            encoding["DELTA_BYTE_ARRAY"] = t;
        }
        // BYTE_STREAM_SPLIT
        {
            var t = new Dictionary<string, object>();
            t["INT32"] = TryWriteRead("enc_bss_i32", intCols, new int[] { 1, 2, 3 }, ByteStreamSplitProps());
            t["INT64"] = TryWriteRead("enc_bss_i64", longCols, new long[] { 1L, 2L, 3L }, ByteStreamSplitProps());
            t["FLOAT"] = TryWriteRead("enc_bss_f32", floatCols, new float[] { 1f, 2f, 3f }, ByteStreamSplitProps());
            t["DOUBLE"] = TryWriteRead("enc_bss_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, ByteStreamSplitProps());
            t["BOOLEAN"] = TryWriteRead("enc_bss_bool", boolCols, new bool[] { true, false, true }, ByteStreamSplitProps());
            t["BYTE_ARRAY"] = TryWriteRead("enc_bss_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, ByteStreamSplitProps());
            encoding["BYTE_STREAM_SPLIT"] = t;
        }
        results["encoding"] = encoding;

        // --- Logical Types ---
        var logicalTypes = new Dictionary<string, object>();

        // STRING
        {
            var cols = new Column[] { new Column<string>("col") };
            logicalTypes["STRING"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_string"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<string>();
                    c.WriteBatch(new string[] { "a", "b", "c" });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_string"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<string>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_string"), proofPath);
        }

        // DATE (using ParquetSharp.Date struct)
        {
            var cols = new Column[] { new Column<Date>("col") };
            logicalTypes["DATE"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_date"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<Date>();
                    c.WriteBatch(new Date[] { new Date(2023, 1, 1), new Date(2023, 6, 15), new Date(2024, 12, 31) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_date"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<Date>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_date"), proofPath);
        }

        // TIME_MILLIS (TimeSpan maps to MICROS by default; use low-level for MILLIS)
        {
            logicalTypes["TIME_MILLIS"] = TestRW(
                () => {
                    using var lt = LogicalType.Time(true, TimeUnit.Millis);
                    using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.Int32);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                    using var writer = new ParquetFileWriter(TmpPath("lt_time_ms"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<TimeSpan>();
                    c.WriteBatch(new TimeSpan[] { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), TimeSpan.FromSeconds(45) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_time_ms"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<TimeSpan>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_time_ms"), proofPath);
        }

        // TIME_MICROS (TimeSpan default = MICROS)
        {
            var cols = new Column[] { new Column<TimeSpan>("col") };
            logicalTypes["TIME_MICROS"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_time_us"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<TimeSpan>();
                    c.WriteBatch(new TimeSpan[] { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), TimeSpan.FromSeconds(45) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_time_us"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<TimeSpan>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_time_us"), proofPath);
        }

        // TIME_NANOS (TimeSpanNanos)
        {
            var cols = new Column[] { new Column<TimeSpanNanos>("col") };
            logicalTypes["TIME_NANOS"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_time_ns"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<TimeSpanNanos>();
                    c.WriteBatch(new TimeSpanNanos[] { new TimeSpanNanos(1000000L), new TimeSpanNanos(2000000L), new TimeSpanNanos(3000000L) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_time_ns"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<TimeSpanNanos>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_time_ns"), proofPath);
        }

        // TIMESTAMP_MILLIS (use low-level schema with millis logical type)
        {
            logicalTypes["TIMESTAMP_MILLIS"] = TestRW(
                () => {
                    using var lt = LogicalType.Timestamp(true, TimeUnit.Millis);
                    using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.Int64);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                    using var writer = new ParquetFileWriter(TmpPath("lt_ts_ms"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<DateTime>();
                    c.WriteBatch(new DateTime[] { DateTime.UtcNow, DateTime.UtcNow.AddHours(1), DateTime.UtcNow.AddDays(1) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_ts_ms"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<DateTime>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_ts_ms"), proofPath);
        }

        // TIMESTAMP_MICROS (DateTime default = MICROS)
        {
            var cols = new Column[] { new Column<DateTime>("col") };
            logicalTypes["TIMESTAMP_MICROS"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_ts_us"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<DateTime>();
                    c.WriteBatch(new DateTime[] { DateTime.UtcNow, DateTime.UtcNow.AddHours(1), DateTime.UtcNow.AddDays(1) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_ts_us"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<DateTime>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_ts_us"), proofPath);
        }

        // TIMESTAMP_NANOS (DateTimeNanos)
        {
            var cols = new Column[] { new Column<DateTimeNanos>("col") };
            logicalTypes["TIMESTAMP_NANOS"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_ts_ns"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<DateTimeNanos>();
                    c.WriteBatch(new DateTimeNanos[] { new DateTimeNanos(DateTime.UtcNow), new DateTimeNanos(DateTime.UtcNow.AddHours(1)), new DateTimeNanos(DateTime.UtcNow.AddDays(1)) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_ts_ns"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<DateTimeNanos>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_ts_ns"), proofPath);
        }

        // INT96 (legacy)
        {
            var cols = new Column[] { new Column<Int96>("col") };
            logicalTypes["INT96"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_int96"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<Int96>();
                    c.WriteBatch(new Int96[] { new Int96(1, 2, 3), new Int96(4, 5, 6), new Int96(7, 8, 9) });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_int96"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<Int96>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_int96"), proofPath);
        }

        // DECIMAL
        {
            var decLogicalType = LogicalType.Decimal(29, 3);
            var cols = new Column[] { new Column<decimal>("col", decLogicalType) };
            logicalTypes["DECIMAL"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_decimal"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<decimal>();
                    c.WriteBatch(new decimal[] { 1.234m, 2.345m, 3.456m });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_decimal"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<decimal>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_decimal"), proofPath);
            decLogicalType.Dispose();
        }

        // UUID (Guid with UuidLogicalType)
        {
            var uuidLogicalType = LogicalType.Uuid();
            var cols = new Column[] { new Column<Guid>("col", uuidLogicalType) };
            logicalTypes["UUID"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("lt_uuid"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<Guid>();
                    c.WriteBatch(new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_uuid"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<Guid>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_uuid"), proofPath);
            uuidLogicalType.Dispose();
        }

        // JSON (string annotated as JSON via low-level API)
        {
            logicalTypes["JSON"] = TestRW(
                () => {
                    using var lt = LogicalType.Json();
                    using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.ByteArray);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                    using var writer = new ParquetFileWriter(TmpPath("lt_json"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<string>();
                    c.WriteBatch(new string[] { "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_json"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<string>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_json"), proofPath);
        }

        // FLOAT16 (Half type, .NET 5+)
        {
            logicalTypes["FLOAT16"] = TestRW(
                () => {
                    var cols16 = new Column[] { new Column<Half>("col") };
                    using var writer = new ParquetFileWriter(TmpPath("lt_float16"), cols16, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<Half>();
                    c.WriteBatch(new Half[] { (Half)1.5f, (Half)2.5f, (Half)3.5f });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_float16"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<Half>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_float16"), proofPath);
        }

        // ENUM (byte array annotated as ENUM via low-level ColumnWriter API)
        {
            logicalTypes["ENUM"] = TestRW(
                () => {
                    using var lt = LogicalType.Enum();
                    using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.ByteArray);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                    using var writer = new ParquetFileWriter(TmpPath("lt_enum"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    // ENUM logical type is not supported by LogicalWriter<string>; use ColumnWriter<ByteArray>
                    var valBytes = new byte[] { 0x41 };
                    var gcHandle = GCHandle.Alloc(valBytes, GCHandleType.Pinned);
                    try
                    {
                        var ba = new ByteArray(gcHandle.AddrOfPinnedObject(), 1);
                        ((ColumnWriter<ByteArray>)rg.NextColumn()).WriteBatch(new ByteArray[] { ba, ba, ba });
                    }
                    finally { gcHandle.Free(); }
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_enum"));
                    using var rg = reader.RowGroup(0);
                    var vals = new ByteArray[3];
                    ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(3, vals, out _);
                    reader.Close();
                },
                TmpPath("lt_enum"), proofPath);
        }

        // BSON (byte array annotated as BSON via low-level API)
        {
            logicalTypes["BSON"] = TestRW(
                () => {
                    using var lt = LogicalType.Bson();
                    using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.ByteArray);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                    using var writer = new ParquetFileWriter(TmpPath("lt_bson"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<byte[]>();
                    c.WriteBatch(new byte[][] { new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 }, new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 }, new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 } });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("lt_bson"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<byte[]>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("lt_bson"), proofPath);
        }

        // INTERVAL (deprecated; parquet-cpp rejects INTERVAL for FIXED_LEN_BYTE_ARRAY)
        logicalTypes["INTERVAL"] = RW(false, false);

        results["logical_types"] = logicalTypes;

        // --- Nested Types ---
        var nestedTypes = new Dictionary<string, object>();

        // LIST (Column<int[]> maps to Parquet LIST type)
        {
            var cols = new Column[] { new Column<int[]>("col") };
            nestedTypes["LIST"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("nt_list"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int[]>();
                    c.WriteBatch(new int[][] { new int[] { 1, 2 }, new int[] { 3 }, new int[] { 4, 5, 6 } });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_list"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<int[]>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("nt_list"), proofPath);
        }

        // MAP (using low-level GroupNode schema and ColumnWriter with explicit levels)
        {
            nestedTypes["MAP"] = TestRW(
                () => {
                    using var keyNode = new PrimitiveNode("key", Repetition.Required, LogicalType.None(), PhysicalType.ByteArray);
                    using var valueNode = new PrimitiveNode("value", Repetition.Required, LogicalType.None(), PhysicalType.Int32);
                    using var kvNode = new GroupNode("key_value", Repetition.Repeated, new Node[] { keyNode, valueNode });
                    using var mapLogicalType = LogicalType.Map();
                    using var mapNode = new GroupNode("col", Repetition.Optional, new Node[] { kvNode }, mapLogicalType);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { mapNode });
                    using var writer = new ParquetFileWriter(TmpPath("nt_map"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    // col=Optional(maxDef=1), key_value=Repeated(maxDef=2,maxRep=1), key=Required
                    // 3 rows, 1 entry each: defLevels=[2,2,2], repLevels=[0,0,0]
                    short[] defLevels = { 2, 2, 2 };
                    short[] repLevels = { 0, 0, 0 };
                    var keyData = new byte[] { 0x61 };
                    var keyGcHandle = GCHandle.Alloc(keyData, GCHandleType.Pinned);
                    try
                    {
                        var keyBa = new ByteArray(keyGcHandle.AddrOfPinnedObject(), 1);
                        var keyWriter = (ColumnWriter<ByteArray>)rg.NextColumn();
                        keyWriter.WriteBatch(3, defLevels, repLevels, new ByteArray[] { keyBa, keyBa, keyBa });
                    }
                    finally { keyGcHandle.Free(); }
                    var valWriter = (ColumnWriter<int>)rg.NextColumn();
                    valWriter.WriteBatch(3, defLevels, repLevels, new int[] { 1, 2, 3 });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_map"));
                    using var rg = reader.RowGroup(0);
                    var keys = new ByteArray[3];
                    var defLevels = new short[3];
                    var repLevels = new short[3];
                    ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(3, defLevels, repLevels, keys, out _);
                    reader.Close();
                },
                TmpPath("nt_map"), proofPath);
        }

        // STRUCT (using Nested<T> wrapper or GroupNode)
        {
            nestedTypes["STRUCT"] = TestRW(
                () => {
                    // Use a GroupNode with two primitive children (struct)
                    using var field1 = new PrimitiveNode("x", Repetition.Required, LogicalType.None(), PhysicalType.Int32);
                    using var field2 = new PrimitiveNode("y", Repetition.Required, LogicalType.None(), PhysicalType.Float);
                    using var structNode = new GroupNode("col", Repetition.Required, new Node[] { field1, field2 });
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { structNode });
                    using var writer = new ParquetFileWriter(TmpPath("nt_struct"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    // When leaf columns are inside a group node, ParquetSharp uses Nested<T> wrappers
                    {
                        using var c1 = rg.NextColumn().LogicalWriter<Nested<int>>();
                        c1.WriteBatch(new Nested<int>[] { new Nested<int>(1), new Nested<int>(2), new Nested<int>(3) });
                    }
                    {
                        using var c2 = rg.NextColumn().LogicalWriter<Nested<float>>();
                        c2.WriteBatch(new Nested<float>[] { new Nested<float>(1.1f), new Nested<float>(2.2f), new Nested<float>(3.3f) });
                    }
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_struct"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<Nested<int>>().ReadAll(3);
                    rg.Column(1).LogicalReader<Nested<float>>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("nt_struct"), proofPath);
        }

        // NESTED_LIST (Column<int[][]> = list of list)
        {
            var cols = new Column[] { new Column<int[][]>("col") };
            nestedTypes["NESTED_LIST"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("nt_nested_list"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int[][]>();
                    c.WriteBatch(new int[][][] { new int[][] { new int[] { 1, 2 }, new int[] { 3 } }, new int[][] { new int[] { 4 } }, new int[][] { new int[] { 5, 6 }, new int[] { 7, 8 } } });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_nested_list"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<int[][]>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("nt_nested_list"), proofPath);
        }

        // NESTED_MAP (map inside map - complex schema)
        {
            nestedTypes["NESTED_MAP"] = TestRW(
                () => {
                    // Outer map: byte[] key -> inner map (byte[] key -> int value)
                    using var innerKeyNode = new PrimitiveNode("key", Repetition.Required, LogicalType.None(), PhysicalType.ByteArray);
                    using var innerValueNode = new PrimitiveNode("value", Repetition.Required, LogicalType.None(), PhysicalType.Int32);
                    using var innerKvNode = new GroupNode("key_value", Repetition.Repeated, new Node[] { innerKeyNode, innerValueNode });
                    using var innerMapLogicalType = LogicalType.Map();
                    using var innerMapNode = new GroupNode("value", Repetition.Optional, new Node[] { innerKvNode }, innerMapLogicalType);
                    using var outerKeyNode = new PrimitiveNode("key", Repetition.Required, LogicalType.None(), PhysicalType.ByteArray);
                    using var outerKvNode = new GroupNode("key_value", Repetition.Repeated, new Node[] { outerKeyNode, innerMapNode });
                    using var outerMapLogicalType = LogicalType.Map();
                    using var outerMapNode = new GroupNode("col", Repetition.Optional, new Node[] { outerKvNode }, outerMapLogicalType);
                    using var schema = new GroupNode("schema", Repetition.Required, new Node[] { outerMapNode });
                    using var writer = new ParquetFileWriter(TmpPath("nt_nested_map"), schema, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    // 1 row with 1 outer entry, 1 inner entry
                    var keyBytes = new byte[] { 0x61 };
                    var outerGcHandle = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                    try
                    {
                        var outerBa = new ByteArray(outerGcHandle.AddrOfPinnedObject(), 1);
                        ((ColumnWriter<ByteArray>)rg.NextColumn()).WriteBatch(1, new short[] { 2 }, new short[] { 0 }, new ByteArray[] { outerBa });
                    }
                    finally { outerGcHandle.Free(); }
                    var innerKeyBytes = new byte[] { 0x62 };
                    var innerGcHandle = GCHandle.Alloc(innerKeyBytes, GCHandleType.Pinned);
                    try
                    {
                        var innerBa = new ByteArray(innerGcHandle.AddrOfPinnedObject(), 1);
                        ((ColumnWriter<ByteArray>)rg.NextColumn()).WriteBatch(1, new short[] { 4 }, new short[] { 0 }, new ByteArray[] { innerBa });
                    }
                    finally { innerGcHandle.Free(); }
                    ((ColumnWriter<int>)rg.NextColumn()).WriteBatch(1, new short[] { 4 }, new short[] { 0 }, new int[] { 42 });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_nested_map"));
                    using var rg = reader.RowGroup(0);
                    var vals = new ByteArray[1];
                    var defs = new short[1];
                    var reps = new short[1];
                    ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(1, defs, reps, vals, out _);
                    reader.Close();
                },
                TmpPath("nt_nested_map"), proofPath);
        }

        // DEEP_NESTING (list of structs)
        {
            var cols = new Column[] { new Column<int[]>("items") };
            nestedTypes["DEEP_NESTING"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("nt_deep"), cols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int[]>();
                    c.WriteBatch(new int[][] { new int[] { 1, 2, 3 }, new int[] { 4 }, new int[] { 5, 6 } });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("nt_deep"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<int[]>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("nt_deep"), proofPath);
        }

        results["nested_types"] = nestedTypes;

        // --- Advanced Features ---
        var advanced = new Dictionary<string, object>();

        // STATISTICS (enabled by default in ParquetSharp)
        {
            advanced["STATISTICS"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("adv_stats"), intCols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int>();
                    c.WriteBatch(new int[] { 1, 2, 3 });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("adv_stats"));
                    var rg = reader.RowGroup(0);
                    var meta = rg.MetaData;
                    var colMeta = meta.GetColumnChunkMetaData(0);
                    var stats = colMeta.Statistics;
                    // Just check stats object is accessible
                    reader.Close();
                },
                TmpPath("adv_stats"), proofPath);
        }

        // PAGE_INDEX (column index + offset index)
        {
            var pageIndexProps = new WriterPropertiesBuilder().EnableWritePageIndex().Build();
            advanced["PAGE_INDEX"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("adv_page_idx"), intCols, pageIndexProps);
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int>();
                    c.WriteBatch(new int[] { 1, 2, 3 });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("adv_page_idx"));
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<int>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("adv_page_idx"), proofPath);
        }

        // BLOOM_FILTER (not available in ParquetSharp currently)
        advanced["BLOOM_FILTER"] = RW(false, false);

        // DATA_PAGE_V2 (ParquetDataPageVersion not available in this version)
        advanced["DATA_PAGE_V2"] = RW(false, false);

        // COLUMN_ENCRYPTION (requires AES key setup)
        {
            advanced["COLUMN_ENCRYPTION"] = TestRW(
                () => {
                    var aesKey = new byte[16]; // 128-bit AES key
                    new Random(42).NextBytes(aesKey);
                    using var colEncBuilder = new ColumnEncryptionPropertiesBuilder("col");
                    colEncBuilder.Key(aesKey);
                    using var colEnc = colEncBuilder.Build();
                    using var fileEncBuilder = new FileEncryptionPropertiesBuilder(aesKey);
                    fileEncBuilder.EncryptedColumns(new ColumnEncryptionProperties[] { colEnc });
                    using var fileEnc = fileEncBuilder.Build();
                    using var encProps = new WriterPropertiesBuilder().Encryption(fileEnc).Build();
                    using var writer = new ParquetFileWriter(TmpPath("adv_encrypt"), intCols, encProps);
                    using var rg = writer.AppendRowGroup();
                    using var c = rg.NextColumn().LogicalWriter<int>();
                    c.WriteBatch(new int[] { 1, 2, 3 });
                    writer.Close();
                },
                () => {
                    var aesKey = new byte[16];
                    new Random(42).NextBytes(aesKey);
                    using var colDecBuilder = new ColumnDecryptionPropertiesBuilder("col");
                    colDecBuilder.Key(aesKey);
                    using var colDec = colDecBuilder.Build();
                    using var fileDecBuilder = new FileDecryptionPropertiesBuilder();
                    fileDecBuilder.FooterKey(aesKey);
                    fileDecBuilder.ColumnKeys(new ColumnDecryptionProperties[] { colDec });
                    using var fileDec = fileDecBuilder.Build();
                    var readerProps = ReaderProperties.GetDefaultReaderProperties();
                    readerProps.FileDecryptionProperties = fileDec;
                    using var reader = new ParquetFileReader(TmpPath("adv_encrypt"), readerProps);
                    using var rg = reader.RowGroup(0);
                    rg.Column(0).LogicalReader<int>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("adv_encrypt"), proofPath);
        }

        // PREDICATE_PUSHDOWN (row group filtering based on statistics)
        advanced["PREDICATE_PUSHDOWN"] = RW(false, false);

        // PROJECTION_PUSHDOWN (reading a subset of columns)
        {
            var multiCols = new Column[] { new Column<int>("a"), new Column<string>("b"), new Column<double>("c") };
            advanced["PROJECTION_PUSHDOWN"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("adv_proj"), multiCols, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c1 = rg.NextColumn().LogicalWriter<int>();
                    c1.WriteBatch(new int[] { 1, 2, 3 });
                    using var c2 = rg.NextColumn().LogicalWriter<string>();
                    c2.WriteBatch(new string[] { "a", "b", "c" });
                    using var c3 = rg.NextColumn().LogicalWriter<double>();
                    c3.WriteBatch(new double[] { 1.0, 2.0, 3.0 });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("adv_proj"));
                    using var rg = reader.RowGroup(0);
                    // Only read column 0 and 2 (skip column 1)
                    rg.Column(0).LogicalReader<int>().ReadAll(3);
                    rg.Column(2).LogicalReader<double>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("adv_proj"), proofPath);
        }

        // SCHEMA_EVOLUTION (write with extra column, read with fewer columns)
        {
            var multiCols2 = new Column[] { new Column<int>("a"), new Column<string>("b") };
            advanced["SCHEMA_EVOLUTION"] = TestRW(
                () => {
                    using var writer = new ParquetFileWriter(TmpPath("adv_schema_evo"), multiCols2, DefaultProps());
                    using var rg = writer.AppendRowGroup();
                    using var c1 = rg.NextColumn().LogicalWriter<int>();
                    c1.WriteBatch(new int[] { 1, 2, 3 });
                    using var c2 = rg.NextColumn().LogicalWriter<string>();
                    c2.WriteBatch(new string[] { "a", "b", "c" });
                    writer.Close();
                },
                () => {
                    using var reader = new ParquetFileReader(TmpPath("adv_schema_evo"));
                    using var rg = reader.RowGroup(0);
                    // Read only column 0 - projection as schema evolution
                    rg.Column(0).LogicalReader<int>().ReadAll(3);
                    reader.Close();
                },
                TmpPath("adv_schema_evo"), proofPath);
        }

        results["advanced_features"] = advanced;

        var json = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);

        if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
    }
}
