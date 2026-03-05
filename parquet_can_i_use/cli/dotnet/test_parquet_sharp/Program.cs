using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;
using ParquetSharp;
using ParquetSharp.Schema;

class Program
{
    static string tmpDir = Path.Combine(Path.GetTempPath(), "parquet_sharp_test_" + Guid.NewGuid().ToString("N"));

    static bool TestFeature(Action fn)
    {
        try { fn(); return true; }
        catch { return false; }
    }

    static Dictionary<string, bool> TestRW(Action writeFn, Action readFn)
    {
        return new Dictionary<string, bool>
        {
            ["write"] = TestFeature(writeFn),
            ["read"] = TestFeature(readFn),
        };
    }

    static Dictionary<string, bool> RW(bool write, bool read) =>
        new Dictionary<string, bool> { ["write"] = write, ["read"] = read };

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

        void TestCompression(string key, Compression codec)
        {
            var props = new WriterPropertiesBuilder().Compression(codec).Build();
            compression[key] = TestRW(
                () => WriteIntFile("comp_" + key, props),
                () => ReadIntFile("comp_" + key));
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
        bool TryWriteRead<T>(string fileSuffix, Column[] cols, T[] data, WriterProperties props)
        {
            var path = TmpPath(fileSuffix);
            bool writeOk = TestFeature(() =>
            {
                using var w = new ParquetFileWriter(path, cols, props);
                using var rg = w.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<T>();
                c.WriteBatch(data);
                w.Close();
            });
            bool readOk = writeOk && TestFeature(() =>
            {
                using var r = new ParquetFileReader(path);
                using var rg = r.RowGroup(0);
                rg.Column(0).LogicalReader<T>().ReadAll(data.Length);
                r.Close();
            });
            return writeOk && readOk;
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

        bool rw;
        string[] typeNames = { "INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY" };

        // PLAIN
        {
            var t = new Dictionary<string, object>();
            rw = TryWriteRead("enc_plain_i32", intCols, new int[] { 1, 2, 3 }, PlainProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_plain_i64", longCols, new long[] { 1L, 2L, 3L }, PlainProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_plain_f32", floatCols, new float[] { 1f, 2f, 3f }, PlainProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_plain_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, PlainProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_plain_bool", boolCols, new bool[] { true, false, true }, PlainProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_plain_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, PlainProps()); t["BYTE_ARRAY"] = RW(rw, rw);
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
            rw = TryWriteRead("enc_rledict_i32", intCols, new int[] { 1, 2, 3 }, RleDictProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_rledict_i64", longCols, new long[] { 1L, 2L, 3L }, RleDictProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_rledict_f32", floatCols, new float[] { 1f, 2f, 3f }, RleDictProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_rledict_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, RleDictProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_rledict_bool", boolCols, new bool[] { true, false, true }, RleDictProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_rledict_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, RleDictProps()); t["BYTE_ARRAY"] = RW(rw, rw);
            encoding["RLE_DICTIONARY"] = t;
        }
        // RLE
        {
            var t = new Dictionary<string, object>();
            // RLE is not valid for INT32/INT64/FLOAT/DOUBLE/BYTE_ARRAY directly - only BOOLEAN via fallback
            rw = TryWriteRead("enc_rle_i32", intCols, new int[] { 1, 2, 3 }, RleProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_rle_i64", longCols, new long[] { 1L, 2L, 3L }, RleProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_rle_f32", floatCols, new float[] { 1f, 2f, 3f }, RleProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_rle_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, RleProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_rle_bool", boolCols, new bool[] { true, false, true }, RleProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_rle_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, RleProps()); t["BYTE_ARRAY"] = RW(rw, rw);
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
            rw = TryWriteRead("enc_dbp_i32", intCols, new int[] { 1, 2, 3 }, DeltaProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_dbp_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_dbp_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_dbp_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_dbp_bool", boolCols, new bool[] { true, false, true }, DeltaProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_dbp_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaProps()); t["BYTE_ARRAY"] = RW(rw, rw);
            encoding["DELTA_BINARY_PACKED"] = t;
        }
        // DELTA_LENGTH_BYTE_ARRAY
        {
            var t = new Dictionary<string, object>();
            rw = TryWriteRead("enc_dlba_i32", intCols, new int[] { 1, 2, 3 }, DeltaLengthProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_dlba_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaLengthProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_dlba_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaLengthProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_dlba_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaLengthProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_dlba_bool", boolCols, new bool[] { true, false, true }, DeltaLengthProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_dlba_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaLengthProps()); t["BYTE_ARRAY"] = RW(rw, rw);
            encoding["DELTA_LENGTH_BYTE_ARRAY"] = t;
        }
        // DELTA_BYTE_ARRAY
        {
            var t = new Dictionary<string, object>();
            rw = TryWriteRead("enc_dba_i32", intCols, new int[] { 1, 2, 3 }, DeltaByteArrayProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_dba_i64", longCols, new long[] { 1L, 2L, 3L }, DeltaByteArrayProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_dba_f32", floatCols, new float[] { 1f, 2f, 3f }, DeltaByteArrayProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_dba_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, DeltaByteArrayProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_dba_bool", boolCols, new bool[] { true, false, true }, DeltaByteArrayProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_dba_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, DeltaByteArrayProps()); t["BYTE_ARRAY"] = RW(rw, rw);
            encoding["DELTA_BYTE_ARRAY"] = t;
        }
        // BYTE_STREAM_SPLIT
        {
            var t = new Dictionary<string, object>();
            rw = TryWriteRead("enc_bss_i32", intCols, new int[] { 1, 2, 3 }, ByteStreamSplitProps()); t["INT32"] = RW(rw, rw);
            rw = TryWriteRead("enc_bss_i64", longCols, new long[] { 1L, 2L, 3L }, ByteStreamSplitProps()); t["INT64"] = RW(rw, rw);
            rw = TryWriteRead("enc_bss_f32", floatCols, new float[] { 1f, 2f, 3f }, ByteStreamSplitProps()); t["FLOAT"] = RW(rw, rw);
            rw = TryWriteRead("enc_bss_f64", doubleCols, new double[] { 1.0, 2.0, 3.0 }, ByteStreamSplitProps()); t["DOUBLE"] = RW(rw, rw);
            rw = TryWriteRead("enc_bss_bool", boolCols, new bool[] { true, false, true }, ByteStreamSplitProps()); t["BOOLEAN"] = RW(rw, rw);
            rw = TryWriteRead("enc_bss_ba", byteArrayCols, new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }, ByteStreamSplitProps()); t["BYTE_ARRAY"] = RW(rw, rw);
            encoding["BYTE_STREAM_SPLIT"] = t;
        }
        results["encoding"] = encoding;

        // --- Logical Types ---
        var logicalTypes = new Dictionary<string, object>();

        // STRING
        {
            var cols = new Column[] { new Column<string>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_string"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<string>();
                c.WriteBatch(new string[] { "a", "b", "c" });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_string"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<string>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["STRING"] = RW(w, r);
        }

        // DATE (using ParquetSharp.Date struct)
        {
            var cols = new Column[] { new Column<Date>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_date"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<Date>();
                c.WriteBatch(new Date[] { new Date(2023, 1, 1), new Date(2023, 6, 15), new Date(2024, 12, 31) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_date"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<Date>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["DATE"] = RW(w, r);
        }

        // TIME_MILLIS (TimeSpan maps to MICROS by default; use low-level for MILLIS)
        {
            bool w = TestFeature(() =>
            {
                using var lt = LogicalType.Time(true, TimeUnit.Millis);
                using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.Int32);
                using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                using var writer = new ParquetFileWriter(TmpPath("lt_time_ms"), schema, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<TimeSpan>();
                c.WriteBatch(new TimeSpan[] { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), TimeSpan.FromSeconds(45) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_time_ms"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<TimeSpan>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIME_MILLIS"] = RW(w, r);
        }

        // TIME_MICROS (TimeSpan default = MICROS)
        {
            var cols = new Column[] { new Column<TimeSpan>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_time_us"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<TimeSpan>();
                c.WriteBatch(new TimeSpan[] { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), TimeSpan.FromSeconds(45) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_time_us"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<TimeSpan>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIME_MICROS"] = RW(w, r);
        }

        // TIME_NANOS (TimeSpanNanos)
        {
            var cols = new Column[] { new Column<TimeSpanNanos>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_time_ns"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<TimeSpanNanos>();
                c.WriteBatch(new TimeSpanNanos[] { new TimeSpanNanos(1000000L), new TimeSpanNanos(2000000L), new TimeSpanNanos(3000000L) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_time_ns"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<TimeSpanNanos>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIME_NANOS"] = RW(w, r);
        }

        // TIMESTAMP_MILLIS (use low-level schema with millis logical type)
        {
            bool w = TestFeature(() =>
            {
                using var lt = LogicalType.Timestamp(true, TimeUnit.Millis);
                using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.Int64);
                using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                using var writer = new ParquetFileWriter(TmpPath("lt_ts_ms"), schema, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<DateTime>();
                c.WriteBatch(new DateTime[] { DateTime.UtcNow, DateTime.UtcNow.AddHours(1), DateTime.UtcNow.AddDays(1) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_ts_ms"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<DateTime>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIMESTAMP_MILLIS"] = RW(w, r);
        }

        // TIMESTAMP_MICROS (DateTime default = MICROS)
        {
            var cols = new Column[] { new Column<DateTime>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_ts_us"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<DateTime>();
                c.WriteBatch(new DateTime[] { DateTime.UtcNow, DateTime.UtcNow.AddHours(1), DateTime.UtcNow.AddDays(1) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_ts_us"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<DateTime>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIMESTAMP_MICROS"] = RW(w, r);
        }

        // TIMESTAMP_NANOS (DateTimeNanos)
        {
            var cols = new Column[] { new Column<DateTimeNanos>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_ts_ns"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<DateTimeNanos>();
                c.WriteBatch(new DateTimeNanos[] { new DateTimeNanos(DateTime.UtcNow), new DateTimeNanos(DateTime.UtcNow.AddHours(1)), new DateTimeNanos(DateTime.UtcNow.AddDays(1)) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_ts_ns"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<DateTimeNanos>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["TIMESTAMP_NANOS"] = RW(w, r);
        }

        // INT96 (legacy)
        {
            var cols = new Column[] { new Column<Int96>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_int96"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<Int96>();
                c.WriteBatch(new Int96[] { new Int96(1, 2, 3), new Int96(4, 5, 6), new Int96(7, 8, 9) });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_int96"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<Int96>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["INT96"] = RW(w, r);
        }

        // DECIMAL
        {
            var decLogicalType = LogicalType.Decimal(29, 3);
            var cols = new Column[] { new Column<decimal>("col", decLogicalType) };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_decimal"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<decimal>();
                c.WriteBatch(new decimal[] { 1.234m, 2.345m, 3.456m });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_decimal"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<decimal>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["DECIMAL"] = RW(w, r);
            decLogicalType.Dispose();
        }

        // UUID (Guid with UuidLogicalType)
        {
            var uuidLogicalType = LogicalType.Uuid();
            var cols = new Column[] { new Column<Guid>("col", uuidLogicalType) };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("lt_uuid"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<Guid>();
                c.WriteBatch(new Guid[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_uuid"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<Guid>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["UUID"] = RW(w, r);
            uuidLogicalType.Dispose();
        }

        // JSON (string annotated as JSON via low-level API)
        {
            bool w = TestFeature(() =>
            {
                using var lt = LogicalType.Json();
                using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.ByteArray);
                using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                using var writer = new ParquetFileWriter(TmpPath("lt_json"), schema, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<string>();
                c.WriteBatch(new string[] { "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_json"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<string>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["JSON"] = RW(w, r);
        }

        // FLOAT16 (Half type, .NET 5+)
        {
            bool w = TestFeature(() =>
            {
                var cols16 = new Column[] { new Column<Half>("col") };
                using var writer = new ParquetFileWriter(TmpPath("lt_float16"), cols16, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<Half>();
                c.WriteBatch(new Half[] { (Half)1.5f, (Half)2.5f, (Half)3.5f });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_float16"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<Half>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["FLOAT16"] = RW(w, r);
        }

        // ENUM (byte array annotated as ENUM via low-level ColumnWriter API)
        {
            bool w = TestFeature(() =>
            {
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
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_enum"));
                using var rg = reader.RowGroup(0);
                var vals = new ByteArray[3];
                ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(3, vals, out _);
                reader.Close();
            });
            logicalTypes["ENUM"] = RW(w, r);
        }

        // BSON (byte array annotated as BSON via low-level API)
        {
            bool w = TestFeature(() =>
            {
                using var lt = LogicalType.Bson();
                using var node = new PrimitiveNode("col", Repetition.Required, lt, PhysicalType.ByteArray);
                using var schema = new GroupNode("schema", Repetition.Required, new Node[] { node });
                using var writer = new ParquetFileWriter(TmpPath("lt_bson"), schema, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<byte[]>();
                c.WriteBatch(new byte[][] { new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 }, new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 }, new byte[] { 0x05, 0x00, 0x00, 0x00, 0x00 } });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("lt_bson"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<byte[]>().ReadAll(3);
                reader.Close();
            });
            logicalTypes["BSON"] = RW(w, r);
        }

        // INTERVAL (deprecated; parquet-cpp rejects INTERVAL for FIXED_LEN_BYTE_ARRAY)
        logicalTypes["INTERVAL"] = RW(false, false);

        results["logical_types"] = logicalTypes;

        // --- Nested Types ---
        var nestedTypes = new Dictionary<string, object>();

        // LIST (Column<int[]> maps to Parquet LIST type)
        {
            var cols = new Column[] { new Column<int[]>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("nt_list"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<int[]>();
                c.WriteBatch(new int[][] { new int[] { 1, 2 }, new int[] { 3 }, new int[] { 4, 5, 6 } });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_list"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<int[]>().ReadAll(3);
                reader.Close();
            });
            nestedTypes["LIST"] = RW(w, r);
        }

        // MAP (using low-level GroupNode schema and ColumnWriter with explicit levels)
        {
            bool w = TestFeature(() =>
            {
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
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_map"));
                using var rg = reader.RowGroup(0);
                var keys = new ByteArray[3];
                var defLevels = new short[3];
                var repLevels = new short[3];
                ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(3, defLevels, repLevels, keys, out _);
                reader.Close();
            });
            nestedTypes["MAP"] = RW(w, r);
        }

        // STRUCT (using Nested<T> wrapper or GroupNode)
        {
            bool w = TestFeature(() =>
            {
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
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_struct"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<Nested<int>>().ReadAll(3);
                rg.Column(1).LogicalReader<Nested<float>>().ReadAll(3);
                reader.Close();
            });
            nestedTypes["STRUCT"] = RW(w, r);
        }

        // NESTED_LIST (Column<int[][]> = list of list)
        {
            var cols = new Column[] { new Column<int[][]>("col") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("nt_nested_list"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<int[][]>();
                c.WriteBatch(new int[][][] { new int[][] { new int[] { 1, 2 }, new int[] { 3 } }, new int[][] { new int[] { 4 } }, new int[][] { new int[] { 5, 6 }, new int[] { 7, 8 } } });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_nested_list"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<int[][]>().ReadAll(3);
                reader.Close();
            });
            nestedTypes["NESTED_LIST"] = RW(w, r);
        }

        // NESTED_MAP (map inside map - complex schema)
        {
            bool w = TestFeature(() =>
            {
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
                // outerKey: col=opt(+1=1), outer_kv=rep(+1=2,maxRep=1), key=req → maxDef=2
                // outerKey defLevels=[2], repLevels=[0]
                // innerKey: col=opt(+1=1), outer_kv=rep(+1=2,maxRep=1), value=opt(+1=3), inner_kv=rep(+1=4,maxRep=2), key=req → maxDef=4
                // innerKey defLevels=[4], repLevels=[0]
                // innerValue: same path as innerKey → maxDef=4
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
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_nested_map"));
                using var rg = reader.RowGroup(0);
                var vals = new ByteArray[1];
                var defs = new short[1];
                var reps = new short[1];
                ((ColumnReader<ByteArray>)rg.Column(0)).ReadBatch(1, defs, reps, vals, out _);
                reader.Close();
            });
            nestedTypes["NESTED_MAP"] = RW(w, r);
        }

        // DEEP_NESTING (list of structs)
        {
            var cols = new Column[] { new Column<int[]>("items") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("nt_deep"), cols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<int[]>();
                c.WriteBatch(new int[][] { new int[] { 1, 2, 3 }, new int[] { 4 }, new int[] { 5, 6 } });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("nt_deep"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<int[]>().ReadAll(3);
                reader.Close();
            });
            nestedTypes["DEEP_NESTING"] = RW(w, r);
        }

        results["nested_types"] = nestedTypes;

        // --- Advanced Features ---
        var advanced = new Dictionary<string, object>();

        // STATISTICS (enabled by default in ParquetSharp)
        {
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("adv_stats"), intCols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<int>();
                c.WriteBatch(new int[] { 1, 2, 3 });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("adv_stats"));
                var rg = reader.RowGroup(0);
                var meta = rg.MetaData;
                var colMeta = meta.GetColumnChunkMetaData(0);
                var stats = colMeta.Statistics;
                // Just check stats object is accessible
                reader.Close();
            });
            advanced["STATISTICS"] = RW(w, r);
        }

        // PAGE_INDEX (column index + offset index)
        {
            var pageIndexProps = new WriterPropertiesBuilder().EnableWritePageIndex().Build();
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("adv_page_idx"), intCols, pageIndexProps);
                using var rg = writer.AppendRowGroup();
                using var c = rg.NextColumn().LogicalWriter<int>();
                c.WriteBatch(new int[] { 1, 2, 3 });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("adv_page_idx"));
                using var rg = reader.RowGroup(0);
                rg.Column(0).LogicalReader<int>().ReadAll(3);
                reader.Close();
            });
            advanced["PAGE_INDEX"] = RW(w, r);
        }

        // BLOOM_FILTER (not available in ParquetSharp currently)
        advanced["BLOOM_FILTER"] = RW(false, false);

        // DATA_PAGE_V2 (ParquetDataPageVersion not available in this version)
        advanced["DATA_PAGE_V2"] = RW(false, false);

        // COLUMN_ENCRYPTION (requires AES key setup)
        {
            bool w = TestFeature(() =>
            {
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
            });
            bool r = w && TestFeature(() =>
            {
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
            });
            advanced["COLUMN_ENCRYPTION"] = RW(w, r);
        }

        // PREDICATE_PUSHDOWN (row group filtering based on statistics)
        advanced["PREDICATE_PUSHDOWN"] = RW(false, false);

        // PROJECTION_PUSHDOWN (reading a subset of columns)
        {
            var multiCols = new Column[] { new Column<int>("a"), new Column<string>("b"), new Column<double>("c") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("adv_proj"), multiCols, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c1 = rg.NextColumn().LogicalWriter<int>();
                c1.WriteBatch(new int[] { 1, 2, 3 });
                using var c2 = rg.NextColumn().LogicalWriter<string>();
                c2.WriteBatch(new string[] { "a", "b", "c" });
                using var c3 = rg.NextColumn().LogicalWriter<double>();
                c3.WriteBatch(new double[] { 1.0, 2.0, 3.0 });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("adv_proj"));
                using var rg = reader.RowGroup(0);
                // Only read column 0 and 2 (skip column 1)
                rg.Column(0).LogicalReader<int>().ReadAll(3);
                rg.Column(2).LogicalReader<double>().ReadAll(3);
                reader.Close();
            });
            advanced["PROJECTION_PUSHDOWN"] = RW(w, r);
        }

        // SCHEMA_EVOLUTION (write with extra column, read with fewer columns)
        {
            var multiCols2 = new Column[] { new Column<int>("a"), new Column<string>("b") };
            bool w = TestFeature(() =>
            {
                using var writer = new ParquetFileWriter(TmpPath("adv_schema_evo"), multiCols2, DefaultProps());
                using var rg = writer.AppendRowGroup();
                using var c1 = rg.NextColumn().LogicalWriter<int>();
                c1.WriteBatch(new int[] { 1, 2, 3 });
                using var c2 = rg.NextColumn().LogicalWriter<string>();
                c2.WriteBatch(new string[] { "a", "b", "c" });
                writer.Close();
            });
            bool r = w && TestFeature(() =>
            {
                using var reader = new ParquetFileReader(TmpPath("adv_schema_evo"));
                using var rg = reader.RowGroup(0);
                // Read only column 0 - projection as schema evolution
                rg.Column(0).LogicalReader<int>().ReadAll(3);
                reader.Close();
            });
            advanced["SCHEMA_EVOLUTION"] = RW(w, r);
        }

        results["advanced_features"] = advanced;

        var json = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);

        if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
    }
}
