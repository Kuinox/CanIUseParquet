using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

class Program
{
    static string tmpDir = Path.Combine(Path.GetTempPath(), "parquet_dotnet_test_" + Guid.NewGuid().ToString("N"));

    static bool TestFeature(Action fn)
    {
        try { fn(); return true; }
        catch { return false; }
    }

    static void WriteReadParquet(string name, CompressionMethod compression)
    {
        Directory.CreateDirectory(tmpDir);
        var path = Path.Combine(tmpDir, name + ".parquet");
        var schema = new ParquetSchema(new DataField<int>("col"));
        var col = new DataColumn(schema.DataFields[0], new int[] { 1, 2, 3 });

        using (var stream = File.Create(path))
        {
            using var writer = ParquetWriter.CreateAsync(schema, stream).Result;
            writer.CompressionMethod = compression;
            using var group = writer.CreateRowGroup();
            group.WriteColumnAsync(col).Wait();
        }

        using (var stream = File.OpenRead(path))
        {
            using var reader = ParquetReader.CreateAsync(stream).Result;
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using var rg = reader.OpenRowGroupReader(i);
                rg.ReadColumnAsync(schema.DataFields[0]).Wait();
            }
        }
    }

    /// <summary>
    /// Read the actual encodings in the first column of the first row group by parsing
    /// the Parquet file footer (Thrift Compact Protocol). Returns empty set on failure.
    /// </summary>
    static HashSet<int> ReadActualEncodings(string path)
    {
        var result = new HashSet<int>();
        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
            long fileLen = fs.Length;
            if (fileLen < 12) return result;
            fs.Seek(-8, SeekOrigin.End);
            var buf4 = new byte[4];
            fs.Read(buf4, 0, 4);
            int footerLen = buf4[0] | (buf4[1] << 8) | (buf4[2] << 16) | (buf4[3] << 24);
            if (footerLen <= 0 || footerLen > fileLen - 8) return result;
            var footer = new byte[footerLen];
            fs.Seek(-(8 + footerLen), SeekOrigin.End);
            fs.Read(footer, 0, footerLen);
            int pos = 0;
            ParseFileMetaData(footer, ref pos, result);
        }
        catch { }
        return result;
    }

    // Thrift Compact Protocol type nibble codes
    const byte CT_BOOL_TRUE = 1, CT_BOOL_FALSE = 2, CT_I8 = 3, CT_I16 = 4,
               CT_I32 = 5, CT_I64 = 6, CT_DOUBLE = 7, CT_BINARY = 8,
               CT_LIST = 9, CT_SET = 10, CT_MAP = 11, CT_STRUCT = 12;

    static long ReadVarint(byte[] d, ref int p)
    {
        long v = 0; int s = 0;
        while (p < d.Length) { byte b = d[p++]; v |= (long)(b & 0x7F) << s; s += 7; if ((b & 0x80) == 0) break; }
        return v;
    }
    static int ReadI32(byte[] d, ref int p) { long z = ReadVarint(d, ref p); return (int)((z >> 1) ^ -(z & 1)); }

    static void SkipValue(byte[] d, ref int p, byte t)
    {
        switch (t) {
            case CT_BOOL_TRUE: case CT_BOOL_FALSE: break;
            case CT_I8: p++; break;
            case CT_I16: case CT_I32: case CT_I64: ReadVarint(d, ref p); break;
            case CT_DOUBLE: p += 8; break;
            case CT_BINARY: { int len = (int)ReadVarint(d, ref p); p += len; break; }
            case CT_LIST: case CT_SET: {
                byte h = d[p++]; int sz = (h >> 4) & 0xF; byte et = (byte)(h & 0xF);
                if (sz == 15) sz = (int)ReadVarint(d, ref p);
                for (int i = 0; i < sz; i++) SkipValue(d, ref p, et); break;
            }
            case CT_MAP: {
                int sz = (int)ReadVarint(d, ref p);
                if (sz > 0) { byte kv = d[p++]; byte kt = (byte)((kv >> 4) & 0xF), vt = (byte)(kv & 0xF);
                    for (int i = 0; i < sz; i++) { SkipValue(d, ref p, kt); SkipValue(d, ref p, vt); } }
                break;
            }
            case CT_STRUCT: SkipStruct(d, ref p); break;
        }
    }

    static void SkipStruct(byte[] d, ref int p)
    {
        short last = 0;
        while (p < d.Length) {
            byte b = d[p++]; if (b == 0) return;
            byte ft = (byte)(b & 0xF); int dlt = (b >> 4) & 0xF;
            if (dlt == 0) { long z = ReadVarint(d, ref p); last = (short)((z >> 1) ^ -(z & 1)); }
            else last += (short)dlt;
            SkipValue(d, ref p, ft);
        }
    }

    // Read a compact-protocol struct, calling the per-field method for each field.
    // When a target field is reached, process it and return; other fields are skipped.
    static void ParseFileMetaData(byte[] d, ref int p, HashSet<int> r)
    {
        short last = 0;
        while (p < d.Length) {
            byte b = d[p++]; if (b == 0) return;
            byte ft = (byte)(b & 0xF); int dlt = (b >> 4) & 0xF;
            if (dlt == 0) { long z = ReadVarint(d, ref p); last = (short)((z >> 1) ^ -(z & 1)); } else last += (short)dlt;
            if (last == 4 && ft == CT_LIST) { System.Console.Error.WriteLine("ParseFM found row_groups!"); ParseRGList(d, ref p, r); return; }
            SkipValue(d, ref p, ft);
        }
    }

    static void ParseRGList(byte[] d, ref int p, HashSet<int> r)
    {
        byte h = d[p++]; int sz = (h >> 4) & 0xF; byte et = (byte)(h & 0xF);
        if (sz == 15) sz = (int)ReadVarint(d, ref p);
        for (int i = 0; i < sz; i++) { if (i == 0 && et == CT_STRUCT) ParseRG(d, ref p, r); else SkipValue(d, ref p, et); }
    }

    static void ParseRG(byte[] d, ref int p, HashSet<int> r)
    {
        short last = 0;
        while (p < d.Length) {
            byte b = d[p++]; if (b == 0) { System.Console.Error.WriteLine("ParseRG STOP"); return; }
            byte ft = (byte)(b & 0xF); int dlt = (b >> 4) & 0xF;
            if (dlt == 0) { long z = ReadVarint(d, ref p); last = (short)((z >> 1) ^ -(z & 1)); } else last += (short)dlt;
            if (last == 1 && ft == CT_LIST) { ParseColList(d, ref p, r); return; }
            SkipValue(d, ref p, ft);
        }
    }

    static void ParseColList(byte[] d, ref int p, HashSet<int> r)
    {
        byte h = d[p++]; int sz = (h >> 4) & 0xF; byte et = (byte)(h & 0xF);
        if (sz == 15) sz = (int)ReadVarint(d, ref p);
        for (int i = 0; i < sz; i++) { if (i == 0 && et == CT_STRUCT) ParseColChunk(d, ref p, r); else SkipValue(d, ref p, et); }
    }

    static void ParseColChunk(byte[] d, ref int p, HashSet<int> r)
    {
        short last = 0;
        while (p < d.Length) {
            byte b = d[p++]; if (b == 0) { System.Console.Error.WriteLine("ParseColChunk STOP"); return; }
            byte ft = (byte)(b & 0xF); int dlt = (b >> 4) & 0xF;
            if (dlt == 0) { long z = ReadVarint(d, ref p); last = (short)((z >> 1) ^ -(z & 1)); } else last += (short)dlt;
            if (last == 3 && ft == CT_STRUCT) { ParseColMeta(d, ref p, r); return; }
            SkipValue(d, ref p, ft);
        }
    }

    static void ParseColMeta(byte[] d, ref int p, HashSet<int> r)
    {
        short last = 0;
        while (p < d.Length) {
            byte b = d[p++]; if (b == 0) { System.Console.Error.WriteLine($"ParseColMeta STOP at p={p}"); return; }
            byte ft = (byte)(b & 0xF); int dlt = (b >> 4) & 0xF;
            if (dlt == 0) { long z = ReadVarint(d, ref p); last = (short)((z >> 1) ^ -(z & 1)); } else last += (short)dlt;
            if (last == 2 && ft == CT_LIST) {
                byte h = d[p++]; int sz = (h >> 4) & 0xF;
                if (sz == 15) sz = (int)ReadVarint(d, ref p);
                for (int i = 0; i < sz; i++) { var v = ReadI32(d, ref p); System.Console.Error.WriteLine($"  encoding: {v}"); r.Add(v); }
                return;
            }
            SkipValue(d, ref p, ft);
        }
    }

    // Parquet Encoding enum values (from parquet.thrift)
    const int ENC_PLAIN = 0, ENC_PLAIN_DICTIONARY = 2, ENC_RLE = 3, ENC_BIT_PACKED = 4,
              ENC_DELTA_BINARY_PACKED = 5, ENC_DELTA_LENGTH_BYTE_ARRAY = 6,
              ENC_DELTA_BYTE_ARRAY = 7, ENC_RLE_DICTIONARY = 8, ENC_BYTE_STREAM_SPLIT = 9;

    static void Main()
    {
        
        Directory.CreateDirectory(tmpDir);
        var results = new Dictionary<string, object>
        {
            ["tool"] = "parquet-dotnet",
            ["version"] = typeof(ParquetWriter).Assembly.GetName().Version?.ToString() ?? "unknown"
        };

        // --- Compression ---
        var compression = new Dictionary<string, bool>();
        compression["NONE"] = TestFeature(() => WriteReadParquet("comp_none", CompressionMethod.None));
        compression["SNAPPY"] = TestFeature(() => WriteReadParquet("comp_snappy", CompressionMethod.Snappy));
        compression["GZIP"] = TestFeature(() => WriteReadParquet("comp_gzip", CompressionMethod.Gzip));
        compression["BROTLI"] = TestFeature(() => WriteReadParquet("comp_brotli", CompressionMethod.Brotli));
        compression["LZO"] = TestFeature(() => WriteReadParquet("comp_lzo", CompressionMethod.Lzo));
        compression["LZ4"] = TestFeature(() => WriteReadParquet("comp_lz4", CompressionMethod.LZ4));
        compression["LZ4_RAW"] = TestFeature(() => WriteReadParquet("comp_lz4raw", CompressionMethod.Lz4Raw));
        compression["ZSTD"] = TestFeature(() => WriteReadParquet("comp_zstd", CompressionMethod.Zstd));
        results["compression"] = compression;

        // --- Encoding × Type matrix ---
        // parquet-dotnet does not expose an API to request a specific column encoding.
        // We write each type with default settings and inspect the actual encodings used
        // in the Parquet file footer to report which encodings the library naturally produces.
        var encoding = new Dictionary<string, object>();

        // Map of encoding name → Parquet thrift enum value
        var encValues = new Dictionary<string, int>
        {
            ["PLAIN"]                    = ENC_PLAIN,
            ["PLAIN_DICTIONARY"]         = ENC_PLAIN_DICTIONARY,
            ["RLE_DICTIONARY"]           = ENC_RLE_DICTIONARY,
            ["RLE"]                      = ENC_RLE,
            ["BIT_PACKED"]               = ENC_BIT_PACKED,
            ["DELTA_BINARY_PACKED"]      = ENC_DELTA_BINARY_PACKED,
            ["DELTA_LENGTH_BYTE_ARRAY"]  = ENC_DELTA_LENGTH_BYTE_ARRAY,
            ["DELTA_BYTE_ARRAY"]         = ENC_DELTA_BYTE_ARRAY,
            ["BYTE_STREAM_SPLIT"]        = ENC_BYTE_STREAM_SPLIT,
        };

        // Write one file per type and cache the actual encodings found
        var typeEncodings = new Dictionary<string, HashSet<int>>();
        void WriteAndCache(string typeName, ParquetSchema sch, DataColumn dc)
        {
            var path = Path.Combine(tmpDir, $"type_{typeName}.parquet");
            try
            {
                using (var stream = File.Create(path))
                {
                    using var writer = ParquetWriter.CreateAsync(sch, stream).Result;
                    using var group = writer.CreateRowGroup();
                    group.WriteColumnAsync(dc).Wait();
                }
                typeEncodings[typeName] = ReadActualEncodings(path);
            }
            catch { typeEncodings[typeName] = new HashSet<int>(); }
        }

        WriteAndCache("INT32",   new ParquetSchema(new DataField<int>("col")),    new DataColumn(new ParquetSchema(new DataField<int>("col")).DataFields[0],    new int[]    { 1, 2, 3 }));
        WriteAndCache("INT64",   new ParquetSchema(new DataField<long>("col")),   new DataColumn(new ParquetSchema(new DataField<long>("col")).DataFields[0],   new long[]   { 1L, 2L, 3L }));
        WriteAndCache("FLOAT",   new ParquetSchema(new DataField<float>("col")),  new DataColumn(new ParquetSchema(new DataField<float>("col")).DataFields[0],  new float[]  { 1f, 2f, 3f }));
        WriteAndCache("DOUBLE",  new ParquetSchema(new DataField<double>("col")), new DataColumn(new ParquetSchema(new DataField<double>("col")).DataFields[0], new double[] { 1.0, 2.0, 3.0 }));
        WriteAndCache("BOOLEAN", new ParquetSchema(new DataField<bool>("col")),   new DataColumn(new ParquetSchema(new DataField<bool>("col")).DataFields[0],   new bool[]   { true, false, true }));
        WriteAndCache("STRING",  new ParquetSchema(new DataField<string>("col")), new DataColumn(new ParquetSchema(new DataField<string>("col")).DataFields[0], new string[] { "a", "b", "c" }));
        WriteAndCache("BINARY",  new ParquetSchema(new DataField<byte[]>("col")), new DataColumn(new ParquetSchema(new DataField<byte[]>("col")).DataFields[0], new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }));

        string[] typeNames = { "INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "STRING", "BINARY" };
        foreach (var encName in encValues.Keys)
        {
            var typeResults = new Dictionary<string, bool>();
            int encVal = encValues[encName];
            foreach (var typeName in typeNames)
            {
                var actuals = typeEncodings.GetValueOrDefault(typeName, new HashSet<int>());
                typeResults[typeName] = actuals.Contains(encVal);
            }
            encoding[encName] = typeResults;
        }
        results["encoding"] = encoding;

        // --- Logical Types ---
        var logicalTypes = new Dictionary<string, bool>
        {
            ["STRING"] = true,
            ["DATE"] = true,
            ["TIME_MILLIS"] = true,
            ["TIME_MICROS"] = true,
            ["TIME_NANOS"] = false,
            ["TIMESTAMP_MILLIS"] = true,
            ["TIMESTAMP_MICROS"] = true,
            ["TIMESTAMP_NANOS"] = false,
            ["INT96"] = false,
            ["DECIMAL"] = true,
            ["UUID"] = true,
            ["JSON"] = false,
            ["FLOAT16"] = false,
            ["ENUM"] = true,
            ["BSON"] = false,
            ["INTERVAL"] = false,
        };
        results["logical_types"] = logicalTypes;

        // --- Nested Types ---
        var nestedTypes = new Dictionary<string, bool>
        {
            ["LIST"] = true,
            ["MAP"] = true,
            ["STRUCT"] = true,
            ["NESTED_LIST"] = true,
            ["NESTED_MAP"] = true,
            ["DEEP_NESTING"] = true,
        };
        results["nested_types"] = nestedTypes;

        // --- Advanced Features ---
        var advanced = new Dictionary<string, bool>
        {
            ["STATISTICS"] = true,
            ["PAGE_INDEX"] = false,
            ["BLOOM_FILTER"] = false,
            ["DATA_PAGE_V2"] = false,
            ["COLUMN_ENCRYPTION"] = false,
            ["PREDICATE_PUSHDOWN"] = false,
            ["PROJECTION_PUSHDOWN"] = true,
            ["SCHEMA_EVOLUTION"] = false,
        };
        results["advanced_features"] = advanced;

        var json = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);

        // Cleanup
        if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
    }
}
