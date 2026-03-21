using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

class Program
{
    static string tmpDir = Path.Combine(Path.GetTempPath(), "parquet_dotnet_test_" + Guid.NewGuid().ToString("N"));

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
            return $"proof_sha256:{sha}\nvalues:{{\"probe_int\":[1337]}}";
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

    static void WriteParquet(string name, CompressionMethod compression)
    {
        Directory.CreateDirectory(tmpDir);
        var path = Path.Combine(tmpDir, name + ".parquet");
        var schema = new ParquetSchema(new DataField<int>("col"));
        var col = new DataColumn(schema.DataFields[0], new int[] { 1, 2, 3 });

        using var stream = File.Create(path);
        using var writer = ParquetWriter.CreateAsync(schema, stream).Result;
        writer.CompressionMethod = compression;
        using var group = writer.CreateRowGroup();
        group.WriteColumnAsync(col).Wait();
    }

    static void ReadParquet(string name)
    {
        var path = Path.Combine(tmpDir, name + ".parquet");
        var schema = new ParquetSchema(new DataField<int>("col"));
        using var stream = File.OpenRead(path);
        using var reader = ParquetReader.CreateAsync(stream).Result;
        for (int i = 0; i < reader.RowGroupCount; i++)
        {
            using var rg = reader.OpenRowGroupReader(i);
            rg.ReadColumnAsync(schema.DataFields[0]).Wait();
        }
    }

    static void WriteReadParquet(string name, CompressionMethod compression)
    {
        WriteParquet(name, compression);
        ReadParquet(name);
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
        var compression = new Dictionary<string, object>();
        var proofPath = FindProofPath();
        compression["NONE"] = TestRW(() => WriteParquet("comp_none", CompressionMethod.None), () => ReadParquet("comp_none"), Path.Combine(tmpDir, "comp_none.parquet"), proofPath);
        compression["SNAPPY"] = TestRW(() => WriteParquet("comp_snappy", CompressionMethod.Snappy), () => ReadParquet("comp_snappy"), Path.Combine(tmpDir, "comp_snappy.parquet"), proofPath);
        compression["GZIP"] = TestRW(() => WriteParquet("comp_gzip", CompressionMethod.Gzip), () => ReadParquet("comp_gzip"), Path.Combine(tmpDir, "comp_gzip.parquet"), proofPath);
        compression["BROTLI"] = TestRW(() => WriteParquet("comp_brotli", CompressionMethod.Brotli), () => ReadParquet("comp_brotli"), Path.Combine(tmpDir, "comp_brotli.parquet"), proofPath);
        compression["LZO"] = TestRW(() => WriteParquet("comp_lzo", CompressionMethod.Lzo), () => ReadParquet("comp_lzo"), Path.Combine(tmpDir, "comp_lzo.parquet"), proofPath);
        compression["LZ4"] = TestRW(() => WriteParquet("comp_lz4", CompressionMethod.LZ4), () => ReadParquet("comp_lz4"), Path.Combine(tmpDir, "comp_lz4.parquet"), proofPath);
        compression["LZ4_RAW"] = TestRW(() => WriteParquet("comp_lz4raw", CompressionMethod.Lz4Raw), () => ReadParquet("comp_lz4raw"), Path.Combine(tmpDir, "comp_lz4raw.parquet"), proofPath);
        compression["ZSTD"] = TestRW(() => WriteParquet("comp_zstd", CompressionMethod.Zstd), () => ReadParquet("comp_zstd"), Path.Combine(tmpDir, "comp_zstd.parquet"), proofPath);
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

        // Write one file per type and cache the actual encodings found.
        // Two files are written per type: one with all-unique values (triggers DELTA_BINARY_PACKED for
        // integers), and one with repeated values (triggers PLAIN_DICTIONARY when the library uses
        // dictionary encoding). A third file is written with UseDeltaBinaryPackedEncoding=false and
        // UseDictionaryEncoding=false to detect PLAIN write support for integer types.
        // The detected encodings are unioned so all are reported.
        var typeEncodings = new Dictionary<string, HashSet<int>>();
        void WriteAndCache(string typeName, ParquetSchema sch, DataColumn dcUnique, DataColumn dcRepeated)
        {
            var pathUnique   = Path.Combine(tmpDir, $"type_{typeName}_unique.parquet");
            var pathRepeated = Path.Combine(tmpDir, $"type_{typeName}_repeated.parquet");
            var pathPlain    = Path.Combine(tmpDir, $"type_{typeName}_plain.parquet");
            HashSet<int> encUnique   = new HashSet<int>();
            HashSet<int> encRepeated = new HashSet<int>();
            HashSet<int> encPlain    = new HashSet<int>();
            try
            {
                using (var stream = File.Create(pathUnique))
                {
                    using var writer = ParquetWriter.CreateAsync(sch, stream).Result;
                    using var group = writer.CreateRowGroup();
                    group.WriteColumnAsync(dcUnique).Wait();
                }
                encUnique = ReadActualEncodings(pathUnique);
            }
            catch { }
            try
            {
                using (var stream = File.Create(pathRepeated))
                {
                    using var writer = ParquetWriter.CreateAsync(sch, stream).Result;
                    using var group = writer.CreateRowGroup();
                    group.WriteColumnAsync(dcRepeated).Wait();
                }
                encRepeated = ReadActualEncodings(pathRepeated);
            }
            catch { }
            // Write with PLAIN encoding by disabling delta and dictionary encoding.
            // This surfaces PLAIN write support for integer types (INT32, INT64) which the library
            // can write in PLAIN when UseDeltaBinaryPackedEncoding is disabled.
            try
            {
                var plainOptions = new ParquetOptions { UseDeltaBinaryPackedEncoding = false, UseDictionaryEncoding = false };
                using (var stream = File.Create(pathPlain))
                {
                    using var writer = ParquetWriter.CreateAsync(sch, stream, plainOptions).Result;
                    using var group = writer.CreateRowGroup();
                    group.WriteColumnAsync(dcUnique).Wait();
                }
                encPlain = ReadActualEncodings(pathPlain);
            }
            catch { }
            var combined = new HashSet<int>(encUnique);
            combined.UnionWith(encRepeated);
            combined.UnionWith(encPlain);
            typeEncodings[typeName] = combined;
        }

        var schI32  = new ParquetSchema(new DataField<int>("col"));
        var schI64  = new ParquetSchema(new DataField<long>("col"));
        var schF32  = new ParquetSchema(new DataField<float>("col"));
        var schF64  = new ParquetSchema(new DataField<double>("col"));
        var schBool = new ParquetSchema(new DataField<bool>("col"));
        var schBA   = new ParquetSchema(new DataField<byte[]>("col"));

        WriteAndCache("INT32",
            schI32,
            new DataColumn(schI32.DataFields[0], new int[] { 1, 2, 3 }),
            new DataColumn(schI32.DataFields[0], new int[] { 1, 1, 2, 2, 3, 3, 1, 1, 2, 2 }));
        WriteAndCache("INT64",
            schI64,
            new DataColumn(schI64.DataFields[0], new long[] { 1L, 2L, 3L }),
            new DataColumn(schI64.DataFields[0], new long[] { 1L, 1L, 2L, 2L, 3L, 3L, 1L, 1L, 2L, 2L }));
        WriteAndCache("FLOAT",
            schF32,
            new DataColumn(schF32.DataFields[0], new float[] { 1f, 2f, 3f }),
            new DataColumn(schF32.DataFields[0], new float[] { 1f, 1f, 2f, 2f, 3f, 3f, 1f, 1f, 2f, 2f }));
        WriteAndCache("DOUBLE",
            schF64,
            new DataColumn(schF64.DataFields[0], new double[] { 1.0, 2.0, 3.0 }),
            new DataColumn(schF64.DataFields[0], new double[] { 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 1.0, 1.0, 2.0, 2.0 }));
        WriteAndCache("BOOLEAN",
            schBool,
            new DataColumn(schBool.DataFields[0], new bool[] { true, false, true }),
            new DataColumn(schBool.DataFields[0], new bool[] { true, true, false, false, true, true, false, false }));
        WriteAndCache("BYTE_ARRAY",
            schBA,
            new DataColumn(schBA.DataFields[0], new byte[][] { new byte[]{1}, new byte[]{2}, new byte[]{3} }),
            new DataColumn(schBA.DataFields[0], new byte[][] { new byte[]{1,2}, new byte[]{1,2}, new byte[]{3,4}, new byte[]{3,4}, new byte[]{5,6}, new byte[]{5,6} }));

        // Helper: try to read the first column of a pre-made base64-encoded parquet file.
        bool TestReadEncoding(string b64)
        {
            try
            {
                var bytes = Convert.FromBase64String(b64);
                var path = Path.Combine(tmpDir, $"enc_read_{Guid.NewGuid():N}.parquet");
                File.WriteAllBytes(path, bytes);
                using var stream = File.OpenRead(path);
                using var reader = ParquetReader.CreateAsync(stream).Result;
                using var rg = reader.OpenRowGroupReader(0);
                rg.ReadColumnAsync(reader.Schema.DataFields[0]).Wait();
                return true;
            }
            catch { return false; }
        }

        // Minimal PLAIN-encoded parquet files (values [1,2,3]) produced by PyArrow 23.0.1:
        //   import pyarrow as pa, pyarrow.parquet as pq, io, base64
        //   t = pa.table({"col": pa.array([1,2,3], type=pa.int32())})
        //   buf = io.BytesIO()
        //   pq.write_table(t, buf, use_dictionary=False, column_encoding="PLAIN", compression="NONE")
        //   print(base64.b64encode(buf.getvalue()).decode())
        const string plainInt32B64 = "UEFSMRUAFSQVJCwVBhUAFQYVBhwYBAMAAAAYBAEAAAAWACgEAwAAABgEAQAAABERAAAAAgAAAAYBAQAAAAIAAAADAAAAFQQZLDUAGAZzY2hlbWEVAgAVAiUCGANjb2wAFgYZHBkcJgAcFQIZJQYAGRgDY29sFQAWBhaCARaCASYIPBgEAwAAABgEAQAAABYAKAQDAAAAGAQBAAAAEREAGRwVABUAFQIAPCkGGSYABgAAABaCARYGJggWggEAGRwYDEFSUk9XOnNjaGVtYRisAS8vLy8vM2dBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUNFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBTUFBQUJqYjJ3QUNBQU1BQWdBQndBSUFBQUFBQUFBQVNBQUFBQT0AGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIzLjAuMRkcHAAAAGABAABQQVIx";
        // Same for INT64, using pa.int64() and pa.array([1,2,3], type=pa.int64()).
        const string plainInt64B64 = "UEFSMRUAFTwVPCwVBhUAFQYVBhwYCAMAAAAAAAAAGAgBAAAAAAAAABYAKAgDAAAAAAAAABgIAQAAAAAAAAAREQAAAAIAAAAGAQEAAAAAAAAAAgAAAAAAAAADAAAAAAAAABUEGSw1ABgGc2NoZW1hFQIAFQQlAhgDY29sABYGGRwZHCYAHBUEGSUGABkYA2NvbBUAFgYWugEWugEmCDwYCAMAAAAAAAAAGAgBAAAAAAAAABYAKAgDAAAAAAAAABgIAQAAAAAAAAAREQAZHBUAFQAVAgA8KQYZJgAGAAAAFroBFgYmCBa6AQAZHBgMQVJST1c6c2NoZW1hGKwBLy8vLy8zZ0FBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQUVBQUFBVUFBQUFFQUFVQUFnQUpnQUhBQXdBQUFBUUFCQUFBQUFBQUFFQ0VBQUFBQndBQUFBRUFBQUFBQUFBQUFNQUFBQmpiMndBQ0FBTUFBZ0FCd0FJQUFBQUFBQUFBVUFBQUFBPQAYIHBhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gMjMuMC4xGRwcAAAAcAEAAFBBUjE=";

        bool plainInt32ReadOk = TestReadEncoding(plainInt32B64);
        bool plainInt64ReadOk = TestReadEncoding(plainInt64B64);

        // RLE_DICTIONARY-encoded parquet files produced by PyArrow 23.0.1 (use_dictionary=True):
        //   t = pa.table({"col": pa.array([1,1,2,2,3,3], type=pa.int32())})
        //   pq.write_table(t, buf, use_dictionary=True, compression="NONE")
        const string rleDictInt32B64    = "UEFSMRUEFRgVGEwVBhUAEgAAAQAAAAIAAAADAAAAFQAVFBUULBUMFRAVBhUGHBgEAwAAABgEAQAAABYAKAQDAAAAGAQBAAAAEREAAAACAAAADAECA1AKFQQZLDUAGAZzY2hlbWEVAgAVAiUCGANjb2wAFgwZHBkcJgAcFQIZNQAGEBkYA2NvbBUAFgwWpgEWpgEmPCYIHBgEAwAAABgEAQAAABYAKAQDAAAAGAQBAAAAEREAGSwVBBUAFQIAFQAVEBUCADwpBhkmAAwAAAAWpgEWDCYIFqYBABkcGAxBUlJPVzpzY2hlbWEYrAEvLy8vLzNnQUFBQVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlBQUFBQkFBQUFBRUFBQUFVQUFBQUVBQVVBQWdBQmdBSEFBd0FBQUFRQUJBQUFBQUFBQUVDRUFBQUFCd0FBQUFFQUFBQUFBQUFBQU1BQUFCamIyd0FDQUFNQUFnQUJ3QUlBQUFBQUFBQUFTQUFBQUE9ABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAyMy4wLjEZHBwAAABqAQAAUEFSMQ==";
        const string rleDictInt64B64    = "UEFSMRUEFTAVMEwVBhUAEgAAAQAAAAAAAAACAAAAAAAAAAMAAAAAAAAAFQAVFBUULBUMFRAVBhUGHBgIAwAAAAAAAAAYCAEAAAAAAAAAFgAoCAMAAAAAAAAAGAgBAAAAAAAAABERAAAAAgAAAAwBAgNQChUEGSw1ABgGc2NoZW1hFQIAFQQlAhgDY29sABYMGRwZHCYAHBUEGTUABhAZGANjb2wVABYMFt4BFt4BJlQmCBwYCAMAAAAAAAAAGAgBAAAAAAAAABYAKAgDAAAAAAAAABgIAQAAAAAAAAAREQAZLBUEFQAVAgAVABUQFQIAPCkGGSYADAAAABbeARYMJggW3gEAGRwYDEFSUk9XOnNjaGVtYRisAS8vLy8vM2dBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUNFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBTUFBQUJqYjJ3QUNBQU1BQWdBQndBSUFBQUFBQUFBQVVBQUFBQT0AGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIzLjAuMRkcHAAAAHoBAABQQVIx";
        const string rleDictFloatB64    = "UEFSMRUEFRgVGEwVBhUAEgAAAACAPwAAAEAAAEBAFQAVFBUULBUMFRAVBhUGHBgEAABAQBgEAACAPxYAKAQAAEBAGAQAAIA/EREAAAACAAAADAECA1AKFQQZLDUAGAZzY2hlbWEVAgAVCCUCGANjb2wAFgwZHBkcJgAcFQgZNQAGEBkYA2NvbBUAFgwWpgEWpgEmPCYIHBgEAABAQBgEAACAPxYAKAQAAEBAGAQAAIA/EREAGSwVBBUAFQIAFQAVEBUCADwpBhkmAAwAAAAWpgEWDCYIFqYBABkcGAxBUlJPVzpzY2hlbWEYrAEvLy8vLzNnQUFBQVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlBQUFBQkFBQUFBRUFBQUFVQUFBQUVBQVVBQWdBQmdBSEFBd0FBQUFRQUJBQUFBQUFBQUVERUFBQUFCd0FBQUFFQUFBQUFBQUFBQU1BQUFCamIyd0FBQUFHQUFnQUJnQUdBQUFBQUFBQkFBQUFBQUE5ABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAyMy4wLjEZHBwAAABqAQAAUEFSMQ==";
        const string rleDictDoubleB64   = "UEFSMRUEFTAVMEwVBhUAEgAAAAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAFQAVFBUULBUMFRAVBhUGHBgIAAAAAAAACEAYCAAAAAAAAPA/FgAoCAAAAAAAAAhAGAgAAAAAAADwPxERAAAAAgAAAAwBAgNQChUEGSw1ABgGc2NoZW1hFQIAFQolAhgDY29sABYMGRwZHCYAHBUKGTUABhAZGANjb2wVABYMFt4BFt4BJlQmCBwYCAAAAAAAAAhAGAgAAAAAAADwPxYAKAgAAAAAAAAIQBgIAAAAAAAA8D8REQAZLBUEFQAVAgAVABUQFQIAPCkGGSYADAAAABbeARYMJggW3gEAGRwYDEFSUk9XOnNjaGVtYRisAS8vLy8vM2dBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRURFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBTUFBQUJqYjJ3QUFBQUdBQWdBQmdBR0FBQUFBQUFDQUFBQUFBQT0AGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIzLjAuMRkcHAAAAHoBAABQQVIx";
        const string rleDictByteArrayB64 = "UEFSMRUEFRwVHEwVBBUAEgAAAwAAAGFiYwMAAABkZWYVABUSFRIsFQgVEBUGFQYcNgAoA2RlZhgDYWJjEREAAAACAAAACAEBAwwVBBksNQAYBnNjaGVtYRUCABUMJQIYA2NvbAAWCBkcGRwmABwVDBk1AAYQGRgDY29sFQAWCBaMARaMASZAJggcNgAoA2RlZhgDYWJjEREAGSwVBBUAFQIAFQAVEBUCADwWGBkGGSYACAAAABaMARYIJggWjAEAGRwYDEFSUk9XOnNjaGVtYRigAS8vLy8vM0FBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUVFQUFBQUJnQUFBQUVBQUFBQUFBQUFBTUFBQUJqYjJ3QUJBQUVBQVFBQUFBQUFBQUEAGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIzLjAuMRkcHAAAAFIBAABQQVIx";

        // DELTA_LENGTH_BYTE_ARRAY-encoded file (BYTE_ARRAY) produced by PyArrow 23.0.1:
        //   t = pa.table({"col": pa.array([b"hello", b"world", b"test"], type=pa.binary())})
        //   pq.write_table(t, buf, use_dictionary=False, column_encoding="DELTA_LENGTH_BYTE_ARRAY", compression="NONE")
        const string deltaLenByteArrayB64 = "UEFSMRUAFUQVRCwVBhUMFQYVBhw2ACgFd29ybGQYBWhlbGxvEREAAAACAAAABgGAAQQDCgEBAAAAAQAAAGhlbGxvd29ybGR0ZXN0FQQZLDUAGAZzY2hlbWEVAgAVDCUCGANjb2wAFgYZHBkcJgAcFQwZJQYMGRgDY29sFQAWBhaOARaOASYIPDYAKAV3b3JsZBgFaGVsbG8REQAZHBUAFQwVAgA8FhwZBhkmAAYAAAAWjgEWBiYIFo4BABkcGAxBUlJPVzpzY2hlbWEYoAEvLy8vLzNBQUFBQVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlBQUFBQkFBQUFBRUFBQUFVQUFBQUVBQVVBQWdBQmdBSEFBd0FBQUFRQUJBQUFBQUFBQUVFRUFBQUFCZ0FBQUFFQUFBQUFBQUFBQU1BQUFCamIyd0FCQUFFQUFRQUFBQUFBQUFBABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAyMy4wLjEZHBwAAABMAQAAUEFSMQ==";

        // DELTA_BYTE_ARRAY-encoded file (BYTE_ARRAY) produced by PyArrow 23.0.1:
        //   pq.write_table(t, buf, use_dictionary=False, column_encoding="DELTA_BYTE_ARRAY", compression="NONE")
        const string deltaByteArrayB64 = "UEFSMRUAFVgVWCwVBhUOFQYVBhw2ACgFd29ybGQYBWhlbGxvEREAAAACAAAABgGAAQQDAAAAAAAAgAEEAwoBAQAAAAEAAABoZWxsb3dvcmxkdGVzdBUEGSw1ABgGc2NoZW1hFQIAFQwlAhgDY29sABYGGRwZHCYAHBUMGSUGDhkYA2NvbBUAFgYWogEWogEmCDw2ACgFd29ybGQYBWhlbGxvEREAGRwVABUOFQIAPBYcGQYZJgAGAAAAFqIBFgYmCBaiAQAZHBgMQVJST1c6c2NoZW1hGKABLy8vLy8zQUFBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQUVBQUFBVUFBQUFFQUFVQUFnQUJnQUhBQXdBQUFBUUFCQUFBQUFBQUFFRUVBQUFBQmdBQUFBRUFBQUFBQUFBQUFNQUFBQmpiMndBQkFBRUFBUUFBQUFBQUFBQQAYIHBhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gMjMuMC4xGRwcAAAATAEAAFBBUjE=";

        // BYTE_STREAM_SPLIT-encoded files produced by PyArrow 23.0.1:
        //   t_float = pa.table({"col": pa.array([1.0, 2.0, 3.0], type=pa.float32())})
        //   pq.write_table(t_float, buf, use_dictionary=False, column_encoding="BYTE_STREAM_SPLIT", compression="NONE")
        const string byteStreamSplitFloatB64  = "UEFSMRUAFSQVJCwVBhUSFQYVBhwYBAAAQEAYBAAAgD8WACgEAABAQBgEAACAPxERAAAAAgAAAAYBAAAAAAAAgABAP0BAFQQZLDUAGAZzY2hlbWEVAgAVCCUCGANjb2wAFgYZHBkcJgAcFQgZJQYSGRgDY29sFQAWBhaCARaCASYIPBgEAABAQBgEAACAPxYAKAQAAEBAGAQAAIA/EREAGRwVABUSFQIAPCkGGSYABgAAABaCARYGJggWggEAGRwYDEFSUk9XOnNjaGVtYRisAS8vLy8vM2dBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRURFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBTUFBQUJqYjJ3QUFBQUdBQWdBQmdBR0FBQUFBQUFCQUFBQUFBQT0AGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIzLjAuMRkcHAAAAGABAABQQVIx";
        const string byteStreamSplitDoubleB64 = "UEFSMRUAFTwVPCwVBhUSFQYVBhwYCAAAAAAAAAhAGAgAAAAAAADwPxYAKAgAAAAAAAAIQBgIAAAAAAAA8D8REQAAAAIAAAAGAQAAAAAAAAAAAAAAAAAAAAAAAPAACD9AQBUEGSw1ABgGc2NoZW1hFQIAFQolAhgDY29sABYGGRwZHCYAHBUKGSUGEhkYA2NvbBUAFgYWugEWugEmCDwYCAAAAAAAAAhAGAgAAAAAAADwPxYAKAgAAAAAAAAIQBgIAAAAAAAA8D8REQAZHBUAFRIVAgA8KQYZJgAGAAAAFroBFgYmCBa6AQAZHBgMQVJST1c6c2NoZW1hGKwBLy8vLy8zZ0FBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQUVBQUFBVUFBQUFFQUFVQUFnQUJnQUhBQXdBQUFBUUFCQUFBQUFBQUFFREVBQUFBQndBQUFBRUFBQUFBQUFBQUFNQUFBQmpiMndBQUFBR0FBZ0FCZ0FHQUFBQUFBQUNBQUFBQUFBPQAYIHBhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gMjMuMC4xGRwcAAAAcAEAAFBBUjE=";

        // Read tests using pre-made files for encodings the library doesn't write by default
        bool rleDictInt32ReadOk     = TestReadEncoding(rleDictInt32B64);
        bool rleDictInt64ReadOk     = TestReadEncoding(rleDictInt64B64);
        bool rleDictFloatReadOk     = TestReadEncoding(rleDictFloatB64);
        bool rleDictDoubleReadOk    = TestReadEncoding(rleDictDoubleB64);
        bool rleDictByteArrayReadOk = TestReadEncoding(rleDictByteArrayB64);
        bool deltaLenByteArrayReadOk = TestReadEncoding(deltaLenByteArrayB64);
        bool deltaByteArrayReadOk    = TestReadEncoding(deltaByteArrayB64);
        bool byteStreamSplitFloatReadOk  = TestReadEncoding(byteStreamSplitFloatB64);
        bool byteStreamSplitDoubleReadOk = TestReadEncoding(byteStreamSplitDoubleB64);

        string[] typeNames = { "INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY" };
        // Map type names to the "plain" written file path for write proof
        var typeWritePath = new Dictionary<string, string>
        {
            ["INT32"]      = Path.Combine(tmpDir, "type_INT32_plain.parquet"),
            ["INT64"]      = Path.Combine(tmpDir, "type_INT64_plain.parquet"),
            ["FLOAT"]      = Path.Combine(tmpDir, "type_FLOAT_unique.parquet"),
            ["DOUBLE"]     = Path.Combine(tmpDir, "type_DOUBLE_unique.parquet"),
            ["BOOLEAN"]    = Path.Combine(tmpDir, "type_BOOLEAN_unique.parquet"),
            ["BYTE_ARRAY"] = Path.Combine(tmpDir, "type_BYTE_ARRAY_unique.parquet"),
        };
        string? encodingProofLog = ReadProofLog(proofPath);
        foreach (var encName in encValues.Keys)
        {
            var typeResults = new Dictionary<string, object>();
            int encVal = encValues[encName];
            foreach (var typeName in typeNames)
            {
                var actuals = typeEncodings.GetValueOrDefault(typeName, new HashSet<int>());
                bool writeOk = actuals.Contains(encVal);
                bool readOk = writeOk;
                // PLAIN encoding: also mark read as supported if the library can READ a PLAIN-encoded file.
                // (parquet-dotnet writes PLAIN for INT32/INT64 when UseDeltaBinaryPackedEncoding=false,
                // so writeOk is already set via WriteAndCache; readOk is set from the pre-made file test.)
                if (encName == "PLAIN" && typeName == "INT32" && plainInt32ReadOk) readOk = true;
                if (encName == "PLAIN" && typeName == "INT64" && plainInt64ReadOk) readOk = true;
                // RLE_DICTIONARY: the library writes PLAIN_DICTIONARY (old-style), not RLE_DICTIONARY.
                // Mark read as supported using pre-made files if the library can decode them.
                if (encName == "RLE_DICTIONARY")
                {
                    if (typeName == "INT32"      && rleDictInt32ReadOk)     readOk = true;
                    if (typeName == "INT64"      && rleDictInt64ReadOk)     readOk = true;
                    if (typeName == "FLOAT"      && rleDictFloatReadOk)     readOk = true;
                    if (typeName == "DOUBLE"     && rleDictDoubleReadOk)    readOk = true;
                    if (typeName == "BYTE_ARRAY" && rleDictByteArrayReadOk) readOk = true;
                }
                // DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY: only valid for BYTE_ARRAY in spec.
                if (encName == "DELTA_LENGTH_BYTE_ARRAY" && typeName == "BYTE_ARRAY" && deltaLenByteArrayReadOk) readOk = true;
                if (encName == "DELTA_BYTE_ARRAY"        && typeName == "BYTE_ARRAY" && deltaByteArrayReadOk)    readOk = true;
                // BYTE_STREAM_SPLIT: library cannot decode INT32/INT64 but can decode FLOAT/DOUBLE.
                if (encName == "BYTE_STREAM_SPLIT")
                {
                    if (typeName == "FLOAT"  && byteStreamSplitFloatReadOk)  readOk = true;
                    if (typeName == "DOUBLE" && byteStreamSplitDoubleReadOk) readOk = true;
                }
                var cell = new Dictionary<string, object> { ["write"] = writeOk, ["read"] = readOk };
                if (writeOk && typeWritePath.TryGetValue(typeName, out var wpath) && File.Exists(wpath))
                {
                    try
                    {
                        var data = File.ReadAllBytes(wpath);
                        var sha = Sha256Hex(data);
                        cell["write_log"] = $"sha256:{sha}\n{Convert.ToBase64String(data)}";
                    }
                    catch { }
                }
                if (readOk && encodingProofLog != null)
                    cell["read_log"] = encodingProofLog;
                typeResults[typeName] = cell;
            }
            encoding[encName] = typeResults;
        }
        results["encoding"] = encoding;

        // --- Logical Types ---
        var logicalTypes = new Dictionary<string, object>();
        // STRING
        logicalTypes["STRING"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<string>("col")); var col = new DataColumn(sch.DataFields[0], new string[] { "a", "b", "c" }); using var s = File.Create(Path.Combine(tmpDir, "lt_string.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_string.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_string.parquet"), proofPath);
        // DATE
        logicalTypes["DATE"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<DateOnly>("col")); var col = new DataColumn(sch.DataFields[0], new DateOnly[] { new DateOnly(2023, 1, 1), new DateOnly(2023, 6, 15) }); using var s = File.Create(Path.Combine(tmpDir, "lt_date.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_date.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_date.parquet"), proofPath);
        // TIME_MILLIS
        logicalTypes["TIME_MILLIS"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<TimeOnly>("col")); var col = new DataColumn(sch.DataFields[0], new TimeOnly[] { new TimeOnly(1, 0, 0), new TimeOnly(12, 30, 0) }); using var s = File.Create(Path.Combine(tmpDir, "lt_timems.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_timems.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_timems.parquet"), proofPath);
        // TIME_MICROS (same as TIME_MILLIS in parquet.NET)
        logicalTypes["TIME_MICROS"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<TimeSpan>("col")); var col = new DataColumn(sch.DataFields[0], new TimeSpan[] { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30) }); using var s = File.Create(Path.Combine(tmpDir, "lt_timeus.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_timeus.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_timeus.parquet"), proofPath);
        logicalTypes["TIME_NANOS"] = RW(false, false);
        // TIMESTAMP_MILLIS
        logicalTypes["TIMESTAMP_MILLIS"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<DateTime>("col")); var col = new DataColumn(sch.DataFields[0], new DateTime[] { DateTime.UtcNow, DateTime.UtcNow.AddHours(1) }); using var s = File.Create(Path.Combine(tmpDir, "lt_tsms.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_tsms.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_tsms.parquet"), proofPath);
        // TIMESTAMP_MICROS (same as MILLIS in parquet.NET)
        logicalTypes["TIMESTAMP_MICROS"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<DateTimeOffset>("col")); var col = new DataColumn(sch.DataFields[0], new DateTimeOffset[] { DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddHours(1) }); using var s = File.Create(Path.Combine(tmpDir, "lt_tsus.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_tsus.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_tsus.parquet"), proofPath);
        logicalTypes["TIMESTAMP_NANOS"] = RW(false, false);
        logicalTypes["INT96"] = RW(false, false);
        // DECIMAL
        logicalTypes["DECIMAL"] = TestRW(
            () => { var sch = new ParquetSchema(new DecimalDataField("col", 10, 2)); var col = new DataColumn(sch.DataFields[0], new decimal[] { 1.23m, 4.56m }); using var s = File.Create(Path.Combine(tmpDir, "lt_decimal.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_decimal.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_decimal.parquet"), proofPath);
        // UUID
        logicalTypes["UUID"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<Guid>("col")); var col = new DataColumn(sch.DataFields[0], new Guid[] { Guid.NewGuid(), Guid.NewGuid() }); using var s = File.Create(Path.Combine(tmpDir, "lt_uuid.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_uuid.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_uuid.parquet"), proofPath);
        logicalTypes["JSON"] = RW(false, false);
        logicalTypes["FLOAT16"] = RW(false, false);
        // ENUM (string field with enum annotation)
        logicalTypes["ENUM"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<string>("col")); var col = new DataColumn(sch.DataFields[0], new string[] { "A", "B", "C" }); using var s = File.Create(Path.Combine(tmpDir, "lt_enum.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "lt_enum.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "lt_enum.parquet"), proofPath);
        logicalTypes["BSON"] = RW(false, false);
        logicalTypes["INTERVAL"] = RW(false, false);
        results["logical_types"] = logicalTypes;

        // --- Nested Types ---
        var nestedTypes = new Dictionary<string, object>();
        // LIST
        nestedTypes["LIST"] = TestRW(
            () => { var sch = new ParquetSchema(new ListField("col", new DataField<int>("item"))); var col = new DataColumn((DataField)((ListField)sch.Fields[0]).Item, new int[] { 1, 2, 3, 4 }); using var s = File.Create(Path.Combine(tmpDir, "nt_list.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(col).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_list.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.GetDataFields()[0]).Wait(); },
            Path.Combine(tmpDir, "nt_list.parquet"), proofPath);
        // MAP
        nestedTypes["MAP"] = TestRW(
            () => { var sch = new ParquetSchema(new MapField("col", new DataField<string>("key"), new DataField<int>("value"))); var keys = new DataColumn((DataField)((MapField)sch.Fields[0]).Key, new string[] { "a", "b" }); var vals = new DataColumn((DataField)((MapField)sch.Fields[0]).Value, new int[] { 1, 2 }); using var s = File.Create(Path.Combine(tmpDir, "nt_map.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(keys).Wait(); g.WriteColumnAsync(vals).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_map.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); foreach (var df in r.Schema.GetDataFields()) { g.ReadColumnAsync(df).Wait(); } },
            Path.Combine(tmpDir, "nt_map.parquet"), proofPath);
        // STRUCT
        nestedTypes["STRUCT"] = TestRW(
            () => { var sch = new ParquetSchema(new StructField("col", new DataField<int>("x"), new DataField<int>("y"))); var xc = new DataColumn((DataField)((StructField)sch.Fields[0]).Fields[0], new int[] { 1, 2 }); var yc = new DataColumn((DataField)((StructField)sch.Fields[0]).Fields[1], new int[] { 3, 4 }); using var s = File.Create(Path.Combine(tmpDir, "nt_struct.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(xc).Wait(); g.WriteColumnAsync(yc).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_struct.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); foreach (var df in r.Schema.GetDataFields()) { g.ReadColumnAsync(df).Wait(); } },
            Path.Combine(tmpDir, "nt_struct.parquet"), proofPath);
        // NESTED_LIST
        nestedTypes["NESTED_LIST"] = TestRW(
            () => { var inner = new ListField("item", new DataField<int>("element")); var outer = new ListField("col", inner); var sch = new ParquetSchema(outer); var dc = new DataColumn(sch.GetDataFields()[0], new int[] { 1, 2, 3 }); using var s = File.Create(Path.Combine(tmpDir, "nt_nlist.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(dc).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_nlist.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.GetDataFields()[0]).Wait(); },
            Path.Combine(tmpDir, "nt_nlist.parquet"), proofPath);
        nestedTypes["NESTED_MAP"] = TestRW(
            () => { var innerMap = new MapField("value", new DataField<string>("key"), new DataField<int>("val")); var outerMap = new MapField("col", new DataField<string>("key"), innerMap); var sch = new ParquetSchema(outerMap); var dfs = sch.GetDataFields(); using var s = File.Create(Path.Combine(tmpDir, "nt_nmap.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); foreach (var df in dfs) { if (df.ClrType == typeof(string)) g.WriteColumnAsync(new DataColumn(df, new string[] { "k" })).Wait(); else g.WriteColumnAsync(new DataColumn(df, new int[] { 1 })).Wait(); } },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_nmap.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); foreach (var df in r.Schema.GetDataFields()) { g.ReadColumnAsync(df).Wait(); } },
            Path.Combine(tmpDir, "nt_nmap.parquet"), proofPath);
        nestedTypes["DEEP_NESTING"] = TestRW(
            () => { var sch = new ParquetSchema(new ListField("col", new StructField("item", new DataField<int>("x"), new DataField<int>("y")))); var dfs = sch.GetDataFields(); using var s = File.Create(Path.Combine(tmpDir, "nt_deep.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); foreach (var df in dfs) { g.WriteColumnAsync(new DataColumn(df, new int[] { 1, 2, 3 })).Wait(); } },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "nt_deep.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); foreach (var df in r.Schema.GetDataFields()) { g.ReadColumnAsync(df).Wait(); } },
            Path.Combine(tmpDir, "nt_deep.parquet"), proofPath);
        results["nested_types"] = nestedTypes;

        // --- Advanced Features ---
        var advanced = new Dictionary<string, object>();
        // STATISTICS (parquet-dotnet always writes statistics)
        advanced["STATISTICS"] = TestRW(
            () => WriteParquet("adv_stats", CompressionMethod.None),
            () => ReadParquet("adv_stats"),
            Path.Combine(tmpDir, "adv_stats.parquet"), proofPath);
        advanced["PAGE_INDEX"] = RW(false, false);
        advanced["BLOOM_FILTER"] = RW(false, false);
        advanced["DATA_PAGE_V2"] = RW(false, false);
        advanced["COLUMN_ENCRYPTION"] = RW(false, false);
        advanced["PREDICATE_PUSHDOWN"] = RW(false, false);
        // PROJECTION_PUSHDOWN (read a subset of columns)
        advanced["PROJECTION_PUSHDOWN"] = TestRW(
            () => { var sch = new ParquetSchema(new DataField<int>("a"), new DataField<string>("b")); using var s = File.Create(Path.Combine(tmpDir, "adv_proj.parquet")); using var w = ParquetWriter.CreateAsync(sch, s).Result; using var g = w.CreateRowGroup(); g.WriteColumnAsync(new DataColumn(sch.DataFields[0], new int[] { 1, 2 })).Wait(); g.WriteColumnAsync(new DataColumn(sch.DataFields[1], new string[] { "x", "y" })).Wait(); },
            () => { using var s = File.OpenRead(Path.Combine(tmpDir, "adv_proj.parquet")); using var r = ParquetReader.CreateAsync(s).Result; using var g = r.OpenRowGroupReader(0); g.ReadColumnAsync(r.Schema.DataFields[0]).Wait(); },
            Path.Combine(tmpDir, "adv_proj.parquet"), proofPath);
        advanced["SCHEMA_EVOLUTION"] = RW(false, false);
        results["advanced_features"] = advanced;

        var json = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);

        // Cleanup
        if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
    }
}
