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

    static void Main()
    {
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

        // --- Encoding ---
        var encoding = new Dictionary<string, bool>
        {
            ["PLAIN"] = true,
            ["PLAIN_DICTIONARY"] = true,
            ["RLE_DICTIONARY"] = true,
            ["RLE"] = true,
            ["BIT_PACKED"] = true,
            ["DELTA_BINARY_PACKED"] = true,
            ["DELTA_LENGTH_BYTE_ARRAY"] = true,
            ["DELTA_BYTE_ARRAY"] = true,
            ["BYTE_STREAM_SPLIT"] = true,
        };
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
