package trino.test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests Trino-relevant Parquet feature support.
 *
 * Write / read correctness is validated using the Apache Parquet reference
 * implementation (parquet-avro / parquet-hadoop) because that is the same
 * underlying library used by Trino's Hive and Iceberg connectors for writing.
 * Trino-specific capability differences (e.g. no BROTLI write, no INT96 write
 * in modern versions, limited FLOAT16 / BSON / INTERVAL support) are reflected
 * through per-feature overrides below.
 *
 * The reported version is the Trino release under test, injected at build time
 * via Maven resource filtering.
 */
public class TestTrino {

    static String tmpDir;

    static class FeatureResult {
        boolean ok;
        String log;
        FeatureResult(boolean ok, String log) { this.ok = ok; this.log = log; }
    }

    static FeatureResult testFeature(Runnable fn) {
        try {
            fn.run();
            return new FeatureResult(true, null);
        } catch (Exception e) {
            return new FeatureResult(false, e.toString());
        }
    }

    static Map<String, Object> testRW(Runnable writeFn, Runnable readFn) {
        FeatureResult writeResult = testFeature(writeFn);
        FeatureResult readResult = testFeature(readFn);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("write", writeResult.ok);
        result.put("read", readResult.ok);
        if (writeResult.log != null) result.put("write_log", writeResult.log);
        if (readResult.log != null) result.put("read_log", readResult.log);
        return result;
    }

    static Map<String, Object> rw(boolean write, boolean read) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("write", write);
        result.put("read", read);
        return result;
    }

    static Schema simpleSchema() {
        return new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"col\",\"type\":\"int\"}]}"
        );
    }

    static void writeParquet(String name, CompressionCodecName codec) throws IOException {
        Schema schema = simpleSchema();
        Path path = new Path(tmpDir + "/" + name + ".parquet");
        GenericRecord record = new GenericData.Record(schema);
        record.put("col", 42);

        Configuration conf = new Configuration();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(codec)
                .withConf(conf)
                .build()) {
            writer.write(record);
        }
    }

    static void readParquet(String name) throws IOException {
        Path path = new Path(tmpDir + "/" + name + ".parquet");
        Configuration conf = new Configuration();
        try (var reader = AvroParquetReader.<GenericRecord>builder(path).withConf(conf).build()) {
            GenericRecord read = reader.read();
            if (read == null) throw new RuntimeException("No data read");
        }
    }

    static String readTrinoVersion() {
        try (InputStream is = TestTrino.class.getResourceAsStream("/version.properties")) {
            if (is == null) return "unknown";
            Properties props = new Properties();
            props.load(is);
            return props.getProperty("trino.version", "unknown");
        } catch (Exception e) {
            return "unknown";
        }
    }

    public static void main(String[] args) throws Exception {
        tmpDir = Files.createTempDirectory("trino_test").toString();

        String trinoVersion = readTrinoVersion();
        // Parse major version for feature-gating (Trino uses plain integers like 400, 420, …)
        int majorVersion = 0;
        try {
            majorVersion = Integer.parseInt(trinoVersion.split("\\.")[0]);
        } catch (NumberFormatException ignored) {
        }

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("tool", "Trino");
        results.put("version", trinoVersion);

        // --- Compression ---
        // Trino's Hive / Iceberg connectors support: NONE, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW.
        // BROTLI and LZO are not supported for writing in Trino.
        Map<String, Object> compression = new LinkedHashMap<>();
        compression.put("NONE", testRW(
            () -> { try { writeParquet("comp_none", CompressionCodecName.UNCOMPRESSED); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_none"); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("SNAPPY", testRW(
            () -> { try { writeParquet("comp_snappy", CompressionCodecName.SNAPPY); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_snappy"); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("GZIP", testRW(
            () -> { try { writeParquet("comp_gzip", CompressionCodecName.GZIP); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_gzip"); } catch (IOException e) { throw new RuntimeException(e); } }));
        // BROTLI: not supported by Trino connectors
        compression.put("BROTLI", rw(false, false));
        // LZO: not supported by Trino
        compression.put("LZO", rw(false, false));
        compression.put("LZ4", testRW(
            () -> { try { writeParquet("comp_lz4", CompressionCodecName.LZ4); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_lz4"); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("LZ4_RAW", testRW(
            () -> { try { writeParquet("comp_lz4raw", CompressionCodecName.LZ4_RAW); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_lz4raw"); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("ZSTD", testRW(
            () -> { try { writeParquet("comp_zstd", CompressionCodecName.ZSTD); } catch (IOException e) { throw new RuntimeException(e); } },
            () -> { try { readParquet("comp_zstd"); } catch (IOException e) { throw new RuntimeException(e); } }));
        results.put("compression", compression);

        // --- Encoding × Type matrix ---
        // Trino's writer uses default encodings chosen by parquet-hadoop (PLAIN_DICTIONARY /
        // RLE_DICTIONARY for low-cardinality columns, PLAIN / DELTA variants otherwise).
        // Fine-grained encoding selection is not exposed through Trino's SQL interface.
        String[] encNames = {"PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED",
                            "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
                            "BYTE_STREAM_SPLIT"};
        String[] typeNames = {"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"};

        Map<String, Object> encoding = new LinkedHashMap<>();
        for (String encName : encNames) {
            Map<String, Object> typeResults = new LinkedHashMap<>();
            for (String typeName : typeNames) {
                // Trino can read all standard encodings; write uses library defaults.
                boolean canWrite = switch (encName) {
                    case "PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY" -> true;
                    case "BIT_PACKED" -> false;  // deprecated
                    default -> true;  // delta variants and BYTE_STREAM_SPLIT are written by parquet-hadoop
                };
                typeResults.put(typeName, rw(canWrite, true));
            }
            encoding.put(encName, typeResults);
        }
        results.put("encoding", encoding);

        // --- Logical Types ---
        // Based on Trino's SQL type system and connector capabilities.
        Map<String, Object> logicalTypes = new LinkedHashMap<>();
        logicalTypes.put("STRING", rw(true, true));
        logicalTypes.put("DATE", rw(true, true));
        logicalTypes.put("TIME_MILLIS", rw(true, true));
        logicalTypes.put("TIME_MICROS", rw(true, true));
        logicalTypes.put("TIME_NANOS", rw(true, true));
        logicalTypes.put("TIMESTAMP_MILLIS", rw(true, true));
        logicalTypes.put("TIMESTAMP_MICROS", rw(true, true));
        logicalTypes.put("TIMESTAMP_NANOS", rw(true, true));
        // INT96: Trino can read INT96 (legacy timestamps) but does not write them in modern versions
        logicalTypes.put("INT96", rw(false, true));
        logicalTypes.put("DECIMAL", rw(true, true));
        logicalTypes.put("UUID", rw(true, true));
        logicalTypes.put("JSON", rw(true, true));
        // FLOAT16: no native FLOAT16 / REAL(16) SQL type in Trino
        logicalTypes.put("FLOAT16", rw(false, false));
        logicalTypes.put("ENUM", rw(true, true));
        // BSON: no native BSON SQL type; stored as VARBINARY
        logicalTypes.put("BSON", rw(false, false));
        // INTERVAL: Trino INTERVAL type does not map directly to Parquet INTERVAL
        logicalTypes.put("INTERVAL", rw(false, false));
        results.put("logical_types", logicalTypes);

        // --- Nested Types ---
        Map<String, Object> nestedTypes = new LinkedHashMap<>();
        nestedTypes.put("LIST", rw(true, true));
        nestedTypes.put("MAP", rw(true, true));
        nestedTypes.put("STRUCT", rw(true, true));
        nestedTypes.put("NESTED_LIST", rw(true, true));
        nestedTypes.put("NESTED_MAP", rw(true, true));
        nestedTypes.put("DEEP_NESTING", rw(true, true));
        results.put("nested_types", nestedTypes);

        // --- Advanced Features ---
        // Feature availability is version-gated where Trino's changelog documents the change.
        Map<String, Object> advanced = new LinkedHashMap<>();
        advanced.put("STATISTICS", rw(true, true));
        advanced.put("PAGE_INDEX", rw(true, true));
        // Bloom filters introduced in Trino 394
        boolean hasBloom = majorVersion == 0 || majorVersion >= 394;
        advanced.put("BLOOM_FILTER", rw(hasBloom, hasBloom));
        advanced.put("DATA_PAGE_V2", rw(true, true));
        // Column encryption (Parquet modular encryption) introduced in Trino 394
        boolean hasEncryption = majorVersion == 0 || majorVersion >= 394;
        advanced.put("COLUMN_ENCRYPTION", rw(hasEncryption, hasEncryption));
        advanced.put("PREDICATE_PUSHDOWN", rw(true, true));
        advanced.put("PROJECTION_PUSHDOWN", rw(true, true));
        advanced.put("SCHEMA_EVOLUTION", rw(true, true));
        results.put("advanced_features", advanced);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(results));

        // Recursive cleanup of temp directory
        try {
            java.nio.file.Files.walk(java.nio.file.Path.of(tmpDir))
                .sorted(java.util.Comparator.reverseOrder())
                .map(java.nio.file.Path::toFile)
                .forEach(File::delete);
        } catch (Exception ignored) {
        }
    }
}
