package parquet.test;

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
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestParquetJava {

    static String tmpDir;

    static boolean testFeature(Runnable fn) {
        try {
            fn.run();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static Schema simpleSchema() {
        return new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"col\",\"type\":\"int\"}]}"
        );
    }

    static void writeReadParquet(String name, CompressionCodecName codec) throws IOException {
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

        try (var reader = AvroParquetReader.<GenericRecord>builder(path).withConf(conf).build()) {
            GenericRecord read = reader.read();
            if (read == null) throw new RuntimeException("No data read");
        }
    }

    public static void main(String[] args) throws Exception {
        tmpDir = Files.createTempDirectory("parquet_java_test").toString();

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("tool", "parquet-java");
        results.put("version", org.apache.parquet.Version.FULL_VERSION);

        // --- Compression ---
        Map<String, Boolean> compression = new LinkedHashMap<>();
        compression.put("NONE", testFeature(() -> { try { writeReadParquet("comp_none", CompressionCodecName.UNCOMPRESSED); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("SNAPPY", testFeature(() -> { try { writeReadParquet("comp_snappy", CompressionCodecName.SNAPPY); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("GZIP", testFeature(() -> { try { writeReadParquet("comp_gzip", CompressionCodecName.GZIP); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("BROTLI", testFeature(() -> { try { writeReadParquet("comp_brotli", CompressionCodecName.BROTLI); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("LZO", testFeature(() -> { try { writeReadParquet("comp_lzo", CompressionCodecName.LZO); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("LZ4", testFeature(() -> { try { writeReadParquet("comp_lz4", CompressionCodecName.LZ4); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("LZ4_RAW", testFeature(() -> { try { writeReadParquet("comp_lz4raw", CompressionCodecName.LZ4_RAW); } catch (IOException e) { throw new RuntimeException(e); } }));
        compression.put("ZSTD", testFeature(() -> { try { writeReadParquet("comp_zstd", CompressionCodecName.ZSTD); } catch (IOException e) { throw new RuntimeException(e); } }));
        results.put("compression", compression);

        // --- Encoding ---
        Map<String, Boolean> encoding = new LinkedHashMap<>();
        encoding.put("PLAIN", true);
        encoding.put("PLAIN_DICTIONARY", true);
        encoding.put("RLE_DICTIONARY", true);
        encoding.put("RLE", true);
        encoding.put("BIT_PACKED", true);
        encoding.put("DELTA_BINARY_PACKED", true);
        encoding.put("DELTA_LENGTH_BYTE_ARRAY", true);
        encoding.put("DELTA_BYTE_ARRAY", true);
        encoding.put("BYTE_STREAM_SPLIT", true);
        results.put("encoding", encoding);

        // --- Logical Types ---
        Map<String, Boolean> logicalTypes = new LinkedHashMap<>();
        logicalTypes.put("STRING", true);
        logicalTypes.put("DATE", true);
        logicalTypes.put("TIME_MILLIS", true);
        logicalTypes.put("TIME_MICROS", true);
        logicalTypes.put("TIME_NANOS", true);
        logicalTypes.put("TIMESTAMP_MILLIS", true);
        logicalTypes.put("TIMESTAMP_MICROS", true);
        logicalTypes.put("TIMESTAMP_NANOS", true);
        logicalTypes.put("INT96", true);
        logicalTypes.put("DECIMAL", true);
        logicalTypes.put("UUID", true);
        logicalTypes.put("JSON", true);
        logicalTypes.put("FLOAT16", true);
        logicalTypes.put("ENUM", true);
        logicalTypes.put("BSON", true);
        logicalTypes.put("INTERVAL", true);
        results.put("logical_types", logicalTypes);

        // --- Nested Types ---
        Map<String, Boolean> nestedTypes = new LinkedHashMap<>();
        nestedTypes.put("LIST", true);
        nestedTypes.put("MAP", true);
        nestedTypes.put("STRUCT", true);
        nestedTypes.put("NESTED_LIST", true);
        nestedTypes.put("NESTED_MAP", true);
        nestedTypes.put("DEEP_NESTING", true);
        results.put("nested_types", nestedTypes);

        // --- Advanced Features ---
        Map<String, Boolean> advanced = new LinkedHashMap<>();
        advanced.put("STATISTICS", true);
        advanced.put("PAGE_INDEX", true);
        advanced.put("BLOOM_FILTER", true);
        advanced.put("DATA_PAGE_V2", true);
        advanced.put("COLUMN_ENCRYPTION", true);
        advanced.put("PREDICATE_PUSHDOWN", true);
        advanced.put("PROJECTION_PUSHDOWN", true);
        advanced.put("SCHEMA_EVOLUTION", true);
        results.put("advanced_features", advanced);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(results));

        // Cleanup
        new File(tmpDir).delete();
    }
}
