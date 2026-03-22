/**
 * Test hyparquet's Parquet read support, using @dsnp/parquetjs to write test files
 * and pyarrow-generated fixture files for feature coverage.
 * hyparquet is a pure-JS reader; write results are always false.
 */
import { parquetRead, parquetMetadata } from 'hyparquet';
import { ParquetWriter, ParquetSchema } from '@dsnp/parquetjs';
import { readFileSync, mkdtempSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createHash } from 'crypto';

const TOOL = 'hyparquet';
let VERSION = '1.0.0';
try {
  const pkg = JSON.parse(readFileSync(new URL('./node_modules/hyparquet/package.json', import.meta.url)));
  VERSION = pkg.version;
} catch {}

const tmp = mkdtempSync(join(tmpdir(), 'hyparquet_test_'));

function sha256Hex(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

// Locate the fixtures directory.  The PARQUET_FIXTURES_DIR env var is set by the
// run_multiversion.py harness; fall back to relative paths for local development.
function findFixturesDir() {
  const envDir = process.env.PARQUET_FIXTURES_DIR;
  if (envDir && existsSync(envDir)) return envDir;
  const candidates = [
    'fixtures',
    '../../../fixtures',
    'parquet_can_i_use/fixtures',
  ];
  for (const c of candidates) {
    try { if (existsSync(c)) return c; } catch {}
  }
  return null;
}

const FIXTURES_DIR = findFixturesDir();

function findProofPath() {
  if (!FIXTURES_DIR) return null;
  const p = join(FIXTURES_DIR, 'proof', 'proof.parquet');
  return existsSync(p) ? p : null;
}

async function readFileProofLog(filePath) {
  if (!filePath || !existsSync(filePath)) return null;
  try {
    const data = readFileSync(filePath);
    const sha = sha256Hex(data);
    const buffer = toArrayBuffer(data);
    const metadata = parquetMetadata(buffer);
    const colNames = (metadata.row_groups?.[0]?.columns ?? [])
      .map(c => c.meta_data?.path_in_schema?.join('.') ?? '');
    const rows = [];
    await parquetRead({ file: buffer, onComplete: d => rows.push(...d) });
    const values = {};
    for (const name of colNames) values[name] = [];
    for (const row of rows) {
      const arr = Array.isArray(row) ? row : [row];
      for (let i = 0; i < colNames.length; i++) {
        values[colNames[i]].push(arr[i]);
      }
    }
    return `proof_sha256:${sha}\nvalues:${JSON.stringify(values)}`;
  } catch (e) {
    return `proof_read_error:${e.message}`;
  }
}

function writeProofLog(filePath) {
  try {
    const data = readFileSync(filePath);
    const sha = sha256Hex(data);
    const b64 = data.toString('base64');
    return `sha256:${sha}\n${b64}`;
  } catch (e) {
    return null;
  }
}

function toArrayBuffer(nodeBuffer) {
  return nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength);
}

async function testRead(filePath) {
  try {
    const buffer = toArrayBuffer(readFileSync(filePath));
    const metadata = parquetMetadata(buffer);
    const rows = [];
    await parquetRead({ file: buffer, onComplete: data => rows.push(...data) });
    return { ok: true, log: null };
  } catch (e) {
    return { ok: false, log: e?.stack || String(e) };
  }
}

/**
 * Try to read a fixture file and return a {write, read, ...} result.
 * Since hyparquet is read-only, write is always false.
 * If no fixture is available, the feature is marked as not tested (write:false, read:false).
 */
async function testReadFixture(fixturePath) {
  if (!fixturePath || !existsSync(fixturePath)) {
    const reason = fixturePath
      ? `Fixture file not found: ${fixturePath}`
      : 'No fixture path provided for this feature';
    return { write: false, read: false, write_log: reason, read_log: reason };
  }
  const { ok: readOk, log: readLog } = await testRead(fixturePath);
  const entry = { write: false, read: readOk };
  if (readOk) {
    const pl = await readFileProofLog(fixturePath);
    if (pl) entry.read_log = pl;
  } else if (!readOk && readLog) {
    entry.read_log = readLog;
  }
  return entry;
}

/**
 * Return a result object for a feature that is explicitly not supported.
 * Includes a reason as both write_log and read_log.
 */
function notSupported(reason) {
  return { write: false, read: false, write_log: reason, read_log: reason };
}

async function writeAndRead(schema, rows) {
  const path = join(tmp, `test_${Date.now()}_${Math.random().toString(36).slice(2)}.parquet`);
  let writeLog = null;
  try {
    const writer = await ParquetWriter.openFile(schema, path);
    for (const row of rows) {
      await writer.appendRow(row);
    }
    await writer.close();
  } catch (e) {
    writeLog = e?.stack || String(e);
    const result = { write: false, read: false };
    if (writeLog) result.write_log = writeLog;
    return result;
  }
  const wpl = writeProofLog(path);
  const { ok: readOk, log: readLog } = await testRead(path);
  const result = { write: true, read: readOk };
  if (wpl) result.write_log = wpl;
  if (readOk) {
    const pl = await readFileProofLog(path);
    if (pl) result.read_log = pl;
  } else if (readLog) {
    result.read_log = readLog;
  }
  return result;
}

async function main() {
  // Self-check: write a minimal UNCOMPRESSED file with the helper library and
  // immediately read it back with hyparquet.  If this round-trip fails the test
  // harness infrastructure is broken (e.g. an incompatible helper version), so
  // we report a CLI error rather than letting every feature appear as
  // "unsupported".
  const selfCheckPath = join(tmp, 'self_check.parquet');
  let selfCheckPassed = false;
  try {
    const selfSchema = new ParquetSchema({ col: { type: 'INT32' } });
    const selfWriter = await ParquetWriter.openFile(selfSchema, selfCheckPath);
    await selfWriter.appendRow({ col: 42 });
    await selfWriter.close();
    const selfBuffer = toArrayBuffer(readFileSync(selfCheckPath));
    await parquetRead({ file: selfBuffer, onComplete: () => {} });
    selfCheckPassed = true;
  } catch (err) {
    // Any error (write failure, read failure, API incompatibility) means the
    // harness cannot exercise hyparquet — treat as infrastructure broken.
    process.stderr.write(`Self-check failed: ${err}\n`);
    selfCheckPassed = false;
  }
  if (!selfCheckPassed) {
    process.stdout.write(JSON.stringify({
      cli_error: true,
      cli_error_type: 'self_check_failed',
      tool: TOOL,
      version: VERSION,
    }) + '\n');
    process.exit(1);
  }

  const results = {
    tool: TOOL,
    version: VERSION,
    compression: {},
    encoding: {},
    logical_types: {},
    nested_types: {},
    advanced_features: {},
  };

  // --- Compression ---
  // Try to write with parquetjs; if the codec is not supported by parquetjs, fall back to a
  // fixture file written by pyarrow to test hyparquet's read capability independently.
  const compressionCodecs = {
    NONE: 'UNCOMPRESSED',
    SNAPPY: 'SNAPPY',
    GZIP: 'GZIP',
    BROTLI: 'BROTLI',
    LZO: 'LZO',
    LZ4: 'LZ4',
    LZ4_RAW: 'LZ4_RAW',
    ZSTD: 'ZSTD',
  };
  const schema = new ParquetSchema({ col: { type: 'INT32' } });
  for (const [name, codec] of Object.entries(compressionCodecs)) {
    const fixturePath = FIXTURES_DIR ? join(FIXTURES_DIR, 'compression', `comp_${name}.parquet`) : null;
    try {
      const path = join(tmp, `comp_${name}.parquet`);
      const writer = await ParquetWriter.openFile(schema, path, { compression: codec });
      await writer.appendRow({ col: 1 });
      await writer.close();
      const { ok: readOk, log: readLog } = await testRead(path);
      const entry = { write: false, read: readOk };
      if (readOk) {
        const pl = await readFileProofLog(path);
        if (pl) entry.read_log = pl;
      } else if (readLog) {
        entry.read_log = readLog;
      }
      results.compression[name] = entry;
    } catch (e) {
      // parquetjs does not support this codec; try fixture file instead
      results.compression[name] = await testReadFixture(fixturePath);
      if (!results.compression[name].read && !results.compression[name].read_log) {
        results.compression[name].write_log = e?.stack || String(e);
      }
    }
  }

  // --- Encodings ---
  // Test each encoding using a fixture file written by pyarrow.
  // hyparquet is a reader; write is always false.
  const encodingTypes = ['INT32', 'INT64', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'BYTE_ARRAY'];
  const encodings = [
    'PLAIN', 'PLAIN_DICTIONARY', 'RLE_DICTIONARY', 'RLE', 'BIT_PACKED',
    'DELTA_BINARY_PACKED', 'DELTA_LENGTH_BYTE_ARRAY', 'DELTA_BYTE_ARRAY',
    'BYTE_STREAM_SPLIT', 'BYTE_STREAM_SPLIT_EXTENDED',
  ];

  for (const enc of encodings) {
    results.encoding[enc] = {};
    // Test against the fixture file for this encoding (one file covers all types for read).
    const fixturePath = FIXTURES_DIR ? join(FIXTURES_DIR, 'encodings', `enc_${enc}.parquet`) : null;
    const encResult = await testReadFixture(fixturePath);
    for (const ptype of encodingTypes) {
      results.encoding[enc][ptype] = { ...encResult };
    }
  }

  // --- Logical Types ---
  // For types supported by parquetjs, write with parquetjs and read with hyparquet.
  // For other types, use fixture files written by pyarrow.
  const logicalTypeTests = {
    STRING:           async () => writeAndRead(new ParquetSchema({ c: { type: 'UTF8' } }), [{ c: 'hello' }]),
    DATE:             async () => writeAndRead(new ParquetSchema({ c: { type: 'DATE' } }), [{ c: new Date('2024-01-01') }]),
    TIME_MILLIS:      async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_TIME_MILLIS.parquet') : null),
    TIME_MICROS:      async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_TIME_MICROS.parquet') : null),
    TIME_NANOS:       async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_TIME_NANOS.parquet') : null),
    TIMESTAMP_MILLIS: async () => writeAndRead(new ParquetSchema({ c: { type: 'TIMESTAMP_MILLIS' } }), [{ c: new Date('2024-01-01') }]),
    TIMESTAMP_MICROS: async () => writeAndRead(new ParquetSchema({ c: { type: 'TIMESTAMP_MICROS' } }), [{ c: new Date('2024-01-01') }]),
    TIMESTAMP_NANOS:  async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_TIMESTAMP_NANOS.parquet') : null),
    INT96:            async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_INT96.parquet') : null),
    DECIMAL:          async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_DECIMAL.parquet') : null),
    UUID:             async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_UUID.parquet') : null),
    JSON:             async () => writeAndRead(new ParquetSchema({ c: { type: 'UTF8' } }), [{ c: '{"key":"val"}' }]),
    FLOAT16:          async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_FLOAT16.parquet') : null),
    ENUM:             async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_ENUM.parquet') : null),
    BSON:             async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_BSON.parquet') : null),
    INTERVAL:         async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_INTERVAL.parquet') : null),
    UNKNOWN:          async () => testReadFixture(FIXTURES_DIR ? join(FIXTURES_DIR, 'logical_types', 'lt_UNKNOWN.parquet') : null),
    VARIANT:          async () => notSupported('VARIANT logical type is not supported by hyparquet'),
    GEOMETRY:         async () => notSupported('GEOMETRY logical type is not supported by hyparquet'),
    GEOGRAPHY:        async () => notSupported('GEOGRAPHY logical type is not supported by hyparquet'),
  };

  for (const [name, testFn] of Object.entries(logicalTypeTests)) {
    try {
      results.logical_types[name] = await testFn();
    } catch (e) {
      results.logical_types[name] = { write: false, read: false, write_log: e?.stack || String(e) };
    }
  }

  // --- Nested Types ---
  // Use fixture files written by pyarrow to test read capability for each nested type.
  const nestedTypeNames = ['LIST', 'MAP', 'STRUCT', 'NESTED_LIST', 'NESTED_MAP', 'DEEP_NESTING'];
  for (const name of nestedTypeNames) {
    const fixturePath = FIXTURES_DIR ? join(FIXTURES_DIR, 'nested_types', `nt_${name}.parquet`) : null;
    results.nested_types[name] = await testReadFixture(fixturePath);
  }

  // --- Advanced Features ---
  // Use fixture files written by pyarrow to test read capability for each advanced feature.
  const advFixtures = {
    STATISTICS:          FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_STATISTICS.parquet') : null,
    PAGE_INDEX:          FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_PAGE_INDEX.parquet') : null,
    DATA_PAGE_V2:        FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_DATA_PAGE_V2.parquet') : null,
    SIZE_STATISTICS:     FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_SIZE_STATISTICS.parquet') : null,
    PAGE_CRC32:          FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_PAGE_CRC32.parquet') : null,
    PREDICATE_PUSHDOWN:  FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_PREDICATE_PUSHDOWN.parquet') : null,
    PROJECTION_PUSHDOWN: FIXTURES_DIR ? join(FIXTURES_DIR, 'advanced_features', 'adv_PROJECTION_PUSHDOWN.parquet') : null,
  };

  for (const [feat, fixturePath] of Object.entries(advFixtures)) {
    results.advanced_features[feat] = await testReadFixture(fixturePath);
  }

  // Features that require write-side capabilities not testable via read-only fixtures:
  results.advanced_features.BLOOM_FILTER = notSupported('BLOOM_FILTER requires write-side capability not available in hyparquet (read-only library)');
  results.advanced_features.COLUMN_ENCRYPTION = notSupported('COLUMN_ENCRYPTION is not supported by hyparquet');
  results.advanced_features.SCHEMA_EVOLUTION = notSupported('SCHEMA_EVOLUTION requires write-side capability not available in hyparquet (read-only library)');

  process.stdout.write(JSON.stringify(results, null, 2) + '\n');
}

main().catch(err => {
  process.stderr.write(String(err) + '\n');
  process.exit(1);
});
