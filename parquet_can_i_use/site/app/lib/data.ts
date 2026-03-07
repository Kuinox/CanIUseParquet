import { MatrixData } from "../types/matrix";

export type InternalCategory =
  | "compression"
  | "encoding"
  | "logical_types"
  | "nested_types"
  | "advanced_features";

const PARQUET_FORMAT_BASE =
  "https://github.com/apache/parquet-format/blob/master";

export const CATEGORY_SPEC_LINKS: Record<InternalCategory, string> = {
  compression: `${PARQUET_FORMAT_BASE}/Compression.md`,
  encoding: `${PARQUET_FORMAT_BASE}/Encodings.md`,
  logical_types: `${PARQUET_FORMAT_BASE}/LogicalTypes.md`,
  nested_types: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#nested-types`,
  advanced_features: `${PARQUET_FORMAT_BASE}/README.md`,
};

export const FEATURE_SPEC_LINKS: Record<string, string> = {
  // Compression codecs
  NONE: `${PARQUET_FORMAT_BASE}/Compression.md#uncompressed`,
  SNAPPY: `${PARQUET_FORMAT_BASE}/Compression.md#snappy`,
  GZIP: `${PARQUET_FORMAT_BASE}/Compression.md#gzip`,
  LZO: `${PARQUET_FORMAT_BASE}/Compression.md#lzo`,
  BROTLI: `${PARQUET_FORMAT_BASE}/Compression.md#brotli`,
  LZ4: `${PARQUET_FORMAT_BASE}/Compression.md#lz4`,
  ZSTD: `${PARQUET_FORMAT_BASE}/Compression.md#zstd`,
  LZ4_RAW: `${PARQUET_FORMAT_BASE}/Compression.md#lz4_raw`,
  // Encodings
  PLAIN: `${PARQUET_FORMAT_BASE}/Encodings.md#PLAIN`,
  PLAIN_DICTIONARY: `${PARQUET_FORMAT_BASE}/Encodings.md#DICTIONARY`,
  RLE_DICTIONARY: `${PARQUET_FORMAT_BASE}/Encodings.md#DICTIONARY`,
  RLE: `${PARQUET_FORMAT_BASE}/Encodings.md#RLE`,
  BIT_PACKED: `${PARQUET_FORMAT_BASE}/Encodings.md#BITPACKED`,
  DELTA_BINARY_PACKED: `${PARQUET_FORMAT_BASE}/Encodings.md#DELTAENC`,
  DELTA_LENGTH_BYTE_ARRAY: `${PARQUET_FORMAT_BASE}/Encodings.md#DELTALENGTH`,
  DELTA_BYTE_ARRAY: `${PARQUET_FORMAT_BASE}/Encodings.md#DELTASTRING`,
  BYTE_STREAM_SPLIT: `${PARQUET_FORMAT_BASE}/Encodings.md#BYTESTREAMSPLIT`,
  BYTE_STREAM_SPLIT_EXTENDED: `${PARQUET_FORMAT_BASE}/Encodings.md#BYTESTREAMSPLIT`,
  // Logical types
  STRING: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#string`,
  ENUM: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#enum`,
  UUID: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#uuid`,
  DATE: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#date`,
  TIME_MILLIS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#time`,
  TIME_MICROS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#time`,
  TIME_NANOS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#time`,
  TIMESTAMP_MILLIS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#timestamp`,
  TIMESTAMP_MICROS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#timestamp`,
  TIMESTAMP_NANOS: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#timestamp`,
  DECIMAL: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#decimal`,
  JSON: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#json`,
  BSON: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#bson`,
  FLOAT16: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#float16`,
  INTERVAL: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#interval`,
  UNKNOWN: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#unknown-always-null`,
  VARIANT: `${PARQUET_FORMAT_BASE}/VariantEncoding.md`,
  GEOMETRY: `${PARQUET_FORMAT_BASE}/Geospatial.md`,
  GEOGRAPHY: `${PARQUET_FORMAT_BASE}/Geospatial.md`,
  // Nested types
  LIST: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#lists`,
  MAP: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#maps`,
  STRUCT: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#nested-types`,
  NESTED_LIST: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#lists`,
  NESTED_MAP: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#maps`,
  DEEP_NESTING: `${PARQUET_FORMAT_BASE}/LogicalTypes.md#nested-types`,
  // Advanced features
  BLOOM_FILTER: `${PARQUET_FORMAT_BASE}/BloomFilter.md`,
  COLUMN_ENCRYPTION: `${PARQUET_FORMAT_BASE}/Encryption.md`,
  PAGE_INDEX: `${PARQUET_FORMAT_BASE}/PageIndex.md`,
  SIZE_STATISTICS: `${PARQUET_FORMAT_BASE}/PageIndex.md`,
  PREDICATE_PUSHDOWN: `${PARQUET_FORMAT_BASE}/PageIndex.md`,
  DATA_PAGE_V2: `${PARQUET_FORMAT_BASE}/README.md#data-pages`,
  PAGE_CRC32: `${PARQUET_FORMAT_BASE}/README.md#checksumming`,
};

export function getFeatureSpecLink(
  category: InternalCategory,
  feature: string
): string {
  return FEATURE_SPEC_LINKS[feature] ?? CATEGORY_SPEC_LINKS[category];
}

export const CATEGORY_LABELS: Record<InternalCategory, string> = {
  compression: "Compression Codecs",
  encoding: "Encodings",
  logical_types: "Logical Types",
  nested_types: "Nested & Complex Types",
  advanced_features: "Advanced Features",
};

export const CATEGORY_DESCRIPTIONS: Record<InternalCategory, string> = {
  compression: "Compression algorithms supported when writing Parquet files",
  encoding: "Column encoding schemes and the physical types they support",
  logical_types: "High-level logical types built on top of physical types",
  nested_types: "Nested and complex data structures",
  advanced_features: "Advanced Parquet features for performance and correctness",
};

export const CATEGORY_TO_SLUG: Record<InternalCategory, string> = {
  compression: "compression",
  encoding: "encoding",
  logical_types: "logical-types",
  nested_types: "nested-types",
  advanced_features: "advanced-features",
};

export const SLUG_TO_CATEGORY: Record<string, InternalCategory> = {
  compression: "compression",
  encoding: "encoding",
  "logical-types": "logical_types",
  "nested-types": "nested_types",
  "advanced-features": "advanced_features",
};

export function featureToSlug(feature: string): string {
  return feature.toLowerCase();
}

export function slugToFeature(
  slug: string,
  features: string[]
): string | undefined {
  return features.find((f) => f.toLowerCase() === slug);
}

export function getCategoryFeatures(
  data: MatrixData,
  category: InternalCategory
): string[] {
  switch (category) {
    case "compression":
      return data.categories.compression;
    case "encoding":
      return data.categories.encoding;
    case "logical_types":
      return data.categories.logical_types;
    case "nested_types":
      return data.categories.nested_types;
    case "advanced_features":
      return data.categories.advanced_features;
  }
}

export interface SearchItem {
  name: string;
  category: InternalCategory;
  categorySlug: string;
  categoryLabel: string;
  featureSlug: string;
  href: string;
}

export function buildSearchIndex(
  data: MatrixData,
  basePath: string = ""
): SearchItem[] {
  const items: SearchItem[] = [];
  for (const [cat, slug] of Object.entries(CATEGORY_TO_SLUG) as [
    InternalCategory,
    string,
  ][]) {
    const features = getCategoryFeatures(data, cat);
    for (const feature of features) {
      const featureSlug = featureToSlug(feature);
      items.push({
        name: feature,
        category: cat,
        categorySlug: slug,
        categoryLabel: CATEGORY_LABELS[cat],
        featureSlug,
        href: `${basePath}/${slug}/${featureSlug}`,
      });
    }
  }
  return items;
}
