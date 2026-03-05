import { MatrixData } from "../types/matrix";

export type InternalCategory =
  | "compression"
  | "encoding"
  | "logical_types"
  | "nested_types"
  | "advanced_features";

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
