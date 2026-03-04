export interface FeatureEntry {
  supported: boolean;
  since: string | null;
  not_applicable?: boolean;
}

export interface ToolData {
  display_name: string;
  language: string;
  latest_version: string;
  tested_versions: string[];
  compression: Record<string, FeatureEntry>;
  encoding: Record<string, Record<string, FeatureEntry>>;
  logical_types: Record<string, FeatureEntry>;
  nested_types: Record<string, FeatureEntry>;
  advanced_features: Record<string, FeatureEntry>;
}

export interface BuildMetadata {
  expected_tools: string[];
  available_tools: string[];
  missing_tools: string[];
}

export interface MatrixData {
  tools: Record<string, ToolData>;
  categories: {
    compression: string[];
    encoding: string[];
    encoding_types: string[];
    logical_types: string[];
    nested_types: string[];
    advanced_features: string[];
  };
  build_metadata?: BuildMetadata;
}
