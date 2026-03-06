export interface ApacheRef {
  write: boolean;
  read: boolean;
  note?: string;
}

export interface FeatureEntry {
  write: boolean;
  read: boolean;
  write_since?: string | null;
  read_since?: string | null;
  not_applicable?: boolean;
  apache_ref?: ApacheRef;
}

export interface ToolData {
  display_name: string;
  language: string;
  latest_version: string;
  tested_versions: string[];
  /** All versions from versions.json, ordered oldest to newest */
  all_versions?: string[];
  /** Map from version string to ISO date string (YYYY-MM-DD) */
  version_dates: Record<string, string>;
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
