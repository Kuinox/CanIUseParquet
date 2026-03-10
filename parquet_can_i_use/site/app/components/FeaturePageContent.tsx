"use client";

import { ToolData, FeatureEntry, MatrixData } from "../types/matrix";
import { InternalCategory } from "../lib/data";
import FeatureTimeline from "./FeatureTimeline";

function ReadWriteBadge({
  supported,
  since,
  label,
}: {
  supported: boolean;
  since?: string | null;
  label: string;
}) {
  if (supported) {
    return (
      <span className="flex items-center gap-0.5 text-[10px]">
        <span className="text-gray-500">{label}:</span>
        <span className="text-green-400">✅</span>
        {since && <span className="text-green-500">{since}+</span>}
      </span>
    );
  }
  return (
    <span className="flex items-center gap-0.5 text-[10px]">
      <span className="text-gray-500">{label}:</span>
      <span className="text-red-400">❌</span>
    </span>
  );
}

interface Props {
  feature: string;
  category: InternalCategory;
  toolIds: string[];
  data: {
    tools: Record<string, ToolData>;
    categories: MatrixData["categories"];
  };
}

function getEntryForTool(
  tool: ToolData,
  category: InternalCategory,
  feature: string
): FeatureEntry | undefined {
  switch (category) {
    case "compression":
      return tool.compression[feature];
    case "logical_types":
      return tool.logical_types[feature];
    case "nested_types":
      return tool.nested_types[feature];
    case "advanced_features":
      return tool.advanced_features[feature];
    default:
      return undefined;
  }
}

// For encoding, we show each data type row for this encoding type
function EncodingFeatureContent({
  feature,
  toolIds,
  tools,
  categories,
}: {
  feature: string;
  toolIds: string[];
  tools: Record<string, ToolData>;
  categories: MatrixData["categories"];
}) {
  return (
    <div>
      <h2 className="text-xl font-semibold text-white mb-4">
        Data Type Support
      </h2>
      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-900">
              <th className="px-3 py-2 text-left text-gray-300 font-medium sticky left-0 bg-gray-900 z-10 min-w-[160px]">
                Data Type
              </th>
              {toolIds.map((tid) => (
                <th
                  key={tid}
                  className="px-3 py-2 text-center text-gray-300 font-medium min-w-[100px]"
                >
                  <div className="flex items-center justify-center gap-1">
                    {tools[tid].display_name}
                    {tools[tid].cli_harness_broken && (
                      <span
                        className="text-orange-400"
                        title="CLI harness broken — results do not reflect library capabilities"
                      >
                        ⚠
                      </span>
                    )}
                  </div>
                  <div className="text-[10px] text-gray-500 font-normal">
                    v{tools[tid].latest_version}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {categories.encoding_types.map((dataType, i) => {
              return (
                <tr
                  key={dataType}
                  className={`${
                    i % 2 === 0 ? "bg-gray-900/50" : "bg-gray-950"
                  }`}
                >
                  <td className="px-3 py-2 font-mono text-xs sticky left-0 bg-inherit z-10 text-gray-300">
                    {dataType}
                  </td>
                  {toolIds.map((tid) => {
                    const entry = tools[tid].encoding[feature]?.[dataType];
                    if (!entry) {
                      return (
                        <td key={tid} className="px-3 py-2 text-center">
                          <span
                            className="text-gray-600"
                            title="Not tested"
                          >
                            ➖
                          </span>
                        </td>
                      );
                    }
                    if (entry.not_applicable) {
                      return (
                        <td
                          key={tid}
                          className="px-3 py-2 text-center bg-gray-800/30"
                        >
                          <span
                            className="text-gray-500"
                            title="Not applicable"
                          >
                            —
                          </span>
                        </td>
                      );
                    }
                    if (entry.cli_error) {
                      return (
                        <td
                          key={tid}
                          className="px-3 py-2 text-center bg-orange-950/20"
                        >
                          <span
                            className="text-orange-400 cursor-default"
                            title="Test infrastructure error — not a library feature gap"
                          >
                            ⚠️
                          </span>
                        </td>
                      );
                    }
                    const bothSupported = entry.write && entry.read;
                    const neitherSupported = !entry.write && !entry.read;
                    const bgClass = bothSupported
                      ? "bg-green-950/30"
                      : neitherSupported
                        ? "bg-red-950/20"
                        : "bg-yellow-950/20";
                    const canMerge =
                      entry.write === entry.read &&
                      entry.write_since === entry.read_since;
                    return (
                      <td
                        key={tid}
                        className={`px-3 py-2 text-center ${bgClass}`}
                      >
                        <div className="flex flex-col items-center gap-0.5">
                          {canMerge ? (
                            <span className="flex items-center gap-0.5 text-[10px]">
                              {entry.write ? (
                                <span className="text-green-400">✅</span>
                              ) : (
                                <span className="text-red-400">❌</span>
                              )}
                              {entry.write_since && (
                                <span className="text-green-500">
                                  {entry.write_since}+
                                </span>
                              )}
                            </span>
                          ) : (
                            <>
                              <ReadWriteBadge
                                supported={entry.write}
                                since={entry.write_since}
                                label="W"
                              />
                              <ReadWriteBadge
                                supported={entry.read}
                                since={entry.read_since}
                                label="R"
                              />
                            </>
                          )}
                        </div>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// For non-encoding: show inline timeline
function NonEncodingContent({
  feature,
  category,
  toolIds,
  tools,
}: {
  feature: string;
  category: InternalCategory;
  toolIds: string[];
  tools: Record<string, ToolData>;
}) {
  return (
    <FeatureTimeline
      toolIds={toolIds}
      tools={tools}
      getEntry={(tool) => getEntryForTool(tool, category, feature)}
    />
  );
}

export default function FeaturePageContent({
  feature,
  category,
  toolIds,
  data,
}: Props) {
  const tools = data.tools;

  if (category === "encoding") {
    return (
      <EncodingFeatureContent
        feature={feature}
        toolIds={toolIds}
        tools={tools}
        categories={data.categories}
      />
    );
  }

  return (
    <NonEncodingContent
      feature={feature}
      category={category}
      toolIds={toolIds}
      tools={tools}
    />
  );
}
