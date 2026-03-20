"use client";

import { useState } from "react";
import Link from "next/link";
import { MatrixData, FeatureEntry, ToolData } from "../types/matrix";
import { InternalCategory, featureToSlug } from "../lib/data";
import LogModal from "./LogModal";

// ─── Shared cell components ───────────────────────────────────────────────────

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

function MergedBadge({
  supported,
  since,
}: {
  supported: boolean;
  since?: string | null;
}) {
  if (supported) {
    return (
      <span className="flex items-center gap-0.5 text-[10px]">
        <span className="text-green-400">✅</span>
        {since && <span className="text-green-500">{since}+</span>}
      </span>
    );
  }
  return (
    <span className="flex items-center gap-0.5 text-[10px]">
      <span className="text-red-400">❌</span>
    </span>
  );
}

function FeatureCell({
  entry,
  onClick,
}: {
  entry: FeatureEntry | undefined;
  onClick?: () => void;
}) {
  if (!entry) {
    return (
      <td className="px-3 py-2 text-center">
        <span className="text-gray-600" title="Not tested">
          ➖
        </span>
      </td>
    );
  }
  if (entry.not_applicable) {
    return (
      <td className="px-3 py-2 text-center bg-gray-800/30">
        <span
          className="text-gray-500 cursor-default"
          title="Not applicable per Parquet spec"
        >
          —
        </span>
      </td>
    );
  }
  if (entry.cli_error) {
    return (
      <td className="px-3 py-2 text-center bg-orange-950/20">
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
  let bgClass = "";
  if (bothSupported) bgClass = "bg-green-950/30";
  else if (neitherSupported) bgClass = "bg-red-950/20";
  else bgClass = "bg-yellow-950/20";

  const canMerge =
    entry.write === entry.read && entry.write_since === entry.read_since;

  const hasLogs = !!(entry.write_log || entry.read_log);
  // Show proof status for any entry that has at least one success
  const hasSuccess = entry.write || entry.read;
  const isClickable = hasLogs || hasSuccess;
  const missingProof = hasSuccess && !hasLogs;

  return (
    <td
      className={`px-3 py-2 text-center ${bgClass}${isClickable ? " cursor-pointer hover:brightness-125 hover:ring-1 hover:ring-inset hover:ring-gray-500" : ""}`}
      onClick={isClickable ? onClick : undefined}
      title={hasLogs ? "Click to view test logs" : missingProof ? "Click to view proof status" : undefined}
    >
      <div className="flex flex-col items-center gap-0.5">
        {canMerge ? (
          <MergedBadge supported={entry.write} since={entry.write_since} />
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
        {hasLogs && (
          <span className="text-[9px] text-gray-500 mt-0.5">📋 logs</span>
        )}
        {missingProof && (
          <span className="text-[9px] text-yellow-600 mt-0.5" title="No proof available">⚠ no proof</span>
        )}
      </div>
    </td>
  );
}

// ─── Non-encoding feature table ───────────────────────────────────────────────

function NonEncodingTable({
  features,
  tools,
  toolIds,
  getEntry,
  categorySlug,
  onCellClick,
}: {
  features: string[];
  tools: Record<string, ToolData>;
  toolIds: string[];
  getEntry: (tool: ToolData, feature: string) => FeatureEntry | undefined;
  categorySlug: string;
  onCellClick: (toolId: string, toolName: string, featureName: string, entry: FeatureEntry) => void;
}) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-800">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-900">
            <th className="px-3 py-2 text-left text-gray-300 font-medium sticky left-0 bg-gray-900 z-10 min-w-[180px]">
              Feature
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
          {features.map((feature, i) => (
            <tr
              key={feature}
              className={`group ${
                i % 2 === 0 ? "bg-gray-900/50" : "bg-gray-950"
              } hover:bg-blue-950/30 transition-colors`}
            >
              <td className="px-3 py-2 font-mono text-xs sticky left-0 bg-inherit z-10 text-gray-300">
                <Link
                  href={`/${categorySlug}/${featureToSlug(feature)}`}
                  className="hover:text-green-400 transition-colors"
                >
                  {feature}
                </Link>
              </td>
              {toolIds.map((tid) => {
                const entry = getEntry(tools[tid], feature);
                return (
                  <FeatureCell
                    key={tid}
                    entry={entry}
                    onClick={entry ? () => onCellClick(tid, tools[tid].display_name, feature, entry) : undefined}
                  />
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Encoding table ───────────────────────────────────────────────────────────

function EncodingSection({
  data,
  tools,
  toolIds,
  onCellClick,
}: {
  data: MatrixData;
  tools: Record<string, ToolData>;
  toolIds: string[];
  onCellClick: (toolId: string, toolName: string, featureName: string, entry: FeatureEntry) => void;
}) {
  const [activeEncoding, setActiveEncoding] = useState<string>(
    data.categories.encoding[0]
  );

  return (
    <div>
      <p className="text-gray-400 text-sm mb-4">
        Each encoding is tested with each physical data type. Select an encoding
        to see type support.
      </p>
      <div className="flex flex-wrap gap-2 mb-4">
        {data.categories.encoding.map((enc) => (
          <button
            key={enc}
            onClick={() => setActiveEncoding(enc)}
            className={`px-3 py-1.5 rounded text-xs font-mono transition-colors ${
              activeEncoding === enc
                ? "bg-blue-600 text-white"
                : "bg-gray-800 text-gray-400 hover:bg-gray-700"
            }`}
          >
            {enc}
          </button>
        ))}
      </div>
      {activeEncoding && (
        <div className="mb-2 flex items-center gap-3">
          <span className="text-sm text-gray-400">
            Viewing{" "}
            <span className="font-mono text-gray-200">{activeEncoding}</span>
          </span>
          <Link
            href={`/encoding/${featureToSlug(activeEncoding)}`}
            className="text-xs text-green-400 hover:underline"
          >
            View {activeEncoding} feature page →
          </Link>
        </div>
      )}
      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-900">
              <th className="px-3 py-2 text-left text-gray-300 font-medium sticky left-0 bg-gray-900 z-10 min-w-[180px]">
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
            {data.categories.encoding_types.map((dataType, i) => (
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
                  const entry = tools[tid].encoding[activeEncoding]?.[dataType];
                  const featureName = `${activeEncoding} × ${dataType}`;
                  return (
                    <FeatureCell
                      key={tid}
                      entry={entry}
                      onClick={entry ? () => onCellClick(tid, tools[tid].display_name, featureName, entry) : undefined}
                    />
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

interface Props {
  data: MatrixData;
  category: InternalCategory;
  categorySlug: string;
}

interface SelectedCell {
  toolName: string;
  featureName: string;
  entry: FeatureEntry;
}

export default function CategoryMatrix({
  data,
  category,
  categorySlug,
}: Props) {
  const toolIds = Object.keys(data.tools);
  const tools = data.tools;
  const [selectedCell, setSelectedCell] = useState<SelectedCell | null>(null);

  function handleCellClick(
    _toolId: string,
    toolName: string,
    featureName: string,
    entry: FeatureEntry
  ) {
    setSelectedCell({ toolName, featureName, entry });
  }

  function getEntryForCategory(
    tool: ToolData,
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

  return (
    <div>
      {category === "encoding" ? (
        <EncodingSection
          data={data}
          tools={tools}
          toolIds={toolIds}
          onCellClick={handleCellClick}
        />
      ) : (
        <NonEncodingTable
          features={
            category === "compression"
              ? data.categories.compression
              : category === "logical_types"
                ? data.categories.logical_types
                : category === "nested_types"
                  ? data.categories.nested_types
                  : data.categories.advanced_features
          }
          tools={tools}
          toolIds={toolIds}
          getEntry={getEntryForCategory}
          categorySlug={categorySlug}
          onCellClick={handleCellClick}
        />
      )}

      {selectedCell && (
        <LogModal
          toolName={selectedCell.toolName}
          featureName={selectedCell.featureName}
          entry={selectedCell.entry}
          onClose={() => setSelectedCell(null)}
        />
      )}
    </div>
  );
}

