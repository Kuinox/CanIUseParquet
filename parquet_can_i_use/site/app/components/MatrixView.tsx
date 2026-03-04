"use client";

import { useState } from "react";
import { MatrixData, FeatureEntry, ToolData } from "../types/matrix";

function FeatureCell({ entry }: { entry: FeatureEntry | undefined }) {
  if (!entry) {
    return (
      <td className="px-3 py-2 text-center">
        <span className="text-gray-600" title="Not tested">➖</span>
      </td>
    );
  }

  if (entry.not_applicable) {
    return (
      <td className="px-3 py-2 text-center bg-gray-800/30">
        <span className="text-gray-500 cursor-default" title="Not applicable per Parquet spec">—</span>
      </td>
    );
  }

  if (entry.supported) {
    return (
      <td className="px-3 py-2 text-center bg-green-950/30">
        <span
          className="text-green-400 cursor-default"
          title={entry.since ? `Supported since v${entry.since}` : "Supported"}
        >
          ✅{entry.since && (
            <span className="text-[10px] text-green-500 block leading-tight">
              {entry.since}+
            </span>
          )}
        </span>
      </td>
    );
  }

  return (
    <td className="px-3 py-2 text-center bg-red-950/20">
      <span className="text-red-400 cursor-default" title="Not supported">❌</span>
    </td>
  );
}

function FeatureTable({
  title,
  features,
  tools,
  toolIds,
  getEntry,
}: {
  title: string;
  features: string[];
  tools: Record<string, ToolData>;
  toolIds: string[];
  getEntry: (tool: ToolData, feature: string) => FeatureEntry | undefined;
}) {
  return (
    <div className="mb-8">
      <h2 className="text-xl font-semibold mb-3 text-white">{title}</h2>
      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-900">
              <th className="px-3 py-2 text-left text-gray-300 font-medium sticky left-0 bg-gray-900 z-10 min-w-[180px]">
                Feature
              </th>
              {toolIds.map((tid) => (
                <th key={tid} className="px-3 py-2 text-center text-gray-300 font-medium min-w-[100px]">
                  <div>{tools[tid].display_name}</div>
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
                className={i % 2 === 0 ? "bg-gray-900/50" : "bg-gray-950"}
              >
                <td className="px-3 py-2 font-mono text-xs text-gray-300 sticky left-0 bg-inherit z-10">
                  {feature}
                </td>
                {toolIds.map((tid) => (
                  <FeatureCell key={tid} entry={getEntry(tools[tid], feature)} />
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

type Category = "compression" | "encoding" | "logical_types" | "nested_types" | "advanced_features";

const CATEGORY_LABELS: Record<Category, string> = {
  compression: "Compression Codecs",
  encoding: "Encoding Types × Data Types",
  logical_types: "Logical Types",
  nested_types: "Nested & Complex Types",
  advanced_features: "Advanced Features",
};

export default function MatrixView({ data }: { data: MatrixData }) {
  const [activeCategory, setActiveCategory] = useState<Category>("compression");
  const [activeEncoding, setActiveEncoding] = useState<string>(data.categories.encoding[0]);

  const toolIds = Object.keys(data.tools);
  const tools = data.tools;
  const categories = Object.keys(CATEGORY_LABELS) as Category[];

  return (
    <div>
      {/* Legend */}
      <div className="mb-6 flex flex-wrap gap-4 text-sm text-gray-400">
        <span>
          <span className="text-green-400">✅</span> Supported{" "}
          <span className="text-green-500 text-xs">(with version if verified)</span>
        </span>
        <span>
          <span className="text-red-400">❌</span> Not supported
        </span>
        <span>
          <span className="text-gray-500">—</span> Not applicable per Parquet spec
        </span>
        <span>
          <span className="text-gray-600">➖</span> Not tested
        </span>
      </div>

      {/* Tools overview */}
      <div className="mb-8 overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-900">
              <th className="px-3 py-2 text-left text-gray-300">Tool</th>
              <th className="px-3 py-2 text-left text-gray-300">Language</th>
              <th className="px-3 py-2 text-left text-gray-300">Latest</th>
              <th className="px-3 py-2 text-left text-gray-300">Versions Tested</th>
            </tr>
          </thead>
          <tbody>
            {toolIds.map((tid, i) => (
              <tr key={tid} className={i % 2 === 0 ? "bg-gray-900/50" : "bg-gray-950"}>
                <td className="px-3 py-2 font-semibold text-white">{tools[tid].display_name}</td>
                <td className="px-3 py-2 text-gray-400">{tools[tid].language}</td>
                <td className="px-3 py-2 font-mono text-green-400">{tools[tid].latest_version}</td>
                <td className="px-3 py-2 font-mono text-gray-500 text-xs">
                  {tools[tid].tested_versions.join(", ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Category tabs */}
      <div className="flex flex-wrap gap-2 mb-6">
        {categories.map((cat) => (
          <button
            key={cat}
            onClick={() => setActiveCategory(cat)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              activeCategory === cat
                ? "bg-green-600 text-white"
                : "bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-gray-200"
            }`}
          >
            {CATEGORY_LABELS[cat]}
          </button>
        ))}
      </div>

      {/* Category content */}
      {activeCategory === "compression" && (
        <FeatureTable
          title="Compression Codecs"
          features={data.categories.compression}
          tools={tools}
          toolIds={toolIds}
          getEntry={(tool, feature) => tool.compression[feature]}
        />
      )}

      {activeCategory === "encoding" && (
        <div>
          <h2 className="text-xl font-semibold mb-3 text-white">Encoding Types × Data Types</h2>
          <p className="text-gray-400 text-sm mb-4">
            Each encoding is tested with each physical data type. Select an encoding to see type support.
          </p>

          {/* Encoding selector */}
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

          <FeatureTable
            title={activeEncoding}
            features={data.categories.encoding_types}
            tools={tools}
            toolIds={toolIds}
            getEntry={(tool, feature) => tool.encoding[activeEncoding]?.[feature]}
          />
        </div>
      )}

      {activeCategory === "logical_types" && (
        <FeatureTable
          title="Logical Types"
          features={data.categories.logical_types}
          tools={tools}
          toolIds={toolIds}
          getEntry={(tool, feature) => tool.logical_types[feature]}
        />
      )}

      {activeCategory === "nested_types" && (
        <FeatureTable
          title="Nested & Complex Types"
          features={data.categories.nested_types}
          tools={tools}
          toolIds={toolIds}
          getEntry={(tool, feature) => tool.nested_types[feature]}
        />
      )}

      {activeCategory === "advanced_features" && (
        <FeatureTable
          title="Advanced Features"
          features={data.categories.advanced_features}
          tools={tools}
          toolIds={toolIds}
          getEntry={(tool, feature) => tool.advanced_features[feature]}
        />
      )}
    </div>
  );
}
