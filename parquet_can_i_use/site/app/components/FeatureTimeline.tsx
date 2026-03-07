"use client";

import { useEffect } from "react";
import { ToolData, FeatureEntry } from "../types/matrix";

// ─── Semver helpers ─────────────────────────────────────────────────────────

function parseSemver(v: string): number[] {
  return v.split(".").map((p) => parseInt(p.replace(/[^0-9]/g, ""), 10) || 0);
}

/** Returns true if `v` >= `since` (both are version strings). */
function versionGte(v: string, since: string): boolean {
  const a = parseSemver(v);
  const b = parseSemver(since);
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const ai = a[i] ?? 0;
    const bi = b[i] ?? 0;
    if (ai > bi) return true;
    if (ai < bi) return false;
  }
  return true; // equal
}

// ─── Types ───────────────────────────────────────────────────────────────────

interface VersionBlock {
  version: string;
  date: string | null; // ISO YYYY-MM-DD, or null if unknown
  writeSupported: boolean;
  readSupported: boolean;
  /** Fraction of the total timeline height (0–1). */
  heightFraction: number;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function dateToMs(iso: string): number {
  return new Date(iso).getTime();
}

const TODAY_MS = Date.now();
const MIN_BLOCK_FRAC = 0.015; // minimum 1.5% height so tiny blocks are visible

/**
 * Build VersionBlock[] for one tool + feature entry.
 * Versions are ordered oldest→newest.
 */
function buildBlocks(
  tool: ToolData,
  entry: FeatureEntry | undefined,
  globalStartMs: number,
  globalEndMs: number,
): VersionBlock[] {
  const versions = tool.all_versions ?? tool.tested_versions;
  const dates = tool.version_dates ?? {};
  const totalMs = globalEndMs - globalStartMs;

  // Compute raw durations: time from this version's release to the next's release
  const rawDurations: number[] = versions.map((ver, i) => {
    const startMs = dates[ver] ? dateToMs(dates[ver]) : null;
    const endMs =
      i + 1 < versions.length && dates[versions[i + 1]]
        ? dateToMs(dates[versions[i + 1]])
        : TODAY_MS;
    if (startMs === null) return 0;
    return Math.max(endMs - startMs, 0);
  });

  // Convert to fractions of the global timeline, with a minimum floor
  const fractions = rawDurations.map((d) => (totalMs > 0 ? d / totalMs : 0));

  // Apply minimum floor and rescale
  const floored = fractions.map((f) => Math.max(f, MIN_BLOCK_FRAC));
  const flooredSum = floored.reduce((a, b) => a + b, 0);
  const normalised = floored.map((f) => (flooredSum > 0 ? f / flooredSum : 1 / versions.length));

  return versions.map((ver, i) => {
    const writeOk =
      !entry?.not_applicable &&
      !!entry?.write &&
      !!entry?.write_since &&
      versionGte(ver, entry.write_since);
    const readOk =
      !entry?.not_applicable &&
      !!entry?.read &&
      !!entry?.read_since &&
      versionGte(ver, entry.read_since);

    return {
      version: ver,
      date: dates[ver] ?? null,
      writeSupported: writeOk,
      readSupported: readOk,
      heightFraction: normalised[i],
    };
  });
}

function blockBg(block: VersionBlock): string {
  if (block.writeSupported && block.readSupported) return "bg-green-700";
  if (block.writeSupported || block.readSupported) return "bg-yellow-700";
  return "bg-red-900";
}

function blockLabel(block: VersionBlock): string {
  if (block.writeSupported && block.readSupported) return "W+R";
  if (block.writeSupported) return "W";
  if (block.readSupported) return "R";
  return "✕";
}

// ─── Component ───────────────────────────────────────────────────────────────

interface Props {
  feature: string;
  /** Label shown in the heading, e.g. "SNAPPY" or "PLAIN × INT32" */
  featureLabel: string;
  toolIds: string[];
  tools: Record<string, ToolData>;
  getEntry: (tool: ToolData) => FeatureEntry | undefined;
  /** Required when `inline` is false/undefined. */
  onClose?: () => void;
  /** When true, renders inline instead of as a modal popup. */
  inline?: boolean;
}

const CHART_HEIGHT_PX = 600;

export default function FeatureTimeline({
  featureLabel,
  toolIds,
  tools,
  getEntry,
  onClose,
  inline,
}: Props) {
  // Close on Escape key (only relevant in popup mode)
  useEffect(() => {
    if (inline) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose?.(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose, inline]);

  // Determine the global time range across all tools that have dates
  let globalStartMs = Infinity;
  for (const tid of toolIds) {
    const dates = Object.values(tools[tid].version_dates ?? {});
    for (const d of dates) {
      const ms = dateToMs(d);
      if (ms < globalStartMs) globalStartMs = ms;
    }
  }
  if (!isFinite(globalStartMs)) globalStartMs = dateToMs("2018-01-01");
  const globalEndMs = TODAY_MS;

  const toolBlocks: Record<string, VersionBlock[]> = {};
  for (const tid of toolIds) {
    toolBlocks[tid] = buildBlocks(tools[tid], getEntry(tools[tid]), globalStartMs, globalEndMs);
  }

  // Year markers along the Y axis
  const startYear = new Date(globalStartMs).getFullYear();
  const endYear = new Date(globalEndMs).getFullYear();
  const years: { label: string; frac: number }[] = [];
  for (let y = startYear; y <= endYear; y++) {
    const ms = new Date(`${y}-01-01`).getTime();
    const frac = (ms - globalStartMs) / (globalEndMs - globalStartMs);
    if (frac >= 0 && frac <= 1) {
      years.push({ label: String(y), frac });
    }
  }

  const content = (
    <div className="bg-gray-950 border border-gray-700 rounded-xl shadow-2xl w-full max-w-6xl">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-gray-800">
        <div>
          <h2 className="text-xl font-bold text-white">
            <span className="text-green-400 font-mono">{featureLabel}</span>
            <span className="text-gray-400 text-base font-normal ml-3">— timeline across libraries</span>
          </h2>
          <p className="text-gray-500 text-xs mt-0.5">
            Block height reflects how long each version was the latest release. Older versions at bottom, newer at top.
          </p>
        </div>
        {!inline && (
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-white transition-colors text-2xl leading-none px-2"
            aria-label="Close"
          >
            ×
          </button>
        )}
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-4 px-6 py-3 border-b border-gray-800 text-xs text-gray-400">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-green-700" /> Write &amp; Read
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-yellow-700" /> Write or Read only
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-red-900" /> Not supported
        </span>
      </div>

      {/* Chart */}
      <div className="px-6 py-4 overflow-x-auto">
        <div className="flex gap-0 min-w-max">
          {/* Y-axis year markers */}
          <div
            className="relative flex-shrink-0 w-12 mr-2 text-right"
            style={{ height: `${CHART_HEIGHT_PX}px` }}
          >
            {years.map(({ label, frac }) => (
              <div
                key={label}
                className="absolute right-0 text-[10px] text-gray-500 -translate-y-1/2 pr-1"
                style={{ top: `${(1 - frac) * 100}%` }}
              >
                {label}
              </div>
            ))}
          </div>

          {/* Y-axis grid lines (behind columns) */}
          <div className="relative flex-1 flex gap-3">
            {/* grid lines */}
            <div
              className="absolute inset-0 pointer-events-none"
              style={{ height: `${CHART_HEIGHT_PX}px` }}
            >
              {years.map(({ label, frac }) => (
                <div
                  key={label}
                  className="absolute left-0 right-0 border-t border-gray-800/60"
                  style={{ top: `${(1 - frac) * 100}%` }}
                />
              ))}
            </div>

            {/* Tool columns */}
            {toolIds.map((tid) => {
              const tool = tools[tid];
              const blocks = toolBlocks[tid];
              return (
                <div key={tid} className="flex flex-col items-center flex-shrink-0 w-28">
                  {/* Tool name header */}
                  <div className="mb-2 text-center">
                    <div className="text-xs font-semibold text-white leading-tight">
                      {tool.display_name}
                    </div>
                    <div className="text-[10px] text-gray-500">{tool.language}</div>
                  </div>

                  {/* Version blocks — newest on top, oldest at bottom */}
                  <div
                    className="w-full flex flex-col-reverse rounded overflow-hidden border border-gray-700"
                    style={{ height: `${CHART_HEIGHT_PX}px` }}
                  >
                    {blocks.map((block) => (
                      <div
                        key={block.version}
                        className={`group relative flex-shrink-0 overflow-hidden ${blockBg(block)} border-t border-black/30 transition-opacity hover:opacity-90`}
                        style={{
                          height: `${block.heightFraction * 100}%`,
                          minHeight: "2px",
                        }}
                        title={`v${block.version}${block.date ? ` (${block.date})` : ""}: ${blockLabel(block)}`}
                      >
                        {/* Content visible when block is tall enough */}
                        <div className="absolute inset-0 flex flex-col items-center justify-center px-1 overflow-hidden">
                          <span className="text-[9px] font-mono text-white/90 truncate leading-tight">
                            v{block.version}
                          </span>
                          {block.date && (
                            <span className="text-[8px] text-white/60 truncate leading-tight">
                              {block.date.slice(0, 7)}
                            </span>
                          )}
                          <span className="text-[8px] text-white/80 leading-tight">
                            {blockLabel(block)}
                          </span>
                        </div>

                        {/* Tooltip on hover for small blocks */}
                        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 z-30 hidden group-hover:flex flex-col items-center bg-gray-900 border border-gray-700 rounded px-2 py-1 text-[10px] whitespace-nowrap shadow-lg pointer-events-none">
                          <span className="font-mono text-white">v{block.version}</span>
                          {block.date && (
                            <span className="text-gray-400">{block.date}</span>
                          )}
                          <span className={block.writeSupported && block.readSupported
                            ? "text-green-400"
                            : block.writeSupported || block.readSupported
                            ? "text-yellow-400"
                            : "text-red-400"}>
                            {block.writeSupported ? "✓ Write" : "✗ Write"} · {block.readSupported ? "✓ Read" : "✗ Read"}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );

  if (inline) {
    return content;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center bg-black/70 overflow-auto py-8 px-4">
      {content}
    </div>
  );
}
