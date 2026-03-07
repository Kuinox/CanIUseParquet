"use client";

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
  /** True if the test CLI itself failed for this version (infra error, not a feature gap). */
  cliError: boolean;
  /** Fraction of the total timeline height (0–1). */
  heightFraction: number;
}

interface MergedBlock {
  /** Oldest version in this range. */
  versionFrom: string;
  /** Newest version in this range. */
  versionTo: string;
  dateFrom: string | null;
  dateTo: string | null;
  writeSupported: boolean;
  readSupported: boolean;
  /** True if all versions in this block had CLI errors (untested, not unsupported). */
  cliError: boolean;
  /** Combined height fraction of all merged blocks. */
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
  const cliErrors = new Set(tool.cli_error_versions ?? []);
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
    const isCliError = cliErrors.has(ver);
    const writeOk =
      !isCliError &&
      !entry?.not_applicable &&
      !!entry?.write &&
      !!entry?.write_since &&
      versionGte(ver, entry.write_since);
    const readOk =
      !isCliError &&
      !entry?.not_applicable &&
      !!entry?.read &&
      !!entry?.read_since &&
      versionGte(ver, entry.read_since);

    return {
      version: ver,
      date: dates[ver] ?? null,
      writeSupported: writeOk,
      readSupported: readOk,
      cliError: isCliError,
      heightFraction: normalised[i],
    };
  });
}

/**
 * Merge consecutive VersionBlocks that have the same support status into
 * a single MergedBlock showing the version range.
 */
function mergeBlocks(blocks: VersionBlock[]): MergedBlock[] {
  if (blocks.length === 0) return [];

  const merged: MergedBlock[] = [];
  let current: MergedBlock = {
    versionFrom: blocks[0].version,
    versionTo: blocks[0].version,
    dateFrom: blocks[0].date,
    dateTo: blocks[0].date,
    writeSupported: blocks[0].writeSupported,
    readSupported: blocks[0].readSupported,
    cliError: blocks[0].cliError,
    heightFraction: blocks[0].heightFraction,
  };

  for (let i = 1; i < blocks.length; i++) {
    const block = blocks[i];
    if (
      block.writeSupported === current.writeSupported &&
      block.readSupported === current.readSupported &&
      block.cliError === current.cliError
    ) {
      // Same status — extend the range
      current.versionTo = block.version;
      current.dateTo = block.date;
      current.heightFraction += block.heightFraction;
    } else {
      merged.push(current);
      current = {
        versionFrom: block.version,
        versionTo: block.version,
        dateFrom: block.date,
        dateTo: block.date,
        writeSupported: block.writeSupported,
        readSupported: block.readSupported,
        cliError: block.cliError,
        heightFraction: block.heightFraction,
      };
    }
  }
  merged.push(current);
  return merged;
}

function blockBg(block: MergedBlock): string {
  if (block.cliError) return "bg-gray-700";
  if (block.writeSupported && block.readSupported) return "bg-green-700";
  if (block.writeSupported) return "bg-amber-600";
  if (block.readSupported) return "bg-yellow-500";
  return "bg-red-900";
}

function blockLabel(block: MergedBlock): string {
  if (block.cliError) return "?";
  if (block.writeSupported && block.readSupported) return "W+R";
  if (block.writeSupported) return "Write only";
  if (block.readSupported) return "Read only";
  return "✕";
}

function versionRangeLabel(block: MergedBlock): string {
  if (block.versionFrom === block.versionTo) return `v${block.versionFrom}`;
  return `v${block.versionFrom} – v${block.versionTo}`;
}

function dateRangeLabel(block: MergedBlock): string | null {
  if (!block.dateFrom) return null;
  const from = block.dateFrom.slice(0, 7);
  if (!block.dateTo || block.dateTo === block.dateFrom) return from;
  return `${from} – ${block.dateTo.slice(0, 7)}`;
}

function tooltipStatusColor(block: MergedBlock): string {
  if (block.cliError) return "text-gray-400";
  if (block.writeSupported && block.readSupported) return "text-green-400";
  if (block.writeSupported) return "text-amber-400";
  if (block.readSupported) return "text-yellow-400";
  return "text-red-400";
}

// ─── Component ───────────────────────────────────────────────────────────────

interface Props {
  toolIds: string[];
  tools: Record<string, ToolData>;
  getEntry: (tool: ToolData) => FeatureEntry | undefined;
}

const CHART_HEIGHT_PX = 600;

export default function FeatureTimeline({
  toolIds,
  tools,
  getEntry,
}: Props) {
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

  const toolMergedBlocks: Record<string, MergedBlock[]> = {};
  for (const tid of toolIds) {
    const raw = buildBlocks(tools[tid], getEntry(tools[tid]), globalStartMs, globalEndMs);
    toolMergedBlocks[tid] = mergeBlocks(raw);
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

  return (
    <div className="w-full">
      {/* Legend */}
      <div className="flex flex-wrap gap-4 mb-2 text-xs text-gray-400">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-green-700" /> Write &amp; Read
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-amber-600" /> Write only
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-yellow-500" /> Read only
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-red-900" /> Not supported
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-gray-700" /> CLI error (untested)
        </span>
      </div>


      {/* Chart */}
      <div className="overflow-x-auto">
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
              const blocks = toolMergedBlocks[tid];
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
                        key={block.versionFrom}
                        className={`group relative flex-shrink-0 overflow-hidden ${blockBg(block)} border-t border-black/30 transition-opacity hover:opacity-90`}
                        style={{
                          height: `${block.heightFraction * 100}%`,
                          minHeight: "2px",
                        }}
                        title={`${versionRangeLabel(block)}: ${blockLabel(block)}`}
                      >
                        {/* Content visible when block is tall enough */}
                        <div className="absolute inset-0 flex flex-col items-center justify-center px-1 overflow-hidden">
                          <span className="text-[9px] font-mono text-white/90 truncate leading-tight w-full text-center">
                            {versionRangeLabel(block)}
                          </span>
                          <span className="text-[8px] text-white/80 leading-tight">
                            {blockLabel(block)}
                          </span>
                        </div>

                        {/* Tooltip on hover for small blocks */}
                        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 z-30 hidden group-hover:flex flex-col items-center bg-gray-900 border border-gray-700 rounded px-2 py-1 text-[10px] whitespace-nowrap shadow-lg pointer-events-none">
                          <span className="font-mono text-white">{versionRangeLabel(block)}</span>
                          {dateRangeLabel(block) && (
                            <span className="text-gray-400">{dateRangeLabel(block)}</span>
                          )}
                          {block.cliError ? (
                            <span className={tooltipStatusColor(block)}>
                              CLI error — not tested
                            </span>
                          ) : (
                            <span className={tooltipStatusColor(block)}>
                              {block.writeSupported ? "✓ Write" : "✗ Write"} · {block.readSupported ? "✓ Read" : "✗ Read"}
                            </span>
                          )}
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
}
