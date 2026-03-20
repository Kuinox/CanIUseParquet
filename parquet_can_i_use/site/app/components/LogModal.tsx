"use client";

import { useEffect } from "react";
import { FeatureEntry } from "../types/matrix";

interface Props {
  toolName: string;
  featureName: string;
  entry: FeatureEntry;
  onClose: () => void;
}

/** Parse a write_log in the format "sha256:<hex>\n<base64-data>" */
function parseWriteProof(writeLog: string): { hash: string; base64Data: string } | null {
  if (!writeLog.startsWith("sha256:")) return null;
  const newlineIdx = writeLog.indexOf("\n");
  if (newlineIdx === -1) return null;
  const hash = writeLog.slice("sha256:".length, newlineIdx);
  const base64Data = writeLog.slice(newlineIdx + 1);
  return { hash, base64Data };
}

export default function LogModal({ toolName, featureName, entry, onClose }: Props) {
  // Close on Escape key
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const hasWriteLog = !!entry.write_log;
  const hasReadLog = !!entry.read_log;

  const writeProof = entry.write_log ? parseWriteProof(entry.write_log) : null;
  const isWriteProof = entry.write && writeProof !== null;

  const downloadFilename = `${featureName.toLowerCase().replace(/[\s\u00D7/\\]+/g, "-")}.parquet`;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onClick={onClose}
    >
      <div
        className="bg-gray-900 border border-gray-700 rounded-xl shadow-2xl max-w-3xl w-full max-h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-700 shrink-0">
          <div>
            <h2 className="text-white font-semibold text-lg">Test Logs</h2>
            <p className="text-gray-400 text-sm mt-0.5">
              <span className="font-mono text-gray-200">{toolName}</span>
              {" · "}
              <span className="font-mono text-gray-200">{featureName}</span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-200 transition-colors text-xl leading-none p-1"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto p-5 space-y-4">
          {/* Result summary */}
          <div className="flex gap-4 text-sm">
            <span className={entry.write ? "text-green-400" : "text-red-400"}>
              Write: {entry.write ? "✅" : "❌"}
            </span>
            <span className={entry.read ? "text-green-400" : "text-red-400"}>
              Read: {entry.read ? "✅" : "❌"}
            </span>
          </div>

          {!hasWriteLog && !hasReadLog && !entry.write && !entry.read && (
            <p className="text-gray-400 text-sm italic">
              No logs available for this result.
            </p>
          )}

          {/* Write section */}
          {entry.write && !hasWriteLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                <span className="text-yellow-400">⚠️</span> Write proof
              </h3>
              <p className="text-gray-400 text-sm italic">
                No proof file available for this write result.
              </p>
            </div>
          )}

          {hasWriteLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                {isWriteProof
                  ? <><span className="text-green-400">✅</span> Write proof</>
                  : <><span className="text-red-400">❌</span> Write error</>}
              </h3>
              {isWriteProof && writeProof ? (
                <div className="bg-gray-950 border border-gray-800 rounded-lg p-3 space-y-2">
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    <span className="text-xs font-mono text-green-300 break-all">
                      sha256:{writeProof.hash}
                    </span>
                    <a
                      href={`data:application/octet-stream;base64,${writeProof.base64Data}`}
                      download={downloadFilename}
                      className="shrink-0 px-3 py-1 text-xs bg-green-700 hover:bg-green-600 text-white rounded transition-colors"
                      onClick={(e) => e.stopPropagation()}
                    >
                      ⬇ Download .parquet
                    </a>
                  </div>
                </div>
              ) : (
                <pre className="bg-gray-950 border border-gray-800 rounded-lg p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-words text-red-300">
                  {entry.write_log}
                </pre>
              )}
            </div>
          )}

          {hasReadLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                {entry.read && entry.read_log?.startsWith("proof_sha256:")
                  ? <><span className="text-green-400">✅</span> Read proof</>
                  : <><span className="text-red-400">❌</span> Read error</>}
              </h3>
              <pre className={`bg-gray-950 border border-gray-800 rounded-lg p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-words ${entry.read && entry.read_log?.startsWith("proof_sha256:") ? "text-green-300" : "text-red-300"}`}>
                {entry.read_log}
              </pre>
            </div>
          )}

          {entry.read && !hasReadLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                <span className="text-yellow-400">⚠️</span> Read proof
              </h3>
              <p className="text-gray-400 text-sm italic">
                No proof available for this read result.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
