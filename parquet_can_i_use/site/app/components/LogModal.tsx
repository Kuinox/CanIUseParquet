"use client";

import { useEffect } from "react";
import { FeatureEntry } from "../types/matrix";

interface Props {
  toolName: string;
  featureName: string;
  entry: FeatureEntry;
  onClose: () => void;
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

          {!hasWriteLog && !hasReadLog && (
            <p className="text-gray-400 text-sm italic">
              No error logs available for this result.
            </p>
          )}

          {hasWriteLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                <span className="text-red-400">❌</span> Write error
              </h3>
              <pre className="bg-gray-950 border border-gray-800 rounded-lg p-3 text-xs text-red-300 font-mono overflow-x-auto whitespace-pre-wrap break-words">
                {entry.write_log}
              </pre>
            </div>
          )}

          {hasReadLog && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2 flex items-center gap-2">
                <span className="text-red-400">❌</span> Read error
              </h3>
              <pre className="bg-gray-950 border border-gray-800 rounded-lg p-3 text-xs text-red-300 font-mono overflow-x-auto whitespace-pre-wrap break-words">
                {entry.read_log}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
