"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { SearchItem } from "../lib/data";

interface Props {
  items: SearchItem[];
}

export default function SearchBar({ items }: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const router = useRouter();
  const containerRef = useRef<HTMLDivElement>(null);

  const results =
    query.length >= 1
      ? items
          .filter(
            (item) =>
              item.name.toLowerCase().includes(query.toLowerCase()) ||
              item.categoryLabel.toLowerCase().includes(query.toLowerCase())
          )
          .slice(0, 8)
      : [];

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Escape") {
      setOpen(false);
      setQuery("");
    } else if (e.key === "Enter" && results.length > 0) {
      router.push(results[0].href);
      setOpen(false);
      setQuery("");
    }
  }

  function handleSelect(href: string) {
    router.push(href);
    setOpen(false);
    setQuery("");
  }

  return (
    <div ref={containerRef} className="relative w-full">
      <div className="relative">
        <svg
          className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z"
          />
        </svg>
        <input
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setOpen(true);
          }}
          onFocus={() => query.length >= 1 && setOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder="Search features..."
          className="w-full bg-gray-800/90 border border-gray-700 rounded-lg pl-9 pr-4 py-2 text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-green-500 focus:ring-1 focus:ring-green-500/30 transition-colors"
        />
      </div>

      {open && query.length >= 1 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800/90 border border-gray-700 rounded-lg shadow-xl z-50 overflow-hidden backdrop-blur-sm">
          {results.length === 0 ? (
            <div className="px-4 py-3 text-sm text-gray-400">
              No features found
            </div>
          ) : (
            <ul>
              {results.map((item) => (
                <li key={item.href}>
                  <button
                    className="w-full text-left px-4 py-2.5 hover:bg-gray-700 transition-colors flex items-center justify-between gap-3"
                    onClick={() => handleSelect(item.href)}
                  >
                    <span className="font-mono text-sm text-gray-100">
                      {item.name}
                    </span>
                    <span className="text-xs text-gray-400 flex-shrink-0">
                      {item.categoryLabel}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
