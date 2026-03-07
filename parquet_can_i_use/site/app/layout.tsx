import type { Metadata } from "next";
import "./globals.css";
import { getMatrixData } from "./lib/server";
import { buildSearchIndex } from "./lib/data";
import SearchBar from "./components/SearchBar";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Can I Use: Parquet",
  description:
    "Parquet format compatibility matrix across libraries and query engines",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const data = getMatrixData();
  const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";
  const searchItems = buildSearchIndex(data, basePath);

  return (
    <html lang="en">
      <body className="bg-gray-950 text-gray-100 antialiased">
        <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur-sm sticky top-0 z-50">
          <div className="max-w-[1600px] mx-auto px-4 py-3 flex items-center gap-4">
            <Link href={`${basePath}/`} className="flex-shrink-0">
              <h1 className="text-xl font-bold">
                <span className="text-green-400">Can I Use:</span>{" "}
                <span className="text-white">Parquet</span>
              </h1>
            </Link>
            <div className="flex-1 max-w-md">
              <SearchBar items={searchItems} />
            </div>
            <div className="flex-shrink-0 text-xs text-yellow-400/80 bg-yellow-400/10 border border-yellow-400/20 rounded px-2 py-1">
              ⚠️ AI-generated — information may be inaccurate
            </div>
          </div>
        </header>
        {children}
      </body>
    </html>
  );
}
