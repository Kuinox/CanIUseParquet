import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Can I Use: Parquet",
  description: "Parquet format compatibility matrix across libraries and query engines",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="bg-gray-950 text-gray-100 antialiased">
        {children}
      </body>
    </html>
  );
}
