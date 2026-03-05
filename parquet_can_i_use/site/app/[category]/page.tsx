import { notFound } from "next/navigation";
import Link from "next/link";
import { getMatrixData } from "../lib/server";
import {
  SLUG_TO_CATEGORY,
  CATEGORY_LABELS,
  CATEGORY_DESCRIPTIONS,
} from "../lib/data";
import CategoryMatrix from "../components/CategoryMatrix";

export async function generateStaticParams() {
  return [
    { category: "compression" },
    { category: "encoding" },
    { category: "logical-types" },
    { category: "nested-types" },
    { category: "advanced-features" },
  ];
}

interface Props {
  params: Promise<{ category: string }>;
}

export default async function CategoryPage({ params }: Props) {
  const { category: categorySlug } = await params;
  const internalCategory = SLUG_TO_CATEGORY[categorySlug];
  if (!internalCategory) notFound();

  const data = getMatrixData();
  const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";
  const label = CATEGORY_LABELS[internalCategory];
  const description = CATEGORY_DESCRIPTIONS[internalCategory];

  return (
    <main className="min-h-screen">
      <div className="max-w-[1600px] mx-auto px-4 py-6">
        <nav className="flex items-center gap-2 text-sm text-gray-400 mb-6">
          <Link
            href={`${basePath}/`}
            className="hover:text-green-400 transition-colors"
          >
            Home
          </Link>
          <span>›</span>
          <span className="text-gray-200">{label}</span>
        </nav>

        <h1 className="text-3xl font-bold text-white mb-2">{label}</h1>
        <p className="text-gray-400 mb-8">{description}</p>

        <CategoryMatrix
          data={data}
          category={internalCategory}
          categorySlug={categorySlug}
          basePath={basePath}
        />
      </div>
    </main>
  );
}
