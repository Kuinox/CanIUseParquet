import { notFound } from "next/navigation";
import Link from "next/link";
import { getMatrixData } from "../../lib/server";
import {
  SLUG_TO_CATEGORY,
  CATEGORY_LABELS,
  featureToSlug,
  slugToFeature,
  getCategoryFeatures,
  InternalCategory,
} from "../../lib/data";
import FeaturePageContent from "../../components/FeaturePageContent";

export async function generateStaticParams() {
  const data = getMatrixData();
  const params: { category: string; feature: string }[] = [];

  // Encoding: feature = encoding type slug
  for (const enc of data.categories.encoding) {
    params.push({ category: "encoding", feature: featureToSlug(enc) });
  }

  // Other categories
  const otherCategories: { slug: string; cat: InternalCategory }[] = [
    { slug: "compression", cat: "compression" },
    { slug: "logical-types", cat: "logical_types" },
    { slug: "nested-types", cat: "nested_types" },
    { slug: "advanced-features", cat: "advanced_features" },
  ];

  for (const { slug, cat } of otherCategories) {
    const features = getCategoryFeatures(data, cat);
    for (const feature of features) {
      params.push({ category: slug, feature: featureToSlug(feature) });
    }
  }

  return params;
}

interface Props {
  params: Promise<{ category: string; feature: string }>;
}

export default async function FeaturePage({ params }: Props) {
  const { category: categorySlug, feature: featureSlug } = await params;
  const internalCategory = SLUG_TO_CATEGORY[categorySlug];
  if (!internalCategory) notFound();

  const data = getMatrixData();
  const categoryLabel = CATEGORY_LABELS[internalCategory];

  const features = getCategoryFeatures(data, internalCategory);
  const feature = slugToFeature(featureSlug, features);
  if (!feature) notFound();

  const toolIds = Object.keys(data.tools);
  const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

  const serializedData = {
    tools: data.tools,
    categories: data.categories,
  };

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
          <Link
            href={`${basePath}/${categorySlug}`}
            className="hover:text-green-400 transition-colors"
          >
            {categoryLabel}
          </Link>
          <span>›</span>
          <span className="text-gray-200 font-mono">{feature}</span>
        </nav>

        <div className="flex items-center gap-4 mb-6">
          <Link
            href={`${basePath}/${categorySlug}`}
            className="text-sm text-gray-400 hover:text-green-400 transition-colors flex items-center gap-1"
          >
            ← Back to {categoryLabel}
          </Link>
        </div>

        <h1 className="text-3xl font-bold text-white mb-2 font-mono">
          {feature}
        </h1>
        <p className="text-gray-400 mb-8 text-sm">{categoryLabel}</p>

        <FeaturePageContent
          feature={feature}
          category={internalCategory}
          toolIds={toolIds}
          data={serializedData}
        />
      </div>
    </main>
  );
}
