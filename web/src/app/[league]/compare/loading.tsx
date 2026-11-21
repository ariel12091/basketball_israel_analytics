import TabSkeleton from "@/components/shiny/TabSkeleton";

// The two sides load in the browser after the page arrives; this covers the
// server read of the season catalogue and the first comparison.
export default function CompareLoading() {
  return <TabSkeleton message="Loading comparison…" />;
}
