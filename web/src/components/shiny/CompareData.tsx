"use client";
import { useQuery } from "@tanstack/react-query";
import { comparePath } from "@/lib/compareQuery";
import { noteVersion, withVersion } from "@/lib/dataVersion";
import type { ComparePayload } from "@/server/compareRepo";
export type CompareSeed = Record<string, ComparePayload>;
export function useCompareData(
  params: URLSearchParams,
  seed: CompareSeed,
  enabled = true,
) {
  const url = comparePath(params);
  return useQuery({
    queryKey: ["compare", url],
    enabled,
    initialData: seed[url],
    staleTime: 60000,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const response = await fetch(withVersion(url), { signal });
      const data = await response.json();
      noteVersion("il", data.meta?.version);
      if (!response.ok)
        throw new Error(data.error ?? "Unable to load comparison");
      if (data.meta?.key !== params.toString())
        throw new Error("Comparison response did not match its filters");
      return data as ComparePayload;
    },
  });
}
export function CompareStatus({
  label,
  query,
}: {
  label: string;
  query: ReturnType<typeof useCompareData>;
}) {
  return query.isError ? (
    <div role="alert" className="shiny-output-error">
      {label}: {query.error.message}{" "}
      <button
        type="button"
        className="btn btn-default btn-sm"
        onClick={() => void query.refetch()}
      >
        Retry {label}
      </button>
    </div>
  ) : query.isFetching ? (
    <p role="status">Loading {label}…</p>
  ) : null;
}
