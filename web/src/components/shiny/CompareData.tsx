"use client";
import { createContext, useContext } from "react";
import { useQuery } from "@tanstack/react-query";
import type { League } from "@/rules/gameLogs";
import { comparePath } from "@/lib/compareQuery";
import { noteVersion, withVersion } from "@/lib/dataVersion";
import type { ComparePayload } from "@/server/compareRepo";
export type CompareSeed = Record<string, ComparePayload>;
/** The league every Compare read on the page is for (CompareClient). */
export const CompareLeagueContext = createContext<League>("il");
export function useCompareData(
  params: URLSearchParams,
  seed: CompareSeed,
  enabled = true,
) {
  const league = useContext(CompareLeagueContext);
  const url = comparePath(params, league);
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
      noteVersion(league, data.meta?.version);
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
