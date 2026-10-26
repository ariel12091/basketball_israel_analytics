import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import LineupsClient from "@/components/LineupsClient";
import type { League } from "@/rules/gameLogs";
import {
  LINEUP_QUERY_DEFAULTS,
  lineupsPagePath,
  queryFromSearch,
  type SearchParams,
} from "@/lib/tabQuery";
import { lineupsPageAnswer } from "@/server/lineupsApi";
export default async function LineupsPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League;
  // The client starts with auto on, so the first table uses the auto minimum.
  const path = lineupsPagePath(
    l,
    queryFromSearch(l, await searchParams, LINEUP_QUERY_DEFAULTS),
    true,
  );
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await lineupsPageAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => null);
  return (
    <Suspense fallback={null}>
      <LineupsClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
