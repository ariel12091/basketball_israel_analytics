import { Suspense } from "react";
import { notFound } from "next/navigation";
import { headers } from "next/headers";
import OnOffClient from "@/components/OnOffClient";
import type { League } from "@/rules/gameLogs";
import { onoffPath, queryFromSearch, type SearchParams } from "@/lib/tabQuery";
import { onoffAnswer } from "@/server/onoffApi";
import ui from "@/data/shinyUi.json";

// First table rendered on the server from the same answer the API gives (see
// the Game Logs page for the x-ibpl-client-data test header).
export default async function OnOffPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League;
  const d = l === "il" ? ui.onoffDefaults.israel : ui.onoffDefaults.euroleague;
  const path = onoffPath(
    l,
    queryFromSearch(l, await searchParams, {
      min_all: String(d.minAll),
      min_on: String(d.minOn),
    }),
  );
  const clientOnly = (await headers()).get("x-ibpl-client-data") === "1";
  const data = clientOnly
    ? null
    : await onoffAnswer(l, new URL(path, "http://local").searchParams).catch(
        () => null,
      );
  return (
    <Suspense fallback={null}>
      <OnOffClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
