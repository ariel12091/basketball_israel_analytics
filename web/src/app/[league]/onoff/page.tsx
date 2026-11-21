import { Suspense } from "react";
import { notFound } from "next/navigation";
import OnOffClient from "@/components/OnOffClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { TAB_FIRST_PATH } from "@/lib/tabPages";
import { onoffAnswer } from "@/server/onoffApi";
import { pageMetadata } from "@/lib/pageTitle";
import { streamedTab } from "@/server/streamedTab";

export const generateMetadata = pageMetadata("onoff");
// First table rendered on the server from the same answer the API gives (see
// the Game Logs page for the failed read and the x-ibpl-client-data header).
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
  const path = TAB_FIRST_PATH["onoff"](l, await searchParams);
  return streamedTab({
    page: "onoff",
    league: l,
    read: () => onoffAnswer(l, new URL(path, "http://local").searchParams),
    render: (data) => (
      <Suspense fallback={null}>
        <OnOffClient key={league} league={l} initial={{ url: path, data }} />
      </Suspense>
    ),
  });
}
