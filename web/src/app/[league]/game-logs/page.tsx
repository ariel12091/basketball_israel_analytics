import { Suspense } from "react";
import { notFound } from "next/navigation";
import GameLogsClient from "@/components/GameLogsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { TAB_FIRST_PATH } from "@/lib/tabPages";
import { gameLogsAnswer } from "@/server/gameLogsApi";
import { pageMetadata } from "@/lib/pageTitle";
import { streamedTab } from "@/server/streamedTab";

export const generateMetadata = pageMetadata("game-logs");
// The first table is rendered on the server from the same answer the API
// gives; the client takes over for every later filter change. A failed read
// answers 500 and app/[league]/error.tsx shows the tab, whose client fetches
// (and shows its error state) as before. The browser tests send
// x-ibpl-client-data to keep the data request in the browser, where they
// substitute the stored Shiny answers.
export default async function GameLogsPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League;
  const path = TAB_FIRST_PATH["game-logs"](l, await searchParams);
  return streamedTab({
    page: "game-logs",
    league: l,
    read: () => gameLogsAnswer(l, new URL(path, "http://local").searchParams),
    render: (data) => (
      <Suspense fallback={null}>
        <GameLogsClient key={league} league={l} initial={{ url: path, data }} />
      </Suspense>
    ),
  });
}
