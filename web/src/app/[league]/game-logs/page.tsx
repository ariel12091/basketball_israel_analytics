import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import GameLogsClient from "@/components/GameLogsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { FirstReadFailed, TAB_FIRST_PATH } from "@/lib/tabPages";
import { gameLogsAnswer } from "@/server/gameLogsApi";
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
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await gameLogsAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => {
          throw new FirstReadFailed();
        });
  return (
    <Suspense fallback={null}>
      <GameLogsClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
