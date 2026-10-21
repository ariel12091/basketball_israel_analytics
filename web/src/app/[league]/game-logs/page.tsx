import { Suspense } from "react";
import { notFound } from "next/navigation";
import { headers } from "next/headers";
import GameLogsClient from "@/components/GameLogsClient";
import type { League } from "@/rules/gameLogs";
import {
  gameLogsPath,
  queryFromSearch,
  type SearchParams,
} from "@/lib/tabQuery";
import { gameLogsAnswer } from "@/server/gameLogsApi";

// The first table is rendered on the server from the same answer the API
// gives; the client takes over for every later filter change. A failed read
// leaves the client to fetch (and show its error state) as before. The
// browser tests send x-ibpl-client-data to keep the data request in the
// browser, where they substitute the stored Shiny answers.
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
  const path = gameLogsPath(l, queryFromSearch(l, await searchParams));
  const clientOnly = (await headers()).get("x-ibpl-client-data") === "1";
  const data = clientOnly
    ? null
    : await gameLogsAnswer(l, new URL(path, "http://local").searchParams).catch(
        () => null,
      );
  return (
    <Suspense fallback={null}>
      <GameLogsClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
