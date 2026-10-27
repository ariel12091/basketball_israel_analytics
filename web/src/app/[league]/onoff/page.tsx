import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import OnOffClient from "@/components/OnOffClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { FirstReadFailed, TAB_FIRST_PATH } from "@/lib/tabPages";
import { onoffAnswer } from "@/server/onoffApi";
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
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await onoffAnswer(l, new URL(path, "http://local").searchParams).catch(
          () => {
            throw new FirstReadFailed();
          },
        );
  return (
    <Suspense fallback={null}>
      <OnOffClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
