import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import LineupsClient from "@/components/LineupsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { FirstReadFailed, TAB_FIRST_PATH } from "@/lib/tabPages";
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
  const path = TAB_FIRST_PATH["lineups"](l, await searchParams);
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await lineupsPageAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => {
          throw new FirstReadFailed();
        });
  return (
    <Suspense fallback={null}>
      <LineupsClient key={league} league={l} initial={{ url: path, data }} />
    </Suspense>
  );
}
