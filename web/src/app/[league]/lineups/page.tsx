import { Suspense } from "react";
import { notFound } from "next/navigation";
import LineupsClient from "@/components/LineupsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { TAB_FIRST_PATH } from "@/lib/tabPages";
import { lineupsPageAnswer } from "@/server/pageAnswers";
import { pageMetadata } from "@/lib/pageTitle";
import { streamedTab } from "@/server/streamedTab";

export const generateMetadata = pageMetadata("lineups");
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
  return streamedTab({
    page: "lineups",
    league: l,
    read: () => lineupsPageAnswer(l, new URL(path, "http://local").searchParams),
    render: (data) => (
      <Suspense fallback={null}>
        <LineupsClient key={league} league={l} initial={{ url: path, data }} />
      </Suspense>
    ),
  });
}
