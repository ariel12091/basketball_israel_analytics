import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import CompareClient from "@/components/CompareClient";
import { queryFromSearch, type SearchParams } from "@/lib/tabQuery";
import {
  compareDefaults,
  compareSideParams,
  comparePath,
  resolveCompareQuery,
  comparePlayerTeams,
  withCompareSelection,
  type CompareSelection,
} from "@/lib/compareQuery";
import { compareAnswer } from "@/server/compareRepo";
import type { CompareSeed } from "@/components/shiny/CompareData";
import { compareDefaultPlayers } from "@/lib/compareAsync";
export default async function ComparePage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  if ((await params).league !== "il") notFound();
  let q = resolveCompareQuery(
    queryFromSearch("il", await searchParams, compareDefaults),
  );
  if (!["Teams", "Lineups", "Players"].includes(q.view)) notFound();
  const initial: CompareSeed = {};
  let selection: CompareSelection | undefined;
  if ((await headers()).get("x-ibpl-client-data") !== "1") {
    const refs = new URLSearchParams({ season: q.season, kind: "refs" });
    const data = await compareAnswer(refs).catch(() => null);
    if (data) initial[comparePath(refs)] = data;
    if (
      q.view === "Players" &&
      q.auto_players === "true" &&
      !q.player_a &&
      !q.player_b &&
      data?.players
    ) {
      const defaults = new URLSearchParams({
        season: q.season,
        kind: "player_defaults",
      });
      const scores = await compareAnswer(defaults).catch(() => null);
      if (scores) {
        initial[comparePath(defaults)] = scores;
        const ids = compareDefaultPlayers(
          scores.rows,
          data.players.map((r) => Number(r.player_id)),
        );
        selection = {
          season: q.season,
          a: String(ids[0] ?? ""),
          b: String(ids[1] ?? ids[0] ?? ""),
        };
        q = withCompareSelection(q, selection);
      }
    }
    if (
      q.view !== "Players" ||
      (q.player_a && (q.player_compare === "self" || q.player_b))
    ) {
      const ff = q.metric.startsWith("off_") && q.metric !== "off_ppp",
        kind =
          q.view === "Players" ||
          (q.view === "Teams" && q.cmp_view === "players")
            ? "players"
            : q.view === "Lineups"
              ? ff
                ? "lineup_ff"
                : "lineups"
              : ff
                ? "four_factors"
                : "ratings";
      await Promise.all(
        (["a", "b"] as const).map(async (side) => {
          const s = compareSideParams(q, side, kind);
          if (q.view === "Players") {
            const teams = comparePlayerTeams(q, side, data?.players ?? []);
            if (!teams) return;
            s.set("teams", teams);
          } else if (kind === "players" && q.entity) s.set("teams", q.entity);
          const answer = await compareAnswer(s).catch(() => null);
          if (answer) initial[comparePath(s)] = answer;
        }),
      );
    }
  }
  return (
    <Suspense fallback={null}>
      <CompareClient initial={initial} selection={selection} />
    </Suspense>
  );
}
