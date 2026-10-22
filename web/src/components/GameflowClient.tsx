"use client";
import { useRouter } from "next/navigation";
import Link from "next/link";
import type { League } from "@/rules/gameLogs";
import type { GameflowPayload } from "@/server/gameflowRepo";
import ui from "@/data/shinyUi.json";
import { defaultSeason } from "@/lib/tabQuery";
import { TabFrame, useTabData } from "./shiny/TabKit";
import { GameflowSurface, gameflowPath } from "./shiny/GameflowPanel";
export default function GameflowClient({
  league,
  game,
  team,
  initial,
}: {
  league: League;
  game: number;
  team: number;
  initial: { url: string; data: GameflowPayload | null };
}) {
  const router = useRouter(),
    state = useTabData<GameflowPayload>(
      gameflowPath(league, game, team),
      initial,
    ),
    season = state.data?.meta.season ?? defaultSeason(league),
    back = `/${league}/game-logs?season=${season}`;
  return (
    <TabFrame
      league={league}
      tab="game_logs"
      paneValue="game_logs"
      season={season}
      seasons={league === "il" ? ui.ilSeasons : ui.euroStaticSeasons}
      onSeason={(s) => router.push(`/${league}/game-logs?season=${s}`)}
      views={["Summary", "Four Factors"]}
      view="Summary"
      onView={(v) =>
        router.push(
          `${back}&view=${v === "Four Factors" ? "four_factors" : "summary"}`,
        )
      }
      updated={null}
      filtersId="gameflow-filters"
      sidebar={null}
      mainOnly
      main={
        <>
          <Link href={back}>Back to Game Logs</Link>
          <GameflowSurface
            {...state}
            standalone
            onClose={() => router.push(back)}
          />
        </>
      }
    />
  );
}
