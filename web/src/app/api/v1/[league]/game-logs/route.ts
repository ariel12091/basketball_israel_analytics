import { gameLogs } from "@/rules/gameLogs";
import { getSeason } from "@/server/gameLogsRepo";
import { rawQueryGuard, handleError } from "@/server/api";
import { leagueParam, parseParams } from "@/server/params";
export const runtime = "nodejs";
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league),
      p = parseParams(url.searchParams, league);
    const season = await getSeason(p),
      result = gameLogs(season, [], p);
    const schedule = season.schedule ?? season.rows;
    const teams = [
      ...new Map(
        schedule.map((row) => [
          Number(row.team_id),
          { id: Number(row.team_id), name: String(row.team_name) },
        ]),
      ).values(),
    ].sort((a, b) => (a.name < b.name ? -1 : 1));
    return Response.json(
      {
        ...result,
        meta: {
          ...result.meta,
          league,
          season: p.season,
          view: p.view,
          version: season.version,
          teams,
          rounds: [
            ...new Set(
              schedule
                .map((row) =>
                  Number(row[league === "il" ? "gn" : "round_number"]),
                )
                .filter(Number.isFinite),
            ),
          ].sort((a, b) => a - b),
          phases: [
            ...new Set(
              schedule.map((row) => String(row.phase ?? "")).filter(Boolean),
            ),
          ].sort(),
        },
      },
      {
        headers: {
          "Cache-Control": "public, max-age=0",
          "Vercel-CDN-Cache-Control": "s-maxage=60",
        },
      },
    );
  } catch (error) {
    return handleError(error);
  }
}
