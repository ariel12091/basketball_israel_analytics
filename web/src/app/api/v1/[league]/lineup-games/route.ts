import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { lineupGames } from "@/server/lineupGamesRepo";
export const runtime = "nodejs";
export const maxDuration = 30;
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    return Response.json(
      await lineupGames(
        leagueParam((await context.params).league),
        url.searchParams,
      ),
      {
        headers: {
          "Cache-Control": "public, max-age=0",
          "Vercel-CDN-Cache-Control": "s-maxage=60",
        },
      },
    );
  } catch (e) {
    return handleError(e);
  }
}
