import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { parseContext } from "@/server/contextParams";
import { playerStatFrame } from "@/server/playerStatFilterRepo";
export const runtime = "nodejs";
export const maxDuration = 30;

/** GET /api/v1/{league}/player-stat-filters?<game context>
 * The per-game Player Stats of every team-player for the On/Off Player Stats
 * chips (inventory ON-12); read only while such a chip is active. */
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league);
    const rows = await playerStatFrame(parseContext(url.searchParams, league));
    return Response.json(
      { rows, meta: { version: "" } },
      {
        headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
        },
      },
    );
  } catch (error) {
    return handleError(error);
  }
}
