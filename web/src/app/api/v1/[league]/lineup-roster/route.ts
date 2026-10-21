import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { parseLineups } from "@/server/lineupParams";
import { getLineupRoster } from "@/server/lineupsRepo";
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
      await getLineupRoster(
        parseLineups(
          url.searchParams,
          leagueParam((await context.params).league),
        ),
      ),
      { headers: { "Vercel-CDN-Cache-Control": "s-maxage=60" } },
    );
  } catch (e) {
    return handleError(e);
  }
}
