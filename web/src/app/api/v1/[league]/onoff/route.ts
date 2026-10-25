import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { onoffAnswer } from "@/server/onoffApi";
export const runtime = "nodejs";
export const maxDuration = 30;

/** GET /api/v1/{il|euroleague|eurocup}/onoff?view=summary|four_factors|shot_profile
 * The ranked population of one view for the game context (inventory ON-01..ON-11):
 * ranks are computed before any team or possession narrowing, which the page
 * applies (helpers.R onoff_filter_*_rows). */
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league);
    return Response.json(await onoffAnswer(league, url.searchParams), {
      headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return handleError(error);
  }
}
