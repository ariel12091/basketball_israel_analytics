import { handleError, rawQueryGuard, ParamError } from "@/server/api";
import { leagueParam } from "@/server/params";
import { parseContext } from "@/server/contextParams";
import { getOnOff } from "@/server/onoffRepo";
import type { View } from "@/rules/onoff";
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
    const view = (url.searchParams.get("view") ?? "summary") as View;
    if (
      !["summary", "four_factors", "shot_profile"].includes(view) ||
      (view === "shot_profile" && league !== "il")
    )
      throw new ParamError("view", "Invalid view");
    const c = parseContext(url.searchParams, league);
    const answer = await getOnOff(c, view);
    return Response.json(
      {
        rows: answer.rows,
        league3p: answer.league3p,
        meta: {
          league,
          season: c.season,
          view,
          filtered: answer.filtered,
          version: answer.version,
          teams: answer.teams,
          rounds: answer.rounds,
          phases: answer.phases,
          ...(answer.seasons ? { seasons: answer.seasons } : {}),
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
