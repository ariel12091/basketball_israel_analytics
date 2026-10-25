import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { teamRatingsAnswer } from "@/server/teamRatingsRepo";
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
      await teamRatingsAnswer(
        leagueParam((await context.params).league),
        url.searchParams,
      ),
      {
        headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
        },
      },
    );
  } catch (e) {
    return handleError(e);
  }
}
