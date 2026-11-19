import { handleError, rawQueryGuard } from "@/server/api";
import { leagueParam } from "@/server/params";
import { guideAnswer } from "@/server/guideApi";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";
export const maxDuration = 30;
async function get(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    return Response.json(
      await guideAnswer(
        leagueParam((await context.params).league),
        url.searchParams,
      ),
      { headers: { "Vercel-CDN-Cache-Control": "s-maxage=60" } },
    );
  } catch (e) {
    return handleError(e);
  }
}

export const GET = logged("guide", get);
