import { rawQueryGuard, handleError } from "@/server/api";
import { leagueParam } from "@/server/params";
import { gameLogsAnswer } from "@/server/gameLogsApi";
export const runtime = "nodejs";
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league);
    return Response.json(await gameLogsAnswer(league, url.searchParams), {
      headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return handleError(error);
  }
}
