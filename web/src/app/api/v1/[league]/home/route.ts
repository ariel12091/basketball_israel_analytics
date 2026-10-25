import { handleError, rawQueryGuard, ParamError } from "@/server/api";
import { homeAnswer } from "@/server/homeRepo";
export const runtime = "nodejs";
export const maxDuration = 30;
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    if ((await context.params).league !== "il")
      throw new ParamError("league", "The team dashboard is Israeli only");
    const url = new URL(request.url);
    rawQueryGuard(url);
    const answer = await homeAnswer(url.searchParams);
    return Response.json(answer, {
      headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
      },
    });
  } catch (e) {
    return handleError(e);
  }
}
