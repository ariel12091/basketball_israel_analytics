import { handleError, rawQueryGuard, ParamError } from "@/server/api";
import { compareAnswer } from "@/server/compareRepo";
export const runtime = "nodejs";
export const maxDuration = 30;
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    if ((await context.params).league !== "il")
      throw new ParamError(
        "league",
        "Compare is available for the Israeli League",
      );
    const url = new URL(request.url);
    rawQueryGuard(url);
    return Response.json(await compareAnswer(url.searchParams), {
      headers: {
        "Cache-Control": "no-store",
        "Vercel-CDN-Cache-Control": "no-store",
      },
    });
  } catch (e) {
    return handleError(e);
  }
}
