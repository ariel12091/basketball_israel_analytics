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
        "Cache-Control": answer.meta.errors.length
          ? "no-store"
          : "public, max-age=0",
        "Vercel-CDN-Cache-Control": answer.meta.errors.length
          ? "no-store"
          : "s-maxage=60",
      },
    });
  } catch (e) {
    return handleError(e);
  }
}
