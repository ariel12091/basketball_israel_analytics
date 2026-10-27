import {
  handleError,
  rawQueryGuard,
  ParamError,
  answerJson,
  answerParams,
} from "@/server/api";
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
    const answer = await homeAnswer(answerParams(url));
    return answerJson(answer, url);
  } catch (e) {
    return handleError(e);
  }
}
