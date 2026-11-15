import {
  handleError,
  rawQueryGuard,
  ParamError,
  answerJson,
  answerParams,
  answerShape,
} from "@/server/api";
import { compareAnswer } from "@/server/compareRepo";
import { comparePageAnswer } from "@/server/comparePage";
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
    const search = answerParams(url);
    // shape=table: the page's table; otherwise one side's reader (kind=...).
    const answer =
      answerShape(search) === "table" ? comparePageAnswer : compareAnswer;
    return answerJson(await answer(search), url);
  } catch (e) {
    return handleError(e);
  }
}
