import {
  handleError,
  rawQueryGuard,
  ParamError,
  answerJson,
  answerParams,
  answerFormat,
  answerShape,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { compareAnswer } from "@/server/compareRepo";
import { comparePageAnswer } from "@/server/comparePage";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";
export const maxDuration = 30;
async function get(
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
    const format = answerFormat(search),
      table = answerShape(search) === "table" || format === "csv";
    const answer = await (table ? comparePageAnswer : compareAnswer)(search);
    return format === "csv"
      ? answerCsv(answer, url, "compare", "il")
      : answerJson(answer, url);
  } catch (e) {
    return handleError(e);
  }
}

export const GET = logged("compare", get);
