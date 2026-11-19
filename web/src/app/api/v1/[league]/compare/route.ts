import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
  answerFormat,
  answerShape,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { compareAnswer } from "@/server/compareRepo";
import { leagueParam } from "@/server/params";
import { comparePageAnswer } from "@/server/comparePage";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";
export const maxDuration = 30;
async function get(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const league = leagueParam((await context.params).league);
    const url = new URL(request.url);
    rawQueryGuard(url);
    const search = answerParams(url);
    // shape=table: the page's table; otherwise one side's reader (kind=...).
    const format = answerFormat(search),
      table = answerShape(search) === "table" || format === "csv";
    const answer = await (table ? comparePageAnswer : compareAnswer)(search, league);
    return format === "csv"
      ? answerCsv(answer, url, "compare", league)
      : answerJson(answer, url);
  } catch (e) {
    return handleError(e);
  }
}

export const GET = logged("compare", get);
