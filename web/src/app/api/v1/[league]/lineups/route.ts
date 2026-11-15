import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
  answerFormat,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { leagueParam } from "@/server/params";
import { lineupsPageAnswer } from "@/server/pageAnswers";
export const runtime = "nodejs";
export const maxDuration = 30;
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league),
      search = answerParams(url),
      format = answerFormat(search);
    const answer = await lineupsPageAnswer(league, search);
    return format === "csv"
      ? answerCsv(answer, url, "lineups", league)
      : answerJson(answer, url);
  } catch (e) {
    return handleError(e);
  }
}
