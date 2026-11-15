import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
  answerShape,
} from "@/server/api";
import { leagueParam } from "@/server/params";
import { playerStatsAnswer } from "@/server/playerStatsRepo";
import { playerStatsPageAnswer } from "@/server/pageAnswers";
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
      search = answerParams(url);
    // shape=table: the page's finished table; otherwise the page's population.
    const answer =
      answerShape(search) === "table" ? playerStatsPageAnswer : playerStatsAnswer;
    return answerJson(await answer(league, search), url);
  } catch (e) {
    return handleError(e);
  }
}
