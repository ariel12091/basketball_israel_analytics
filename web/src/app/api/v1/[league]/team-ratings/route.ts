import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
  answerShape,
} from "@/server/api";
import { leagueParam } from "@/server/params";
import { teamRatingsAnswer, teamRatingsPageAnswer } from "@/server/teamRatingsRepo";
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
      answerShape(search) === "table" ? teamRatingsPageAnswer : teamRatingsAnswer;
    return answerJson(await answer(league, search), url);
  } catch (e) {
    return handleError(e);
  }
}
