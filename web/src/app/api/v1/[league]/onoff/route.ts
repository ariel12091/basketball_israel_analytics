import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
  answerFormat,
  answerShape,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { leagueParam } from "@/server/params";
import { onoffAnswer } from "@/server/onoffApi";
import { onoffPageAnswer } from "@/server/pageAnswers";
export const runtime = "nodejs";
export const maxDuration = 30;

/** GET /api/v1/{il|euroleague|eurocup}/onoff?view=summary|four_factors|shot_profile
 * The ranked population of one view for the game context (inventory ON-01..ON-11):
 * ranks are computed before any team or possession narrowing, which the page
 * applies (helpers.R onoff_filter_*_rows); shape=table applies it here. */
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league),
      search = answerParams(url);
    // shape=table (or format=csv): the page's finished table; otherwise the
    // page's population.
    const format = answerFormat(search),
      table = answerShape(search) === "table" || format === "csv";
    const answer = await (table ? onoffPageAnswer : onoffAnswer)(league, search);
    return format === "csv"
      ? answerCsv(answer, url, "onoff", league)
      : answerJson(answer, url);
  } catch (error) {
    return handleError(error);
  }
}
