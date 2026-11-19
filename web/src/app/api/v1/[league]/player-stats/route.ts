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
import { playerStatsAnswer } from "@/server/playerStatsRepo";
import { playerStatsPageAnswer } from "@/server/pageAnswers";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";
export const maxDuration = 30;
async function get(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league),
      search = answerParams(url);
    // shape=table: the page's finished table; otherwise the page's population.
    // shape=table (or format=csv): the page's finished table; otherwise the
    // page's population.
    const format = answerFormat(search),
      table = answerShape(search) === "table" || format === "csv";
    const answer = await (table ? playerStatsPageAnswer : playerStatsAnswer)(league, search);
    return format === "csv"
      ? answerCsv(answer, url, "player_stats", league)
      : answerJson(answer, url);
  } catch (e) {
    return handleError(e);
  }
}

export const GET = logged("player-stats", get);
