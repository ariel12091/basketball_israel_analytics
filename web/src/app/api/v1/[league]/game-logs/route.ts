import {
  rawQueryGuard,
  handleError,
  answerJson,
  answerParams,
  answerFormat,
  answerShape,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { leagueParam } from "@/server/params";
import { gameLogsAnswer } from "@/server/gameLogsApi";
import { logged } from "@/server/requestLog";
export const runtime = "nodejs";
async function get(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league);
    const search = answerParams(url),
      format = answerFormat(search);
    // Already the page's table: shape=table is a no-op, any other is a 400.
    answerShape(search);
    const answer = await gameLogsAnswer(league, search);
    return format === "csv"
      ? answerCsv(answer, url, "game_logs", league)
      : answerJson(answer, url);
  } catch (error) {
    return handleError(error);
  }
}

export const GET = logged("game-logs", get);
