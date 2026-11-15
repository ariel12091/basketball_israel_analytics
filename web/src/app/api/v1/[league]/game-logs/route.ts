import {
  rawQueryGuard,
  handleError,
  answerJson,
  answerParams,
  answerFormat,
} from "@/server/api";
import { answerCsv } from "@/server/csvAnswer";
import { leagueParam } from "@/server/params";
import { gameLogsAnswer } from "@/server/gameLogsApi";
export const runtime = "nodejs";
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    const league = leagueParam((await context.params).league);
    const search = answerParams(url),
      format = answerFormat(search);
    const answer = await gameLogsAnswer(league, search);
    return format === "csv"
      ? answerCsv(answer, url, "game_logs", league)
      : answerJson(answer, url);
  } catch (error) {
    return handleError(error);
  }
}
