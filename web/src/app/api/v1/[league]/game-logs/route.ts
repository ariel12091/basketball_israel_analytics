import {
  rawQueryGuard,
  handleError,
  answerJson,
  answerParams,
} from "@/server/api";
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
    return answerJson(await gameLogsAnswer(league, answerParams(url)), url);
  } catch (error) {
    return handleError(error);
  }
}
