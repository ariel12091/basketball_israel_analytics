import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
} from "@/server/api";
import { leagueParam } from "@/server/params";
import { gameflowAnswer } from "@/server/gameflowRepo";
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
    return answerJson(
      await gameflowAnswer(
        leagueParam((await context.params).league),
        answerParams(url),
      ),
      url,
    );
  } catch (e) {
    return handleError(e);
  }
}

export const GET = logged("gameflow", get);
