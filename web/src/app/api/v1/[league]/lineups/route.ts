import {
  handleError,
  rawQueryGuard,
  answerJson,
  answerParams,
} from "@/server/api";
import { leagueParam } from "@/server/params";
import { lineupsPageAnswer } from "@/server/lineupsApi";
export const runtime = "nodejs";
export const maxDuration = 30;
export async function GET(
  request: Request,
  context: { params: Promise<{ league: string }> },
) {
  try {
    const url = new URL(request.url);
    rawQueryGuard(url);
    return answerJson(
      await lineupsPageAnswer(
        leagueParam((await context.params).league),
        answerParams(url),
      ),
      url,
    );
  } catch (e) {
    return handleError(e);
  }
}
