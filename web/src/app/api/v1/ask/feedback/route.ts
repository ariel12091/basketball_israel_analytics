// "Wrong answer?" in the chat panel: a visitor who presses Send stores that
// one question, the chat's answer, its lookups and an optional note in
// app_feedback.ask_feedback (sql/migrations/2026-11-19_ask_feedback.sql),
// through the one function the app's role may write with. Nothing about the
// visitor is stored: clientId and the IP only feed the in-memory limit.
import { z } from "zod";
import { createLimiter } from "@/ask/limits";
import { currentProvider } from "@/ask/provider";
import { clientIp, sameSite } from "@/ask/requestGuards";
import { MAX_CALLS } from "@/ask/turn";
import { Deadline, withSnapshot } from "@/server/db";
export const runtime = "nodejs";

const Body = z.object({
  league: z.enum(["il", "euroleague", "eurocup"]),
  question: z.string().trim().min(1).max(500),
  answer: z.string().max(4000),
  // The answer's own lookups, as the panel holds them; stored as given.
  calls: z.array(z.record(z.string(), z.unknown())).max(MAX_CALLS),
  note: z.string().max(500).optional(),
  clientId: z.string().min(1).max(64),
});

// Five flags per browser and per address an hour; the database function
// refuses past 200 an hour whatever the instances let through.
const checkFeedbackLimit = createLimiter(() => 5, 3_600_000);

const json = (status: number, body: unknown) =>
  Response.json(body, { status, headers: { "Cache-Control": "no-store" } });

export async function POST(request: Request) {
  if (process.env.ASK_ENABLED !== "true") return json(404, { error: "Not found" });
  if (!sameSite(request)) return json(403, { error: "Forbidden" });
  let body: z.infer<typeof Body>;
  try {
    body = Body.parse(await request.json());
  } catch {
    return json(400, { error: "Invalid feedback" });
  }
  const calls = JSON.stringify(body.calls);
  if (calls.length > 8000) return json(400, { error: "Invalid feedback" });
  // The model the chat runs now (the answer was given by the same deployment).
  let model: string | null = null;
  try {
    model = currentProvider().model;
  } catch {}
  const limit = checkFeedbackLimit([`client:${body.clientId}`, `ip:${clientIp(request)}`], Date.now());
  if (!limit.ok) return json(429, { error: "Thanks, that's enough feedback for now" });
  try {
    const [row] = await withSnapshot(
      new Deadline(5000, 3000),
      (q) =>
        q<{ id: string | null }>(
          "SELECT app_feedback.submit_ask_feedback($1, $2, $3, $4::jsonb, $5, $6) AS id",
          [body.league, body.question, body.answer, calls, body.note ?? null, model],
        ),
      { write: true },
    );
    const stored = row?.id != null;
    console.log(JSON.stringify({ kind: "ask_feedback", stored }));
    return stored ? json(200, { ok: true }) : json(503, { error: "Feedback is full for now; try later" });
  } catch (e) {
    console.error(JSON.stringify({ kind: "ask_feedback", stored: false, error: e instanceof Error ? e.name : "UnknownError" }));
    return json(503, { error: "Could not send; try again" });
  }
}
