// The chat panel's endpoint: one question in, status events then the verified
// answer out as a server-sent event stream (spec "Interfaces").
import { z } from "zod";
import { checkLimit } from "@/ask/limits";
import { logTurn } from "@/ask/log";
import { currentProvider } from "@/ask/provider";
import { TOOLS, type ToolName } from "@/ask/registry";
import { askTurn, type AskAnswer } from "@/ask/turn";
export const runtime = "nodejs";
export const maxDuration = 60;

const league = z.enum(["il", "euroleague", "eurocup"]);
const call = z.object({
  tool: z.enum(Object.keys(TOOLS) as [ToolName, ...ToolName[]]),
  league,
  query: z.record(z.string().max(64), z.union([z.string().max(500), z.array(z.string().max(500)).max(4)])),
  sort: z.object({ key: z.string().max(64), desc: z.boolean() }).optional(),
  limit: z.number().int().min(1).max(10).optional(),
});
const Body = z.object({
  league,
  question: z.string().trim().min(1).max(500),
  history: z
    .array(
      z.object({
        question: z.string().max(500),
        answer: z.string().max(4000),
        calls: z.array(call).max(4),
      }),
    )
    .max(6),
  clientId: z.string().min(1).max(64),
});
const MAX_TURNS = 6;

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  Response.json(body, { status, headers: { "Cache-Control": "no-store", ...headers } });

export async function POST(request: Request) {
  if (process.env.ASK_ENABLED !== "true") return json(404, { error: "Not found" });
  let body: z.infer<typeof Body>;
  try {
    body = Body.parse(await request.json());
  } catch {
    return json(400, { error: "Invalid question" });
  }
  if (body.history.length >= MAX_TURNS)
    return json(400, { error: "This conversation is full; start a new question" });
  const ip = (request.headers.get("x-forwarded-for") ?? "").split(",")[0].trim() || "unknown";
  const limit = checkLimit([`client:${body.clientId}`, `ip:${ip}`], Date.now());
  if (!limit.ok)
    return json(429, { error: "Too many questions; try again later", retryAfterSec: limit.retryAfterSec }, {
      "Retry-After": String(limit.retryAfterSec),
    });

  const provider = currentProvider();
  const started = Date.now();
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let answer: AskAnswer | null = null,
        error: string | undefined;
      try {
        for await (const e of askTurn(
          { league: body.league, question: body.question, history: body.history },
          provider,
          { now: Date.now, followups: process.env.ASK_FOLLOWUPS === "true" },
        )) {
          if (e.type === "final") answer = e.answer;
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(e)}\n\n`));
        }
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify({ type: "error", error: "The answer failed; try again" })}\n\n`),
        );
      } finally {
        logTurn({ question: body.question, provider: provider.name, model: provider.model, answer, ms: Date.now() - started, error });
        controller.close();
      }
    },
  });
  return new Response(stream, {
    headers: { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-store" },
  });
}
