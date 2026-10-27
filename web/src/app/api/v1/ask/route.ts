// The chat panel's endpoint: one question in, status events then the verified
// answer out as a server-sent event stream (spec "Interfaces").
import { z } from "zod";
import { checkLimit } from "@/ask/limits";
import { logTurn } from "@/ask/log";
import { currentProvider } from "@/ask/provider";
import { TOOLS, type ToolName } from "@/ask/registry";
import { MAX_CALLS, askTurn, type AskAnswer } from "@/ask/turn";
export const runtime = "nodejs";
export const maxDuration = 60;

const league = z.enum(["il", "euroleague", "eurocup"]);
const call = z.object({
  tool: z.enum(Object.keys(TOOLS) as [ToolName, ...ToolName[]]),
  league,
  query: z.record(
    z.string().max(64),
    z.union([z.string().max(500), z.array(z.string().max(500)).max(4)]),
  ),
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
        calls: z.array(call).max(MAX_CALLS),
      }),
    )
    .max(6),
  clientId: z.string().min(1).max(64),
});
const MAX_TURNS = 6;

const json = (
  status: number,
  body: unknown,
  headers: Record<string, string> = {},
) =>
  Response.json(body, {
    status,
    headers: { "Cache-Control": "no-store", ...headers },
  });

export async function POST(request: Request) {
  if (process.env.ASK_ENABLED !== "true")
    return json(404, { error: "Not found" });
  // Browsers send Origin on every POST: refuse questions another site's page
  // sends on a visitor's behalf (they would spend this site's tokens).
  const origin = request.headers.get("origin");
  const host =
    request.headers.get("x-forwarded-host") ??
    request.headers.get("host") ??
    new URL(request.url).host;
  let sameSite = false;
  try {
    sameSite = !origin || new URL(origin).host === host;
  } catch {}
  if (!sameSite) return json(403, { error: "Forbidden" });
  let body: z.infer<typeof Body>;
  try {
    body = Body.parse(await request.json());
  } catch {
    return json(400, { error: "Invalid question" });
  }
  if (body.history.length >= MAX_TURNS)
    return json(400, {
      error: "This conversation is full; start a new question",
    });
  const ip =
    (request.headers.get("x-forwarded-for") ?? "").split(",")[0].trim() ||
    "unknown";
  const limit = checkLimit([`client:${body.clientId}`, `ip:${ip}`], Date.now());
  if (!limit.ok)
    return json(
      429,
      {
        error: "Too many questions; try again later",
        retryAfterSec: limit.retryAfterSec,
      },
      {
        "Retry-After": String(limit.retryAfterSec),
      },
    );

  const provider = currentProvider();
  const started = Date.now();
  const encoder = new TextEncoder();
  const abort = new AbortController();
  let ended = false;
  let output: ReadableStreamDefaultController<Uint8Array>;
  const disconnected = () => {
    abort.abort(request.signal.reason);
    if (!ended) {
      ended = true;
      output.close();
    }
  };
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      output = controller;
      request.signal.addEventListener("abort", disconnected, { once: true });
      if (request.signal.aborted) disconnected();
      // Keep start synchronous so consumer cancellation can abort pending work.
      void (async () => {
        let answer: AskAnswer | null = null,
          error: string | undefined;
        try {
          for await (const e of askTurn(
            {
              league: body.league,
              question: body.question,
              history: body.history,
            },
            provider,
            {
              now: Date.now,
              followups: process.env.ASK_FOLLOWUPS === "true",
              signal: abort.signal,
            },
          )) {
            if (ended) break;
            if (e.type === "final") answer = e.answer;
            // The final event names the model, so an eval run records what it measured.
            const event =
              e.type === "final"
                ? { ...e, provider: provider.name, model: provider.model }
                : e;
            controller.enqueue(
              encoder.encode(`data: ${JSON.stringify(event)}\n\n`),
            );
          }
        } catch (e) {
          error = e instanceof Error ? e.message : String(e);
          if (!ended)
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ type: "error", error: "The answer failed; try again" })}\n\n`,
              ),
            );
        } finally {
          request.signal.removeEventListener("abort", disconnected);
          if (!ended) {
            ended = true;
            controller.close();
          }
          logTurn({
            question: body.question,
            provider: provider.name,
            model: provider.model,
            answer,
            ms: Date.now() - started,
            error,
          });
        }
      })();
    },
    cancel(reason) {
      ended = true;
      request.signal.removeEventListener("abort", disconnected);
      abort.abort(reason);
    },
  });
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}
