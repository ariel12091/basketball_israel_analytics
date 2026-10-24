import "server-only";
// Claude behind the ask loop: one messages.create per model step (manual
// loop), the app's tools as client tools, assistant turns sent back unchanged.
import Anthropic from "@anthropic-ai/sdk";
import type { ToolName } from "../registry";
import type { ToolCall } from "../run";
import type { Provider, ProviderMessage, Usage } from "./types";

/** Dollars per million input / output tokens (cache write 1.25x, read 0.1x input). */
const PRICES: Record<string, [number, number]> = {
  "claude-haiku-5-5": [0.1, 0.5],
  "claude-sonnet-5-5": [2, 10],
  "claude-opus-5-5": [4, 20],
};

type Client = Pick<Anthropic, "messages">;

function cost(model: string, u: Anthropic.Usage): Usage {
  const [inp, out] = PRICES[model] ?? [0, 0];
  const read = u.cache_read_input_tokens ?? 0,
    write = u.cache_creation_input_tokens ?? 0;
  const costUsd =
    (u.input_tokens * inp + write * inp * 1.25 + read * inp * 0.1 + u.output_tokens * out) / 1e6;
  return {
    inputTokens: u.input_tokens + read + write,
    outputTokens: u.output_tokens,
    costUsd: Math.round(costUsd * 1e6) / 1e6,
  };
}

const input = (c: ToolCall) => ({
  league: c.league,
  query: c.query,
  ...(c.sort ? { sort: c.sort } : {}),
  ...(c.limit ? { limit: c.limit } : {}),
});

function toParam(m: ProviderMessage): Anthropic.MessageParam {
  if (m.role === "user")
    return m.toolResults
      ? {
          role: "user",
          content: m.toolResults.map((r) => ({
            type: "tool_result" as const,
            tool_use_id: r.id,
            content: JSON.stringify(r.content),
          })),
        }
      : { role: "user", content: m.text ?? "" };
  if (m.raw) return { role: "assistant", content: m.raw as Anthropic.ContentBlockParam[] };
  if (m.toolCalls?.length)
    return {
      role: "assistant",
      content: [
        ...(m.text ? [{ type: "text" as const, text: m.text }] : []),
        ...m.toolCalls.map((tc) => ({
          type: "tool_use" as const,
          id: tc.id,
          name: tc.call.tool,
          input: input(tc.call),
        })),
      ],
    };
  return { role: "assistant", content: m.text ?? "" };
}

export function anthropicProvider(model: string, client: Client = new Anthropic()): Provider {
  return {
    name: "anthropic",
    model,
    async reply(system, tools, messages, signal) {
      const res = await client.messages.create(
        {
          model,
          max_tokens: 4000,
          system,
          cache_control: { type: "ephemeral" },
          output_config: { effort: "low" },
          tools: tools.map((t) => ({
            name: t.name,
            description: t.description,
            input_schema: t.parameters as Anthropic.Tool.InputSchema,
          })),
          messages: messages.map(toParam),
        },
        { signal },
      );
      const usage = cost(model, res.usage);
      if (res.stop_reason === "refusal") return { text: null, toolCalls: [], usage, refused: true };
      const toolCalls = res.content.flatMap((b) =>
        b.type === "tool_use"
          ? [{ id: b.id, call: { tool: b.name as ToolName, ...(b.input as Omit<ToolCall, "tool">) } }]
          : [],
      );
      const text = res.content.flatMap((b) => (b.type === "text" ? [b.text] : [])).join("");
      return { text: text || null, toolCalls, usage, raw: res.content };
    },
  };
}
