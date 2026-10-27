import { describe, expect, it, vi } from "vitest";
import { anthropicProvider } from "./anthropic";
import type { ProviderMessage, ToolSchema } from "./types";

const tools: ToolSchema[] = [
  { name: "lineups", description: "Lineups page", parameters: { type: "object", properties: {} } },
];
const usage = { input_tokens: 1_000_000, output_tokens: 100_000, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 };
function stub(content: unknown[], stop_reason = "end_turn") {
  const create = vi.fn(async (...args: unknown[]) => (void args, { content, stop_reason, usage }));
  return { create, client: { messages: { create } } as never };
}

describe("anthropicProvider", () => {
  it("maps tool_use blocks to toolCalls and keeps the raw assistant content", async () => {
    const raw = [
      { type: "thinking", thinking: "", signature: "s" },
      { type: "tool_use", id: "tu1", name: "lineups", input: { league: "il", query: { team: "2" }, sort: { key: "net_rtg", desc: true } } },
    ];
    const { client } = stub(raw, "tool_use");
    const r = await anthropicProvider("claude-haiku-5-5", client).reply("sys", tools, [{ role: "user", text: "q" }], new AbortController().signal);
    expect(r.toolCalls).toEqual([
      { id: "tu1", call: { tool: "lineups", league: "il", query: { team: "2" }, sort: { key: "net_rtg", desc: true } } },
    ]);
    expect(r.raw).toBe(raw);
  });

  it("sends tool results back as tool_result blocks in one user message, and raw assistant turns unchanged", async () => {
    const { create, client } = stub([{ type: "text", text: "done" }]);
    const raw = [{ type: "tool_use", id: "tu1", name: "lineups", input: {} }];
    const messages: ProviderMessage[] = [
      { role: "user", text: "q" },
      { role: "assistant", toolCalls: [], raw },
      { role: "user", toolResults: [{ id: "tu1", content: { id: "r1", rows: [] } }, { id: "tu2", content: { error: "x" } }] },
    ];
    await anthropicProvider("claude-haiku-5-5", client).reply("sys", tools, messages, new AbortController().signal);
    const body = create.mock.calls[0][0] as { messages: { role: string; content: unknown }[]; tools: unknown[]; system: string; cache_control: unknown };
    expect(body.messages[1]).toEqual({ role: "assistant", content: raw });
    expect(body.messages[2]).toEqual({
      role: "user",
      content: [
        { type: "tool_result", tool_use_id: "tu1", content: JSON.stringify({ id: "r1", rows: [] }) },
        { type: "tool_result", tool_use_id: "tu2", content: JSON.stringify({ error: "x" }) },
      ],
    });
    expect(body.tools).toEqual([{ name: "lineups", description: "Lineups page", input_schema: tools[0].parameters }]);
    expect(body).toMatchObject({ system: "sys", cache_control: { type: "ephemeral" } });
  });

  it("rebuilds tool_use blocks for re-run history calls (no raw)", async () => {
    const { create, client } = stub([{ type: "text", text: "ok" }]);
    const call = { tool: "lineups" as const, league: "il" as const, query: { team: "2" } };
    await anthropicProvider("claude-haiku-5-5", client).reply(
      "sys",
      tools,
      [{ role: "user", text: "q" }, { role: "assistant", toolCalls: [{ id: "r1", call }] }, { role: "user", toolResults: [{ id: "r1", content: {} }] }],
      new AbortController().signal,
    );
    const body = create.mock.calls[0][0] as { messages: { content: unknown }[] };
    expect(body.messages[1].content).toEqual([
      { type: "tool_use", id: "r1", name: "lineups", input: { league: "il", query: { team: "2" } } },
    ]);
  });

  it("computes cost from usage and the model's prices", async () => {
    const { client } = stub([{ type: "text", text: "hi" }]);
    const r = await anthropicProvider("claude-haiku-5-5", client).reply("s", tools, [{ role: "user", text: "q" }], new AbortController().signal);
    // Haiku 5.5: $0.10 per million input, $0.50 per million output.
    expect(r.usage).toEqual({ inputTokens: 1_000_000, outputTokens: 100_000, costUsd: 0.15 });
    expect(r.text).toBe("hi");
  });

  it("refusal becomes refused", async () => {
    const { client } = stub([], "refusal");
    const r = await anthropicProvider("claude-haiku-5-5", client).reply("s", tools, [{ role: "user", text: "q" }], new AbortController().signal);
    expect(r.refused).toBe(true);
  });
});
