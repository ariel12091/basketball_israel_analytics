// A provider that replays fixed replies in order: unit tests and the offline
// browser checks use it, so the loop runs without a model or a key.
import type { Provider, ProviderMessage, ProviderReply } from "./types";

export type ScriptedProvider = Provider & { seen: ProviderMessage[][] };

export function scriptedProvider(script: Partial<ProviderReply>[]): ScriptedProvider {
  let i = 0;
  const seen: ProviderMessage[][] = [];
  return {
    name: "fake",
    model: "scripted",
    seen,
    async reply(_system, _tools, messages) {
      seen.push(structuredClone(messages));
      const r = script[Math.min(i++, script.length - 1)] ?? {};
      return {
        text: r.text ?? null,
        toolCalls: r.toolCalls ?? [],
        usage: r.usage ?? { inputTokens: 0, outputTokens: 0, costUsd: 0 },
        refused: r.refused,
      };
    },
  };
}
