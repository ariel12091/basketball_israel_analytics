// One interface for every model provider: messages and tools in, text or tool
// calls out, with the usage needed for cost. Adapters live beside this file.
import type { ToolName } from "../registry";
import type { ToolCall } from "../run";

export type ToolSchema = {
  name: ToolName;
  description: string;
  parameters: Record<string, unknown>;
};
export type ProviderToolCall = { id: string; call: ToolCall };
export type ProviderMessage = {
  role: "user" | "assistant";
  text?: string;
  toolCalls?: ProviderToolCall[];
  toolResults?: { id: string; content: unknown }[];
  /** The provider's own assistant content, sent back unchanged (thinking and
   * tool-use blocks must survive the round trip). */
  raw?: unknown;
};
export type Usage = { inputTokens: number; outputTokens: number; costUsd: number };
export type ProviderReply = {
  text: string | null;
  toolCalls: ProviderToolCall[];
  usage: Usage;
  /** The provider declined (e.g. a refusal stop reason). */
  refused?: boolean;
  /** Provider-native assistant content for ProviderMessage.raw. */
  raw?: unknown;
};
export interface Provider {
  name: string;
  model: string;
  reply(
    system: string,
    tools: ToolSchema[],
    messages: ProviderMessage[],
    signal: AbortSignal,
  ): Promise<ProviderReply>;
}
