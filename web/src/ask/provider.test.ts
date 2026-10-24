import { afterEach, describe, expect, it, vi } from "vitest";
import { currentProvider } from "./provider";

afterEach(() => vi.unstubAllEnvs());

describe("currentProvider", () => {
  it("anthropic uses ASK_MODEL, defaulting to Claude Haiku 5.5", () => {
    vi.stubEnv("ASK_PROVIDER", "anthropic");
    vi.stubEnv("ANTHROPIC_API_KEY", "test-key");
    expect(currentProvider()).toMatchObject({ name: "anthropic", model: "claude-haiku-5-5" });
    vi.stubEnv("ASK_MODEL", "claude-sonnet-5-5");
    expect(currentProvider()).toMatchObject({ name: "anthropic", model: "claude-sonnet-5-5" });
  });

  it("an unknown provider fails closed", () => {
    vi.stubEnv("ASK_PROVIDER", "nope");
    expect(() => currentProvider()).toThrow(/not configured/);
  });
});
