import "server-only";
// The model behind the box is a server setting: ASK_PROVIDER and ASK_MODEL.
// "fake" replays ASK_FAKE_SCRIPT (a JSON array of replies) for offline checks.
import { readFileSync } from "node:fs";
import { anthropicProvider } from "./providers/anthropic";
import { scriptedProvider } from "./providers/fake";
import type { Provider } from "./providers/types";

export function currentProvider(): Provider {
  const name = process.env.ASK_PROVIDER ?? "";
  if (name === "fake")
    // The fake script is supplied externally by offline test operators, never
    // a production bundle asset. Do not trace arbitrary env paths into the app.
    return scriptedProvider(
      JSON.parse(
        readFileSync(
          /* turbopackIgnore: true */ process.env.ASK_FAKE_SCRIPT ?? "",
          "utf8",
        ),
      ),
    );
  // The model is chosen by the eval (Task 10); Haiku 5.5 until then.
  if (name === "anthropic")
    return anthropicProvider(process.env.ASK_MODEL || "claude-haiku-5-5");
  throw new Error(`Ask provider "${name}" is not configured`);
}
