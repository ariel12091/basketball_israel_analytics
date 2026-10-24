import "server-only";
// The model behind the box is a server setting: ASK_PROVIDER and ASK_MODEL.
// "fake" replays ASK_FAKE_SCRIPT (a JSON array of replies) for offline checks.
import { readFileSync } from "node:fs";
import { scriptedProvider } from "./providers/fake";
import type { Provider } from "./providers/types";

export function currentProvider(): Provider {
  const name = process.env.ASK_PROVIDER ?? "";
  if (name === "fake")
    return scriptedProvider(JSON.parse(readFileSync(process.env.ASK_FAKE_SCRIPT ?? "", "utf8")));
  throw new Error(`Ask provider "${name}" is not configured`);
}
