import { describe, expect, it } from "vitest";
import { answerJson, answerParams } from "./api";

const url = (q: string) => new URL(`https://x.test/api/v1/il/onoff?${q}`);
describe("answerJson", () => {
  it("lets the CDN keep an answer only under the version it was read at", () => {
    const answer = { meta: { version: "2026-10-25 20:51" } };
    const cdn = (q: string, a = answer) =>
      answerJson(a, url(q)).headers.get("Vercel-CDN-Cache-Control");
    expect(cdn("season=2026&v=2026-10-25+20%3A51")).toContain(
      "s-maxage=31536000",
    );
    expect(cdn("season=2026&v=2026-10-24")).toBe("no-store");
    expect(cdn("season=2026")).toBe("no-store");
    expect(cdn("v=", { meta: { version: "" } })).toBe("no-store");
    expect(cdn("v=unknown", { meta: { version: "unknown" } })).toBe("no-store");
  });
  it("hands the answer its query without v", () => {
    expect(answerParams(url("season=2026&v=1&view=x")).toString()).toBe(
      "season=2026&view=x",
    );
  });
});
describe("the requested version", () => {
  it("is visible only to the request that named it, and only for its league", async () => {
    const { requestedVersion } = await import("./dataVersion");
    const seen = await Promise.all([
      Promise.resolve().then(() => {
        answerParams(new URL("https://x.test/api/v1/eurocup/onoff?v=e1"));
        return [requestedVersion("euroleague"), requestedVersion("il")];
      }),
      Promise.resolve().then(() => requestedVersion("euroleague")),
    ]);
    expect(seen).toEqual([["e1", undefined], undefined]);
  });
});
