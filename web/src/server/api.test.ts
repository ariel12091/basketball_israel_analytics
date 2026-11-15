import { describe, expect, it } from "vitest";
import { answerFormat, answerJson, answerParams, answerShape, pageQuery, ParamError } from "./api";

describe("pageQuery", () => {
  it("is the page's query: defaults filled, given values kept", () => {
    const q = pageQuery("il", new URLSearchParams("view=Totals"), { view: "Per Game", min_gp: "1" });
    expect(q.view).toBe("Totals");
    expect(q.min_gp).toBe("1");
    expect(q.season).toBeTruthy();
  });
  it("refuses a key the page does not read, instead of ignoring it", () => {
    expect(() => pageQuery("il", new URLSearchParams("mn_gp=5"), { min_gp: "1" })).toThrow(ParamError);
  });
});

describe("answerShape / answerFormat", () => {
  it("reads shape: absent is the population, table is the finished table", () => {
    const s = new URLSearchParams("season=2027&shape=table");
    expect(answerShape(new URLSearchParams("season=2027"))).toBe("population");
    expect(answerShape(s)).toBe("table");
    // Consumed: the answer functions never see it.
    expect(s.has("shape")).toBe(false);
  });
  it("refuses any other shape (no silent default)", () => {
    expect(() => answerShape(new URLSearchParams("shape=full"))).toThrow(ParamError);
    try {
      answerShape(new URLSearchParams("shape=full"));
    } catch (e) {
      expect((e as ParamError).param).toBe("shape");
    }
  });
  it("reads format: json by default, csv, nothing else", () => {
    const s = new URLSearchParams("format=csv");
    expect(answerFormat(new URLSearchParams(""))).toBe("json");
    expect(answerFormat(new URLSearchParams("format=json"))).toBe("json");
    expect(answerFormat(s)).toBe("csv");
    expect(s.has("format")).toBe(false);
    expect(() => answerFormat(new URLSearchParams("format=xlsx"))).toThrow(ParamError);
  });
});

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
