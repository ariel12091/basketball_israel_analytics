import { describe, expect, it } from "vitest";
import { columnDefinition } from "./display";
import { fillAndVerify } from "./markers";
import { protectSeason } from "./semantics";
import type { ToolCall, ToolResult } from "./run";

const call: ToolCall = { tool: "onoff", league: "il", query: { season: "2027" } };
const result: ToolResult = { id: "r1", call, view: "summary", rows: [{ id: "row1", cells: {
  "Off ON PPP": 108.8, "Off OFF PPP": 81.2, "ON Poss": 388, "OFF Poss": 156,
} }], total: null, link: null, rowCount: 1, version: "v1", extras: { rating_basis: "points per 100 possessions" } };
const check = (text: string, rs = [result], lang: "en" | "he" = "en") => fillAndVerify(text, rs, "compare the player", "2026-27", lang);

describe("metric semantics", () => {
  it.each(["Off PPP on court", "Off PPP off court", "Def PPP on court", "Def PPP off court"])("resolves the page definition for %s", label => {
    expect(columnDefinition(label)).toContain("per 100 possessions");
  });
  it.each(["il", "euroleague", "eurocup"] as const)("allows accurate digit/word units without permitting invented counts in %s", league => {
    const rs = [{ ...result, call: { ...call, league } }];
    expect(check("{{r1.row1.Off ON PPP}} points per 100 possessions", rs).ok).toBe(true);
    expect(check("{{r1.row1.Off ON PPP}} points per hundred possessions", rs).ok).toBe(true);
    expect(check("100 points on {{r1.row1.ON Poss}} possessions", rs).ok).toBe(false);
  });
  it.each([
    "Offense: {{r1.row1.Off ON PPP}} points per possession versus {{r1.row1.Off OFF PPP}} off",
    "Points per possession: {{r1.row1.Off ON PPP}}",
    "{{r1.row1.Off ON PPP}} points per {{r1.row1.ON Poss}}-possession unit",
    "יעילות של {{r1.row1.Off ON PPP}} נקודות לפוזשן",
  ])("rejects a wrong rating unit: %s", text => {
    const filled = check(text);
    expect(filled.ok).toBe(false);
    if (!filled.ok) expect(filled.problems.join(" ")).toMatch(/unit|denominator/i);
  });
  it("allows rating labels, accurate Hebrew units and an explicit correction", () => {
    expect(check("Offensive rating {{r1.row1.Off ON PPP}}, not points per possession").ok).toBe(true);
    expect(check("{{r1.row1.Off ON PPP}} נקודות לכל מאה פוזשנים", [result], "he").ok).toBe(true);
    expect(check("Offense {{r1.row1.Off ON PPP}} {{r1.rating_basis}}").ok).toBe(true);
  });
  it("allows an explicit contrast with a true unscaled ratio and a unit shorthand", () => {
    expect(check("Offensive rating {{r1.row1.Off ON PPP}} is per 100 possessions, while points per possession is the unscaled ratio.").ok).toBe(true);
    expect(check("Offensive rating {{r1.row1.Off ON PPP}} per 100 possessions; {{r1.row1.Off OFF PPP}} per 100 without him.").ok).toBe(true);
  });
  it("checks an incorrect rating definition even if the values are in the preceding sentence", () => {
    expect(check("Off PPP {{r1.row1.Off ON PPP}}. Off PPP means points per possession.").ok).toBe(false);
  });
  const normalized: ToolResult = { ...result, call: { tool: "compare", league: "il", query: { cmp_view: "players", team_rate: "Per 30 Minutes" } }, view: "players", rows: [{ id: "row1", cells: { metric_a: 18.3, metric_b: 5.7, pct: false } }], extras: { rate_basis: "per 30 minutes" } };
  it.each(["points per 30 minutes", "points per thirty minutes", "points per half-hour of playing time", "נקודות לחצי שעת משחק"])("keeps the normalization: %s", unit => {
    expect(check(`{{r1.row1.metric_a}} ${unit}`, [normalized]).ok).toBe(true);
  });
  it("rejects missing scale and per-minute substitution, but accepts its quotable basis", () => {
    expect(check("Normalized scoring {{r1.row1.metric_a}}", [normalized]).ok).toBe(false);
    expect(check("{{r1.row1.metric_a}} points per minute", [normalized]).ok).toBe(false);
    expect(check("{{r1.row1.metric_a}} points {{r1.rate_basis}}", [normalized]).ok).toBe(true);
  });
  it("uses the quoted normalization, not a different unused result's rate", () => {
    const per60 = { ...normalized, id: "r2", call: { tool: "player_stats" as const, league: "euroleague" as const, query: { view: "Per 60 Possessions" } }, view: "Per 60 Possessions", rows: [{ id: "row1", cells: { pts: 12.4 } }], extras: { rate_basis: "per 60 possessions" } };
    expect(check("{{r2.row1.pts}} points per 60 possessions", [normalized, per60]).ok).toBe(true);
    expect(check("{{r2.row1.pts}} points {{r2.values.rate_basis}}", [per60]).ok).toBe(true);
    expect(check("{{r2.row1.pts}} points per minute", [per60]).ok).toBe(false);
    expect(check("{{r2.row1.pts}} normalized", [per60]).ok).toBe(false);
  });
  it("does not apply scoring normalization to a percentage or to a definition without rating values", () => {
    const pct = { ...normalized, rows: [{ id: "row1", cells: { metric_a: 50, metric_b: 40, pct: true } }] };
    expect(check("Accuracy {{r1.row1.metric_a}}", [pct]).ok).toBe(true);
    expect(check("A true per-possession rate is points per possession.").ok).toBe(true);
  });
});

describe("season protection", () => {
  it.each(["il", "euroleague", "eurocup"] as const)("rejects a wrong current year in %s before retrieval", league => {
    const c = { ...call, league, query: { season: "2025" } };
    expect(protectSeason(c, "this season")).toMatchObject({ param: "season", error: expect.stringContaining(league === "il" ? "2027" : "2026") });
  });
  it.each(["il", "euroleague", "eurocup"] as const)("respects explicit ranges and fills an omitted historical year in %s", league => {
    const c = { ...call, league, query: {} };
    const expected = league === "il" ? "2026" : "2025";
    expect(protectSeason(c, "2025-26 defense")).toMatchObject({ query: { season: expected } });
    expect(protectSeason({ ...c, query: { season: "2027" } }, "2025-26 defense")).toMatchObject({ param: "season" });
  });
  it("allows both seasons for an explicitly matched comparison", () => {
    for (const season of ["2027", "2026"]) expect(protectSeason({ ...call, query: { season } }, "Who improved since last season?")).toMatchObject({ query: { season } });
  });
  it("accepts either reading of a bare year, and 'last year' like 'last season'", () => {
    // Israeli seasons end in their year: "2025" is 2025 or 2026; European ones start in it.
    for (const season of ["2025", "2026"])
      expect(protectSeason({ ...call, query: { season } }, "How did Hapoel do in 2025?")).toMatchObject({ query: { season } });
    expect(protectSeason({ ...call, query: { season: "2027" } }, "the 2024 season")).toMatchObject({ param: "season" });
    for (const season of ["2025", "2024"])
      expect(protectSeason({ ...call, league: "euroleague", query: { season } }, "EuroLeague leaders in 2025")).toMatchObject({ query: { season } });
    expect(protectSeason({ ...call, query: { season: "2026" } }, "who led last year?")).toMatchObject({ query: { season: "2026" } });
    expect(protectSeason({ ...call, query: { season: "2026" } }, "מי הוביל בשנה שעברה?")).toMatchObject({ query: { season: "2026" } });
  });
  it("inherits a follow-up season but current wording overrides it", () => {
    const history = [{ calls: [{ ...call, query: { season: "2025" } }] }];
    expect(protectSeason({ ...call, query: {} }, "and his defense?", history)).toMatchObject({ query: { season: "2025" } });
    expect(protectSeason({ ...call, query: { season: "2025" } }, "and this season?", history)).toMatchObject({ param: "season" });
    expect(protectSeason({ tool: "guide", league: "il", query: { topic: "onoff" } }, "2025-26")).not.toHaveProperty("error");
  });
});
