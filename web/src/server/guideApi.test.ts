import { describe, expect, it } from "vitest";
import ui from "@/data/shinyUi.json";
import { GUIDE_TOPICS, guideRows } from "./guideApi";

const q = (s: string) => new URLSearchParams(s);
const plain = (x: unknown) =>
  JSON.stringify(x)
    .match(/"t":"((?:[^"\\]|\\.)*)"/g)!
    .map((m) => JSON.parse(m.slice(4)) as string);

describe("guide: the app's own explanations, not retyped", () => {
  it("glossary: one row per section, holding every item's text", () => {
    const rows = guideRows("il", q("topic=glossary"));
    expect(rows.map((r) => r.title)).toEqual(["Efficiency", "Four Factors", "Shot Splits", "Colors & Ranking", "Filters"]);
    const all = rows.map((r) => r.text).join("\n");
    for (const t of plain(ui.glossary)) expect(all).toContain(t.trim());
  });

  it("On/Off: each view's cards, the example, the impact line and the luck popover", () => {
    const rows = guideRows("il", q("topic=onoff"));
    expect(rows.filter((r) => r.kind === "explainer").map((r) => r.view)).toEqual(["summary", "four_factors", "shot_profile"]);
    expect(rows.find((r) => r.kind === "example")?.text).toContain("Jimmy Clark III");
    expect(rows.find((r) => r.kind === "impact")?.view).toBe("four_factors");
    expect(rows.find((r) => r.kind === "popover")?.text).toMatch(/3PT luck/);
  });

  it("a view narrows to that view; an unknown view or topic is a parameter error", () => {
    const rows = guideRows("il", q("topic=lineups&view=four_factors"));
    expect(rows.filter((r) => r.view).every((r) => r.view === "four_factors")).toBe(true);
    expect(() => guideRows("il", q("topic=lineups&view=shot_profile"))).toThrow(/view must be one of/);
    expect(() => guideRows("il", q("topic=rules"))).toThrow(/topic must be one of/);
  });

  it("EuroLeague: its own fragments, no Israeli example, no Compare", () => {
    expect(guideRows("euroleague", q("topic=player_stats")).some((r) => r.kind === "example")).toBe(false);
    expect(guideRows("euroleague", q("topic=onoff")).some((r) => r.kind === "example")).toBe(false);
    expect(() => guideRows("euroleague", q("topic=compare"))).toThrow(/Israeli League only/);
  });

  it("every topic answers in the Israeli League", () => {
    for (const t of GUIDE_TOPICS) expect(guideRows("il", q(`topic=${t}`)).length, t).toBeGreaterThan(0);
  });
});
