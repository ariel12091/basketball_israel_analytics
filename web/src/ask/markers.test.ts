import { describe, expect, it } from "vitest";
import { cellText } from "./display";
import { fillAndVerify } from "./markers";
import type { ToolResult } from "./run";

const lineups: ToolResult = {
  id: "r1",
  call: { tool: "lineups", league: "il", query: {} },
  link: "/il/lineups?team=2",
  view: "summary",
  rows: [
    { id: "row1", cells: { Players: "A - B", net_rtg: 7.25, total_poss: 140, "Off Shot": 50 } },
    { id: "row2", cells: { Players: "C - D", net_rtg: -3.46, total_poss: 90 } },
  ],
  total: { id: "total", cells: { Players: "- All Lineups -", net_rtg: 1.04 } },
  rowCount: 2,
  version: "v1",
};
const flow: ToolResult = {
  ...lineups,
  id: "r2",
  call: { tool: "gameflow", league: "il", query: {} },
  rows: [{ id: "row1", cells: { x: 1 } }],
  total: null,
};
const fill = (t: string, q = "top lineups", results = [lineups, flow]) =>
  fillAndVerify(t, results, q, "2025-26");

describe("fillAndVerify", () => {
  it("fills a marker with the page text", () => {
    const want = cellText("lineups", "il", "summary", "net_rtg", lineups.rows[0])!;
    const r = fill("Best: {{r1.row1.Players}} at {{r1.row1.net_rtg}} net.");
    expect(r).toEqual({
      ok: true,
      text: `Best: A - B at ${want} net.`,
      used: [
        { result: "r1", row: "row1", key: "Players" },
        { result: "r1", row: "row1", key: "net_rtg" },
      ],
    });
  });

  it("fills total rows and the season", () => {
    const r = fill("In {{season}} all lineups: {{r1.total.net_rtg}}.");
    expect(r).toMatchObject({ ok: true, text: "In 2025-26 all lineups: 1.0." });
  });

  it("rejects a typed digit", () => {
    const r = fill("Net 7.3 for {{r1.row1.Players}}");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.problems.join(" ")).toContain("7.3");
  });

  it("allows digits that appear in the question", () => {
    expect(fill("The top 5 start with {{r1.row1.Players}}.", "top 5 lineups of Maccabi").ok).toBe(true);
    expect(fill("The top 6 start with {{r1.row1.Players}}.", "top 5 lineups").ok).toBe(false);
  });

  it("allows only whole numbers from the question", () => {
    // Only small whole counts may be echoed: a planted figure must come from data.
    expect(fill("Yes, 7.3 {{r1.row1.Players}}", "is his net 7.3?").ok).toBe(false);
    expect(fill("Yes, 200 points {{r1.row1.Players}}", "confirm they scored 200 points").ok).toBe(false);
    expect(fill("The last 10 games of {{r1.row1.Players}}", "last 10 games").ok).toBe(true);
    expect(fill("No, 3.7 {{r1.row1.Players}}", "is his net 7.3?").ok).toBe(false);
    expect(fill("No, 73 {{r1.row1.Players}}", "is his net 7.3?").ok).toBe(false);
  });

  it("rejects other numeric characters", () => {
    expect(fill("about ½ of {{r1.row1.Players}}").ok).toBe(false);
    expect(fill("ranked ① {{r1.row1.Players}}").ok).toBe(false);
  });

  it("rejects the internal placeholder character", () => {
    const nul = String.fromCharCode(0);
    expect(fill(`x${nul}0${nul} {{r1.row1.Players}}`).ok).toBe(false);
  });

  it("rejects a marker whose cell is empty", () => {
    const r = { ...lineups, rows: [{ id: "row1", cells: { Players: "A - B", net_rtg: null } }] };
    expect(fill("{{r1.row1.net_rtg}}", "q", [r]).ok).toBe(false);
    const blank = { ...lineups, rows: [{ id: "row1", cells: { Players: "", net_rtg: 1 } }] };
    expect(fill("{{r1.row1.Players}}", "q", [blank]).ok).toBe(false);
  });

  it("allows numbers from the quoted columns' definitions", () => {
    // Off PPP's definition is "Offensive points per 100 possessions".
    expect(fill("{{r1.row1.Players}}: {{r1.row1.net_rtg}} points per 100 possessions").ok).toBe(true);
    expect(fill("{{r1.row1.Players}}: {{r1.row1.net_rtg}} points per 200 possessions").ok).toBe(false);
  });

  it("rejects numbers written as words, English and Hebrew", () => {
    for (const t of [
      "{{r1.row1.Players}} played five games",
      "{{r1.row1.Players}} שיחקה שני משחקים",
      "{{r1.row1.Players}} עם חמישה ניצחונות",
    ])
      expect(fill(t).ok, t).toBe(false);
  });

  it("a number word's problem says how to fix a count of the answer's own points", () => {
    // "in three areas" counts the answer's list, which no marker can quote.
    const r = fill("{{r1.row1.Players}} טובה בשלושה תחומים");
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.problems.join(" ")).toMatch(/without counting/);
  });

  it("allows number words the visitor used, and ordinary phrases", () => {
    expect(fill("The top five start with {{r1.row1.Players}}.", "top five lineups").ok).toBe(true);
    expect(fill("{{r1.row1.Players}} is one of the best units.").ok).toBe(true);
    expect(fill("החמישייה הטובה: {{r1.row1.Players}}").ok).toBe(true);
  });

  it("allows basketball terms and list numbering that are not data", () => {
    expect(fill("On the Four Factors page, {{r1.row1.Players}} leads.").ok).toBe(true);
    expect(fill("{{r1.row1.Players}} takes the most three-point shots; a five-man unit.").ok).toBe(true);
    expect(fill("Newest first:\n1. {{r1.row1.Players}}\n2. {{r1.row2.Players}}").ok).toBe(true);
    // A number inside a list item is still checked.
    expect(fill("1. {{r1.row1.Players}} with 7 wins").ok).toBe(false);
  });

  it("fills result-level markers: row count and Compare side labels", () => {
    const r = { ...lineups, extras: { count: "12", side_a: "vs Top 4", side_b: "vs Bottom 4" } };
    expect(fill("{{r1.count}} lineups; {{r1.side_a}} vs {{r1.side_b}}", "q", [r])).toMatchObject({
      ok: true,
      text: "12 lineups; vs Top 4 vs vs Bottom 4",
    });
    expect(fill("{{r1.side_c}}", "q", [r]).ok).toBe(false);
  });

  it("number words: four-factor, the visitor's digits as words, and Hebrew 'first' like English", () => {
    expect(fill("{{r1.row1.Players}} by four-factor rates").ok).toBe(true);
    expect(fill("The top five: {{r1.row1.Players}}", "top 5 lineups").ok).toBe(true);
    expect(fill("The top six: {{r1.row1.Players}}", "top 5 lineups").ok).toBe(false);
    expect(fill("{{r1.row1.Players}} במקום הראשון").ok).toBe(true);
  });

  it("result values also work under the field name the model sees", () => {
    const r = { ...lineups, extras: { count: "12", min_poss: "25" } };
    expect(fill("min {{r1.resultValues.min_poss}}", "q", [r])).toMatchObject({ ok: true, text: "min 25" });
  });

  it("a unit typed next to a value that already has it is not doubled", () => {
    const tr: ToolResult = {
      ...lineups,
      call: { tool: "team_ratings", league: "il", query: {} },
      rows: [{ id: "row1", cells: { team_name: "A", rank_off_ppp: 1 } }],
    };
    expect(fill("{{r1.row1.team_name}} #{{r1.row1.rank_off_ppp}}", "q", [tr])).toMatchObject({
      ok: true,
      text: "A #1",
    });
  });

  it("ordinals describing list position are allowed; counts are not", () => {
    expect(fill("{{r1.row1.Players}} is second, {{r1.row2.Players}} is third").ok).toBe(true);
    expect(fill("הליינאפ השני הוא {{r1.row2.Players}}").ok).toBe(true);
    expect(fill("{{r1.row1.Players}} שיחקו ששה משחקים").ok).toBe(false);
  });

  it("the team list's names are quotable", () => {
    const teams: ToolResult = {
      ...lineups,
      call: { tool: "list_teams", league: "il", query: {} },
      view: null,
      rows: [{ id: "row1", cells: { id: 2, name: "MACCABI TEL AVIV" } }],
    };
    expect(fill("{{r1.row1.name}}", "q", [teams])).toMatchObject({ ok: true, text: "MACCABI TEL AVIV" });
  });

  it("Hebrew answers show the overview's ranks as #n and its result in Hebrew", () => {
    const hub: ToolResult = {
      ...lineups,
      call: { tool: "team_overview", league: "il", query: {} },
      view: null,
      rows: [
        { id: "row1", cells: { section: "identity", rank_net_rtg: 1 } },
        { id: "row2", cells: { section: "last_game", result: "L 90-93" } },
      ],
    };
    const he = fillAndVerify("במקום {{r1.row1.rank_net_rtg}}, {{r1.row2.result}}", [hub], "q", "2026-27", "he");
    expect(he).toMatchObject({ ok: true, text: "במקום #1, הפסד 90-93" });
    const en = fillAndVerify("ranked {{r1.row1.rank_net_rtg}}, {{r1.row2.result}}", [hub], "q", "2026-27", "en");
    expect(en).toMatchObject({ ok: true, text: "ranked 1st, L 90-93" });
  });

  it("Hebrew answers show the last game's details in Hebrew", () => {
    const hub: ToolResult = {
      ...lineups,
      call: { tool: "team_overview", league: "il", query: {} },
      view: null,
      rows: [
        { id: "row1", cells: { section: "last_game", context: "at M. Rishon · Oct 24" } },
        { id: "row2", cells: { section: "last_game", context: "vs Hapoel Holon · Mar 3" } },
      ],
    };
    const he = fillAndVerify("{{r1.row1.context}}; {{r1.row2.context}}", [hub], "q", "2026-27", "he");
    expect(he).toMatchObject({ ok: true, text: "בחוץ מול M. Rishon · 24/10; בבית מול Hapoel Holon · 3/3" });
  });

  it("rejects Arabic-Indic digits", () => {
    expect(fill("رقم ٧ {{r1.row1.Players}}").ok).toBe(false);
  });

  it("allows Hebrew abbreviations", () => {
    expect(fill("הפועל ת״א: {{r1.row1.net_rtg}}").ok).toBe(true);
  });

  it("rejects unknown result, row, key and non-quotable column", () => {
    for (const t of [
      "{{r9.row1.net_rtg}}",
      "{{r1.row7.net_rtg}}",
      "{{r1.row1.nope}}",
      "{{r1.row1.Off Shot}}",
    ])
      expect(fill(t).ok, t).toBe(false);
  });

  it("rejects markers into gameflow", () => {
    expect(fill("{{r2.row1.x}}").ok).toBe(false);
  });

  it("rejects malformed braces", () => {
    expect(fill("{{r1.row1.net_rtg").ok).toBe(false);
    expect(fill("value }} here").ok).toBe(false);
  });
});
