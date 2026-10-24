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
    expect(fill("Yes, 7.3 {{r1.row1.Players}}", "is his net 7.3?").ok).toBe(true);
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
