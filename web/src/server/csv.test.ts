import { describe, expect, it } from "vitest";
import { cellText, displaySpec } from "@/ask/display";
import { csvText } from "./csv";
import { csvColumns } from "./csvColumns";

describe("csvText: the page's CSV export format", () => {
  const cols = [
    { label: "Team", text: (r: Record<string, unknown>) => String(r.team ?? "") },
    { label: "Note", text: (r: Record<string, unknown>) => String(r.note ?? "") },
  ];
  it("quotes every field, doubles quotes, joins lines with CRLF", () => {
    expect(csvText(cols, [{ team: 'A "B"', note: "x, y" }])).toBe('"Team","Note"\r\n"A ""B""","x, y"');
  });
  it("defuses formula-like text and keeps numeric text (csv_defuse_formula)", () => {
    expect(csvText(cols, [{ team: "=SUM(A1)", note: "-3.2%" }])).toBe(`"Team","Note"\r\n"'=SUM(A1)","-3.2%"`);
    expect(csvText(cols, [{ team: "@x", note: "+1" }]).split("\r\n")[1]).toBe(`"'@x","+1"`);
  });
  it("an empty table is the header line only", () => {
    expect(csvText(cols, [])).toBe('"Team","Note"');
  });
});

describe("csvColumns: the page's visible columns, as the chat's pinned specs format them", () => {
  const rows = [{ team_id: 2, team_name: "MACCABI", net_rtg: 12.34, rank_net_rtg: 1, off_ppp: 110, team_name_he: "מכבי" }];
  it("labels come from the spec; chat-only columns are left out", () => {
    const cols = csvColumns("team_ratings", "il", null);
    const labels = cols.map((c) => c.label);
    expect(labels[0]).toBe("Team");
    expect(labels).toContain("Net Rtg");
    expect(labels).not.toContain("Team (Hebrew)");
    const spec = displaySpec("team_ratings", "il", null);
    expect(Object.values(spec).filter((s) => !s.chatOnly && !s.derived)).toHaveLength(cols.length);
  });
  it("each cell is the page's text (cellText, pinned by display.test.ts)", () => {
    const cols = csvColumns("team_ratings", "il", null);
    const net = cols.find((c) => c.label === "Net Rtg")!;
    expect(net.text(rows[0])).toBe(cellText("team_ratings", "il", null, "net_rtg", { id: "row1", cells: rows[0] }));
  });
  it("follows the view (On/Off Four Factors) and the league (no DFL outside Israel)", () => {
    expect(csvColumns("onoff", "il", "four_factors").map((c) => c.label)).toContain("Net impact (on-off)");
    expect(csvColumns("player_stats", "euroleague", null).some((c) => /dfl|deflection/i.test(c.label))).toBe(false);
  });
});
