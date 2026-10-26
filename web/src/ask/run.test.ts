import { readFileSync } from "node:fs";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  LINEUP_QUERY_DEFAULTS,
  PLAYER_QUERY_DEFAULTS,
  lineupsPath,
  playerStatsPath,
  queryFromSearch,
} from "@/lib/tabQuery";
import { ParamError } from "@/server/api";
import { DeadlineError } from "@/server/deadline";

const lineups = vi.fn();
const teamRatings = vi.fn();
const playerStats = vi.fn();
const compare = vi.fn();
const home = vi.fn();
const onoff = vi.fn();
vi.mock("@/server/onoffApi", () => ({
  onoffAnswer: (l: string, s: URLSearchParams) => onoff(l, s),
}));
vi.mock("@/server/lineupsApi", () => ({
  lineupsAnswer: (l: string, s: URLSearchParams) => lineups(l, s),
  lineupRosterAnswer: vi.fn(),
}));
vi.mock("@/server/teamRatingsRepo", () => ({
  teamRatingsAnswer: (l: string, s: URLSearchParams) => teamRatings(l, s),
}));
vi.mock("@/server/playerStatsRepo", () => ({
  playerStatsAnswer: (l: string, s: URLSearchParams) => playerStats(l, s),
}));
vi.mock("@/server/compareRepo", () => ({
  compareAnswer: (s: URLSearchParams) => compare(s),
}));
vi.mock("@/server/homeRepo", async (orig) => ({
  ...(await orig<typeof import("@/server/homeRepo")>()),
  homeAnswer: (s: URLSearchParams) => home(s),
}));

const { runTool } = await import("./run");
const { TOOLS } = await import("./registry");
const later = Date.now() + 60_000;
const byNet = { key: "net_rtg", desc: true };

const lineupRows = [
  { team_id: 2, Team: "A", Players: "p1", net_rtg: 3, total_poss: 100 },
  { team_id: 2, Team: "A", Players: "p2", net_rtg: null, total_poss: 100 },
  { team_id: 2, Team: "A", Players: "p3", net_rtg: 7, total_poss: 100 },
  { team_id: 3, Team: "B", Players: "q1", net_rtg: 30, total_poss: 100 },
];
const payload = (rows: unknown[]) => ({ rows, meta: { view: "summary", version: "v1" } });

beforeEach(() => {
  for (const f of [lineups, teamRatings, playerStats, compare, home, onoff]) f.mockReset();
  lineups.mockResolvedValue(payload(lineupRows));
});

describe("runTool: derived On/Off values", () => {
  it("sorts by the 3PT% half of the Off Shot cell", async () => {
    const shot = (id: number, m3: number) => ({
      team_id: 3, player_id: id, Team: "H", Player: `P${id}`, "ON Poss": 500, "OFF Poss": 500,
      "Off Shot ON": 100, off_on_fg2_made: 30, off_on_fg2_att: 60, off_on_fg3_made: m3, off_on_fg3_att: 40,
    });
    onoff.mockResolvedValue({ rows: [shot(1, 12), shot(2, 18), shot(3, 15)], league3p: null, meta: { view: "summary", version: "v1" } });
    const r = await runTool(
      "r1",
      { tool: "onoff", league: "il", query: { teams: "3", min_all: "0", min_on: "0" }, sort: { key: "Off Shot ON 3PT", desc: true } },
      later,
    );
    if (!("rows" in r)) throw new Error(JSON.stringify(r));
    expect(r.rows.map((x) => x.cells.Player)).toEqual(["P2", "P3", "P1"]);
  });

  it("sorts tagged rows first by a tag", async () => {
    // The page rules set sample_flag themselves: P10 has few possessions and
    // the top net impact, so it is the one tagged.
    const row = (id: number) => ({
      team_id: 2, player_id: id, Team: "M", Player: `P${id}`,
      "ON Poss": id === 10 ? 20 : 800, "OFF Poss": 800, "Net RTG Diff": id === 10 ? 40 : id,
    });
    onoff.mockResolvedValue({
      rows: Array.from({ length: 10 }, (_, i) => row(i + 1)),
      league3p: null,
      meta: { view: "summary", version: "v1" },
    });
    const r = await runTool(
      "r1",
      { tool: "onoff", league: "il", query: { teams: "2", min_all: "0", min_on: "0" }, sort: { key: "sample_flag", desc: true } },
      later,
    );
    if (!("rows" in r)) throw new Error(JSON.stringify(r));
    expect(r.rows[0].cells).toMatchObject({ Player: "P10", sample_flag: true });
  });
});

describe("runTool: page tools build their request as the page does", () => {
  it("the API request equals the page builder's, and team reaches the page rows", async () => {
    const query = { season: "2027", team: "2", size: "5" };
    const r = await runTool("r1", { tool: "lineups", league: "il", query, sort: byNet }, later);
    const want = new URL(
      lineupsPath("il", queryFromSearch("il", query, LINEUP_QUERY_DEFAULTS)),
      "http://x",
    ).searchParams;
    expect(lineups.mock.calls[0][1].toString()).toBe(want.toString());
    if (!("rows" in r)) throw new Error(r.error);
    expect(r.rows.map((x) => x.cells.Players)).toEqual(["p3", "p1", "p2"]);
  });

  it("a key the page reads but ignores is rejected (teams on Lineups)", async () => {
    const r = await runTool("r1", { tool: "lineups", league: "il", query: { teams: "2" }, sort: byNet }, later);
    expect(r).toMatchObject({ param: "teams" });
    expect(lineups).not.toHaveBeenCalled();
  });

  it("unknown and misspelled keys are rejected, also where parsers copy params", async () => {
    for (const [tool, key] of [
      ["lineups", "bogus"],
      ["player_stats", "seasn"],
      ["team_ratings", "team"],
    ] as const) {
      const r = await runTool("r1", { tool, league: "il", query: { [key]: "1" }, sort: byNet }, later);
      expect(r, `${tool}/${key}`).toMatchObject({ param: key });
    }
  });

  it("repeated values are rejected", async () => {
    const r = await runTool(
      "r1",
      { tool: "lineups", league: "il", query: { season: ["2026", "2027"] }, sort: byNet },
      later,
    );
    expect(r).toMatchObject({ param: "season" });
  });

  it("a key set to its default is accepted", async () => {
    const r = await runTool(
      "r1",
      { tool: "lineups", league: "il", query: { view: "summary" }, sort: byNet },
      later,
    );
    expect("rows" in r).toBe(true);
  });

  it("player stats request matches the page builder (view stays local)", async () => {
    playerStats.mockResolvedValue({ rows: [], identities: [], meta: {} });
    const query = { season: "2026", view: "Totals", teams: "2" };
    await runTool("r1", { tool: "player_stats", league: "il", query, sort: { key: "pts", desc: true } }, later);
    const want = new URL(
      playerStatsPath("il", queryFromSearch("il", query, PLAYER_QUERY_DEFAULTS)),
      "http://x",
    ).searchParams;
    expect(playerStats.mock.calls[0][1].toString()).toBe(want.toString());
  });

  it("table tools need a sort", async () => {
    const r = await runTool("r1", { tool: "lineups", league: "il", query: {} }, later);
    expect(r).toMatchObject({ param: "sort" });
  });

  it("simple tools accept only their own keys", async () => {
    home.mockResolvedValue({ frames: undefined, meta: {} });
    const bad = await runTool("r1", { tool: "team_overview", league: "il", query: { team: "2" } }, later);
    expect(bad).toMatchObject({ param: "team" });
    const good = await runTool(
      "r1",
      { tool: "team_overview", league: "il", query: { season: "2026", team_id: "2" } },
      later,
    );
    expect(home.mock.calls[0][0].toString()).toBe("season=2026&team_id=2");
    expect("rows" in good).toBe(true);
  });
});

describe("runTool: errors, sorting, limits, links", () => {
  it("ParamError becomes a tool error with its param", async () => {
    lineups.mockRejectedValue(new ParamError("size", "Invalid size"));
    const r = await runTool("r1", { tool: "lineups", league: "il", query: { size: "9" }, sort: byNet }, later);
    expect(r).toMatchObject({ param: "size", error: "Invalid size" });
  });

  it("deadline error becomes a 'took too long' tool error", async () => {
    lineups.mockRejectedValue(new DeadlineError());
    const r = await runTool("r1", { tool: "lineups", league: "il", query: {}, sort: byNet }, later);
    expect(r).toMatchObject({ error: expect.stringMatching(/too long/) });
  });

  it("no call starts after the deadline", async () => {
    const r = await runTool("r1", { tool: "lineups", league: "il", query: {}, sort: byNet }, Date.now() - 1);
    expect(lineups).not.toHaveBeenCalled();
    expect(r).toMatchObject({ error: expect.stringMatching(/time/) });
  });

  it("compare on euroleague returns tool error naming the league", async () => {
    const r = await runTool("r1", { tool: "compare", league: "euroleague", query: {} }, later);
    expect(compare).not.toHaveBeenCalled();
    expect(r).toMatchObject({ error: expect.stringMatching(/Israeli League only/) });
  });

  it("sorts by key, missing last, both directions; the page's TOTAL is split out", async () => {
    const call = { tool: "lineups" as const, league: "il" as const, query: { team: "2" } };
    const desc = await runTool("r1", { ...call, sort: byNet }, later);
    const asc = await runTool("r1", { ...call, sort: { key: "net_rtg", desc: false } }, later);
    if (!("rows" in desc) || !("rows" in asc)) throw new Error("tool error");
    expect(desc.rows.map((r) => r.cells.net_rtg)).toEqual([7, 3, null]);
    expect(asc.rows.map((r) => r.cells.net_rtg)).toEqual([3, 7, null]);
    expect(desc.rows.map((r) => r.id)).toEqual(["row1", "row2", "row3"]);
    expect(desc.total).toMatchObject({ id: "total" });
    // 2026-10-25: the model quoted the "- All Lineups -" label as player names.
    expect(desc.total!.cells).not.toHaveProperty("Players");
    expect(desc).toMatchObject({ view: "summary", version: "v1", rowCount: 3, extras: { count: "3" } });
  });

  it("unknown sort key is a tool error", async () => {
    const r = await runTool("r1", { tool: "lineups", league: "il", query: {}, sort: { key: "nope", desc: true } }, later);
    expect(r).toMatchObject({ error: expect.stringMatching(/nope/) });
    // The error names the columns that can be sorted, so the model can retry.
    expect((r as { error: string }).error).toMatch(/net_rtg/);
    expect((r as { error: string }).error).toMatch(/total_poss/);
  });

  it("limit defaults to 5 and caps at 10; rowCount is pre-limit", async () => {
    const rows = Array.from({ length: 12 }, (_, i) => ({ team_id: 2, net_rtg: i, total_poss: 100 }));
    lineups.mockResolvedValue(payload(rows));
    const base = { tool: "lineups" as const, league: "il" as const, query: {}, sort: byNet };
    const d = await runTool("r1", base, later);
    const big = await runTool("r1", { ...base, limit: 50 }, later);
    if (!("rows" in d) || !("rows" in big)) throw new Error("tool error");
    expect([d.rows.length, big.rows.length, big.rowCount]).toEqual([5, 10, 12]);
  });

  it("lineups ranked by a rating use a 100-possession minimum, also in the link", async () => {
    lineups.mockResolvedValue(
      payload([
        { team_id: 2, Players: "tiny", net_rtg: 233.3, total_poss: 5 },
        { team_id: 2, Players: "real", net_rtg: 12.1, total_poss: 140 },
        { team_id: 2, Players: "real2", net_rtg: 8.4, total_poss: 120 },
        { team_id: 2, Players: "real3", net_rtg: 3.2, total_poss: 101 },
      ]),
    );
    const r = await runTool("r1", { tool: "lineups", league: "il", query: { team: "2" }, sort: byNet }, later);
    if (!("rows" in r)) throw new Error(r.error);
    expect(r.rows.map((x) => x.cells.Players)).toEqual(["real", "real2", "real3"]);
    const stats = JSON.parse(new URL(r.link!, "http://x").searchParams.get("stats")!);
    expect(stats).toEqual([{ col: "total_poss", op: "ge", value: 100 }]);
    expect(r.extras).toMatchObject({ min_poss: "100" });
  });

  it("early in a season the minimum steps down (100, 50, 25) until 3 lineups qualify", async () => {
    lineups.mockResolvedValue(
      payload([
        { team_id: 2, Players: "a", net_rtg: 30, total_poss: 120 },
        { team_id: 2, Players: "b", net_rtg: 20, total_poss: 60 },
        { team_id: 2, Players: "c", net_rtg: 10, total_poss: 55 },
        { team_id: 2, Players: "tiny", net_rtg: 99, total_poss: 8 },
      ]),
    );
    const r = await runTool("r1", { tool: "lineups", league: "il", query: { team: "2" }, sort: byNet }, later);
    if (!("rows" in r)) throw new Error(r.error);
    expect(r.rows.map((x) => x.cells.Players)).toEqual(["a", "b", "c"]);
    expect(r.extras).toMatchObject({ min_poss: "50" });
    expect(r.link).toContain(encodeURIComponent('"value":50'));
    expect(lineups).toHaveBeenCalledTimes(1);
  });

  it("an explicit stats filter, or a volume sort, gets no automatic minimum", async () => {
    lineups.mockResolvedValue(payload([{ team_id: 2, Players: "tiny", net_rtg: 233.3, total_poss: 5 }]));
    const all = await runTool(
      "r1",
      { tool: "lineups", league: "il", query: { team: "2", stats: "[]" }, sort: byNet },
      later,
    );
    const byPoss = await runTool(
      "r1",
      { tool: "lineups", league: "il", query: { team: "2" }, sort: { key: "total_poss", desc: true } },
      later,
    );
    expect([all, byPoss].map((r) => ("rows" in r ? r.rows.length : -1))).toEqual([1, 1]);
  });

  it("link carries the query and page", async () => {
    const r = await runTool(
      "r1",
      { tool: "lineups", league: "il", query: { season: "2027", team: "2", size: "5" }, sort: { key: "total_poss", desc: true } },
      later,
    );
    expect(r).toMatchObject({ link: "/il/lineups?season=2027&team=2&size=5" });
  });

  it("the linked page builds its request with the same builder and answer function", () => {
    for (const t of Object.values(TOOLS)) {
      if (!t.request) continue;
      const src = readFileSync(path.resolve("src/app/[league]", t.page!, "page.tsx"), "utf8");
      // Lineups: the page reads lineupsPageAnswer (lineupsAnswer + lineupPage)
      // at lineupsPagePath (lineupsPath + the table's state).
      const wraps = t.answerName === "lineupsAnswer";
      expect(src, t.name).toMatch(new RegExp(`\\b${wraps ? "lineupsPageAnswer" : t.answerName}\\(`));
      // The page's request URL comes from its entry in lib/tabPages.ts.
      expect(src, t.name).toContain(`TAB_FIRST_PATH["${t.page}"](`);
      const first = readFileSync(path.resolve("src/lib/tabPages.ts"), "utf8");
      const entry = first.match(new RegExp(`(?:"${t.page}"|\\b${t.page}):[^]*?\\),\\n`))?.[0] ?? "";
      expect(entry, t.name).toMatch(new RegExp(`\\b${wraps ? "lineupsPagePath" : t.request.pathName}\\(`));
      expect(entry, t.name).toContain("queryFromSearch(");
    }
  });

  it("list_teams returns the season catalogue (IL) and reader teams (Euro)", async () => {
    const il = await runTool("r1", { tool: "list_teams", league: "il", query: { season: "2027" } }, later);
    if (!("rows" in il)) throw new Error("tool error");
    expect(il.rows.length).toBeGreaterThan(10);
    expect(il.rows[0].cells).toHaveProperty("name");
    // Named team_id like every other tool, so it never collides with the row id.
    expect(il.rows[0].cells).toHaveProperty("team_id");
    expect(il.rows[0].cells).not.toHaveProperty("id");
    expect(il.link).toBeNull();
    teamRatings.mockResolvedValue({ rows: [], meta: { teams: [{ id: 5, name: "Real Madrid" }] } });
    const el = await runTool("r1", { tool: "list_teams", league: "euroleague", query: { season: "2026" } }, later);
    expect(el).toMatchObject({ rows: [{ cells: { team_id: 5, name: "Real Madrid" } }] });
  });

  it("team_overview returns every hub section, not the default 5 rows", async () => {
    const { emptyHomeFrames } = await vi.importActual<typeof import("@/server/homeRepo")>("@/server/homeRepo");
    const ratings = [{ team_id: 2, team_name: "A", off_ppp: 110, def_ppp: 100, net_rtg: 10, rank_net_rtg: 1 }];
    const four_factors = [{ team_id: 2, off_efg: 55, off_tov: 12, off_oreb: 30, off_ftr: 25, def_efg: 50, def_tov: 14, def_oreb: 28, def_ftr: 22 }];
    home.mockResolvedValue({ frames: { ...emptyHomeFrames(), ratings, four_factors }, meta: {} });
    const r = await runTool("r1", { tool: "team_overview", league: "il", query: { season: "2026", team_id: "2" } }, later);
    if (!("rows" in r)) throw new Error(r.error);
    expect(r.rows.length).toBe(r.rowCount);
    expect(r.rowCount).toBeGreaterThan(5);
  });

  it("team_overview rows come from the hub's own rules", async () => {
    const { flatten } = await import("./flatten");
    const { emptyHomeFrames } = await vi.importActual<typeof import("@/server/homeRepo")>("@/server/homeRepo");
    const frames = {
      ...emptyHomeFrames(),
      ratings: [
        { team_id: 2, team_name: "A", off_ppp: 110, def_ppp: 100, net_rtg: 10, rank_net_rtg: 2 },
        { team_id: 3, team_name: "B", off_ppp: 105, def_ppp: 95, net_rtg: 12, rank_net_rtg: 1 },
      ],
      lineups: [{ net_rtg: 4, off_poss: 60, def_poss: 50 }],
    };
    const { rows } = flatten("team_overview", "il", { frames }, new URLSearchParams("team_id=2"));
    expect(rows[0]).toMatchObject({ section: "identity", net_rtg: 10, n_teams: 2, rank_net_rtg: 2 });
    expect(rows.filter((r) => /_lineup$/.test(String(r.section)))).toHaveLength(2);
    expect(flatten("team_overview", "il", { frames }, new URLSearchParams()).rows).toEqual([]);
  });
});

describe("runTool: guide", () => {
  it("returns the whole topic, links to its tab, and reports a bad topic as a parameter", async () => {
    const r = await runTool("r1", { tool: "guide", league: "il", query: { topic: "onoff", view: "summary" } }, later);
    if (!("rows" in r)) throw new Error(r.error);
    expect(r.link).toBe("/il/onoff?view=summary");
    expect(r.rows.length).toBe(r.rowCount);
    expect(r.rows[0].cells).toMatchObject({ kind: "explainer", view: "summary" });
    const g = await runTool("r2", { tool: "guide", league: "il", query: { topic: "glossary" } }, later);
    expect("rows" in g && g.link).toBe(null);
    const bad = await runTool("r3", { tool: "guide", league: "il", query: { topic: "news" } }, later);
    expect(bad).toMatchObject({ param: "topic" });
  });
});
