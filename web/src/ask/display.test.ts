// Pins: for every column the ask box may quote, its text equals what the page
// itself renders for the same row (spec: "Page-identical cell text").
import { readFileSync } from "node:fs";
import path from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import LineupTable from "@/components/shiny/LineupTable";
import PlayerStatsTable from "@/components/shiny/PlayerStatsTable";
import TeamRatingsTable from "@/components/shiny/TeamRatingsTable";
import { columnsFor } from "@/components/shiny/GameLogsTable";
import { ffColumns, spColumns, summaryColumns } from "@/components/shiny/OnOffTables";
import { FF_METRIC_MAP, FF_RTG_MAP, SP_SUFFIX } from "@/rules/onoff";
import type { Row } from "@/rules/gameLogs";
import { cellText, displaySpec } from "./display";

/** The text of each td[data-column] in a rendered one-row table. */
function cells(html: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const m of html.matchAll(/<td[^>]*data-column="([^"]+)"[^>]*>([\s\S]*?)<\/td>/g))
    out[m[1].replace(/&quot;/g, '"')] = m[2]
      // DataTable's phone-only row label sits inside the first numeric cell.
      .split('<span class="ibpl-row-name">')[0]
      .replace(/<br\s*\/?>/g, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/&amp;/g, "&")
      .replace(/&#x27;/g, "'")
      .replace(/&quot;/g, '"')
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">");
  return out;
}
const row = (r: Row) => ({ id: "row1", cells: r });
// Values chosen to exercise rounding (x.x5), signs, thousands and missing.
const sample: Row = {
  team_id: 2,
  Team: "MACCABI",
  team_name: "MACCABI",
  Player: "J. Doe",
  Players: "A - B - C - D - E",
  is_total: 1,
  game_year: 2026,
};
const fill = (keys: string[], base: Row = sample): Row => {
  const r: Row = { ...base };
  keys.forEach((k, i) => {
    if (!(k in r)) r[k] = i % 5 === 3 ? null : i % 2 ? 1234.45 + i : -(i + 0.05);
  });
  return r;
};

function expectSame(
  tool: Parameters<typeof cellText>[0],
  league: "il" | "euroleague",
  view: string | null,
  r: Row,
  page: Record<string, string>,
  line?: (key: string) => number,
) {
  const spec = displaySpec(tool, league, view);
  expect(Object.keys(spec).length, `${tool}/${view}`).toBeGreaterThan(3);
  for (const key of Object.keys(spec)) {
    const base = key.startsWith("rank_") ? key.slice(5) : key;
    expect(page, `${tool}/${view}: page has ${base}`).toHaveProperty([base]);
    const text = page[base].split("\n");
    const want = key.startsWith("rank_")
      ? (text[1] ?? text[0])
      : text[line ? line(key) : 0];
    expect(cellText(tool, league, view, key, row(r)), `${tool}/${view}/${key}`).toBe(want);
  }
}

describe("cellText equals the page column text", () => {
  for (const view of ["summary", "four_factors"] as const)
    it(`lineups ${view}`, () => {
      const r = fill(Object.keys(displaySpec("lineups", "il", view)));
      const html = renderToStaticMarkup(
        createElement(LineupTable, { rows: [r], view, onLineup: () => {} }),
      );
      expectSame("lineups", "il", view, r, cells(html));
    });

  for (const view of ["summary", "four_factors", "shot_profile", "traditional"] as const)
    it(`team_ratings ${view}`, () => {
      const keys = Object.keys(displaySpec("team_ratings", "il", view));
      const r = fill(keys);
      for (const k of keys.filter((x) => x.startsWith("rank_"))) r[k] = 3;
      const html = renderToStaticMarkup(
        createElement(TeamRatingsTable, {
          rows: [r],
          view,
          league: "il",
          side: "offense",
          display: "per_game",
        }),
      );
      expectSame("team_ratings", "il", view, r, cells(html));
    });

  it("player_stats", () => {
    const r = fill(Object.keys(displaySpec("player_stats", "il", null)));
    const html = renderToStaticMarkup(
      createElement(PlayerStatsTable, { rows: [r], league: "il", mode: "Per Game" }),
    );
    expectSame("player_stats", "il", null, r, cells(html));
  });

  for (const view of ["summary", "four_factors"] as const)
    it(`game_logs ${view}`, () => {
      const r = fill(Object.keys(displaySpec("game_logs", "il", view)));
      r.game_date = "2026-03-01";
      const page = Object.fromEntries(
        columnsFor("il", view, null).map((c) => [
          c.id,
          c.render ? c.render(r).text : String(r[c.id] ?? ""),
        ]),
      );
      expectSame("game_logs", "il", view, r, page);
    });

  it("onoff summary", () => {
    const r = fill(Object.keys(displaySpec("onoff", "il", "summary")));
    const ctx = { onTip: () => {}, onPivot: () => {}, showImpact: true, showRanges: false };
    const page = Object.fromEntries(
      summaryColumns([r], ctx).map((c) => [
        c.id,
        c.render ? c.render(r).text : String(r[c.id] ?? ""),
      ]),
    );
    expectSame("onoff", "il", "summary", r, page);
  });

  // Review 2026-10-25: On/Off Four Factors and Shot Profile had no quotable
  // columns, and Summary's Off/Def Shot were not quotable.
  const onoffCtx = { onTip: () => {}, onPivot: () => {}, showImpact: false, showRanges: true };
  it("onoff summary Off/Def Shot: the cell's 2PT% | 3PT% line", () => {
    const r: Row = {
      ...sample,
      "Off Shot ON": 120, off_on_fg2_made: 40, off_on_fg2_att: 77, off_on_fg3_made: 15, off_on_fg3_att: 43,
      "Def Shot OFF": 0, def_off_fg2_made: 0, def_off_fg2_att: 0, def_off_fg3_made: 0, def_off_fg3_att: 0,
    };
    const page = Object.fromEntries(summaryColumns([r], onoffCtx).map((c) => [c.id, c.render?.(r).text ?? ""]));
    const on = cellText("onoff", "il", "summary", "Off Shot ON", row(r))!;
    expect(on).toBe("52% | 35%");
    expect(page["Off Shot ON"].startsWith(on)).toBe(true);
    expect(cellText("onoff", "il", "summary", "Def Shot OFF", row(r))).toBe(page["Def Shot OFF"]);
  });

  for (const [view, columns] of [
    ["four_factors", ffColumns],
    ["shot_profile", spColumns],
  ] as const)
    it(`onoff ${view}: diffs equal the cell, on/off equal the lines inside it`, () => {
      const keys = Object.keys(displaySpec("onoff", "il", view));
      expect(keys.length).toBeGreaterThan(20);
      const r = fill(keys);
      // A Shot Profile diff with a missing part shows "--" and no lines.
      if (view === "shot_profile") r[`off_on_${SP_SUFFIX[0]}`] = null;
      const cols = columns(onoffCtx);
      const parents = view === "four_factors" ? { ...FF_RTG_MAP, ...FF_METRIC_MAP } : null;
      for (const c of cols) {
        if (!keys.includes(c.id)) continue;
        const cell = c.render ? c.render(r) : { text: String(r[c.id] ?? ""), node: null };
        expect(cellText("onoff", "il", view, c.id, row(r)), `${view}/${c.id}`).toBe(cell.text);
        const html = renderToStaticMarkup(createElement("div", null, cell.node));
        const lines =
          view === "four_factors"
            ? (parents![c.id] ?? [])
            : c.id.endsWith(" Diff") && c.id !== "Net Diff"
              ? (() => {
                  const s = c.id.startsWith("Off") ? "off" : "def",
                    m = SP_SUFFIX[cols.filter((x) => x.id.startsWith(c.id.slice(0, 3)) && x.id.endsWith(" Diff")).findIndex((x) => x.id === c.id)];
                  return [`${s}_on_${m}`, `${s}_off_${m}`];
                })()
              : [];
        for (const k of lines) {
          const t = cellText("onoff", "il", view, k, row(r))!;
          if (t !== "--" && t !== "-") expect(html, `${view}/${c.id} shows ${k}=${t}`).toContain(t);
        }
      }
    });

  it("compare and team_overview use the formatters their pages call", () => {
    const cmp = readFileSync(path.resolve("src/components/CompareClient.tsx"), "utf8");
    expect(cmp).toContain('numeric("metric_a", labels[0])');
    expect(cmp).toContain('numeric("metric_b", labels[1])');
    expect(cmp).toContain('numeric("gap", "Gap", true)');
    expect(cmp).toContain("text: String(Math.trunc(num(r.poss_a)))");
    expect(cmp).toContain('text: formatCompare(r[id], "num", signed)');
    const hub = readFileSync(path.resolve("src/components/shiny/HomeHub.tsx"), "utf8");
    expect(hub).toContain('homeFixed(p["Net RTG Diff"], true)');
    expect(hub).toContain("homeOrdinal(rank)");
    const c = (key: string, v: unknown) => cellText("compare", "il", "Teams", key, row({ [key]: v } as Row));
    expect([c("gap", 2.26), c("gap", -0.04), c("metric_a", null), c("poss_a", 99.9)]).toEqual([
      "+2.3",
      "-0.0",
      "--",
      "99",
    ]);
    const h = (key: string, r: Row) => cellText("team_overview", "il", null, key, row(r));
    expect(h("Net RTG Diff", { section: "key_player", "Net RTG Diff": 3.26 })).toBe("+3.3");
    expect(h("rank_net_rtg", { section: "identity", rank_net_rtg: 2 })).toBe("2nd");
    expect(hub).toContain("stat(t.net, row.net_rtg, row.rank_net_rtg, true)");
  });

  it("compare detail and team players show the page's text", () => {
    const d = (key: string, r: Row) => cellText("compare", "il", "detail", key, row(r));
    expect(d("a", { a: 12.04, fmt: "net" })).toBe("+12.0");
    expect(d("b", { b: 55.26, fmt: "pct" })).toBe("55.3%");
    expect(d("gap", { gap: 4.26, fmt: "pct" })).toBe("+4.3%");
    expect(d("gap", { gap: 0, fmt: "rtg" })).toBe("+/-0.0");
    expect(d("gap", { gap: null, fmt: "pct" })).toBe("--");
    expect(d("label", { label: "eFG%" })).toBe("eFG%");
    const p = (key: string, r: Row) => cellText("compare", "il", "players", key, row(r));
    expect(p("metric_a", { metric_a: 20, played_a: true, pct: false })).toBe("20.0");
    expect(p("metric_b", { metric_b: 0, played_b: false, pct: false })).toBe("0.0 (didn't play)");
    expect(p("gap", { gap: 4.26, pct: true })).toBe("+4.3%");
    expect(p("gp_b", { gp_b: 5, played_b: false })).toBe("0");
    expect(p("player_name", { player_name: "P. One" })).toBe("P. One");
  });

  it("columns outside the spec are not quotable", () => {
    expect(cellText("lineups", "il", "summary", "Off Shot", row(sample))).toBeNull();
    expect(cellText("gameflow", "il", null, "anything", row(sample))).toBeNull();
  });
});
