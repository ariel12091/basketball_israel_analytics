"use client";
// A lineup's game log, opened in place beneath its row in the Lineups table
// (user 2026-10-28: no modal windows). Same columns and states as the former
// LineupModal; Close (or Escape) folds it away and returns focus to the
// lineup's link.
import { useEffect, useRef } from "react";
import type { League, Row } from "@/rules/gameLogs";
import type { LineupView } from "@/rules/lineups";
import { rate, zero } from "@/rules/lineups";
import { rRound } from "@/rules/round";
import DataTable, { formatNumber, shotCell, type Column } from "./DataTable";
import { useTabData } from "./TabKit";
export default function LineupGameLog({
  league,
  season,
  row,
  view,
  onClose,
}: {
  league: League;
  season: string;
  row: Row;
  view: LineupView;
  onClose: () => void;
}) {
  const url = `/api/v1/${league}/lineup-games?${new URLSearchParams({ season, team: String(row.team_id), hash: String(row.sub_lineup_hash), view })}`;
  const { data, error, loading, retry } = useTabData<{
    rows: Row[];
    meta: { version: string };
  }>(url);
  const root = useRef<HTMLElement>(null);
  useEffect(() => {
    root.current?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, []);
  const euro = league !== "il",
    ff = !euro && view === "four_factors",
    rows = data?.rows ?? [];
  const cols: Column[] = [];
  const number = (id: string, label: string, digits = 1) =>
    cols.push({
      id,
      label,
      render: (r) => ({
        node: formatNumber(r[id], digits),
        text: formatNumber(r[id], digits),
      }),
    });
  if (euro)
    cols.push(
      { id: "game_date", label: "Date" },
      { id: "round_number", label: "Rd" },
      { id: "opp_team_name", label: "Opponent" },
      { id: "venue", label: "H/A" },
    );
  else
    cols.push(
      { id: "gn", label: "GN" },
      { id: "game_date", label: "Date" },
      { id: "opp_team_name", label: "Opponent" },
      {
        id: "result",
        label: "W/L",
        render: (r) => ({
          node: (
            <span
              style={{
                fontWeight: 700,
                color: r.result === "W" ? "var(--ibpl-pos)" : "var(--ibpl-neg)",
              }}
            >
              {r.result}
            </span>
          ),
          text: String(r.result),
        }),
      },
      { id: "score_display", label: "Score" },
    );
  const shot = (side: string) => {
    const sum = (k: string) =>
      rows.reduce((a, r) => a + zero(r[`${side}_${k}`]), 0);
    cols.push({
      id: `${side}_shot`,
      label: side === "off" ? "Off Shot" : "Def Shot",
      render: (r) =>
        shotCell(
          {
            fg2m: zero(r[`${side}_fg2m`]),
            fg2a: zero(r[`${side}_fg2a`]),
            fg3m: zero(r[`${side}_fg3m`]),
            fg3a: zero(r[`${side}_fg3a`]),
          },
          {
            defense: side === "def",
            minFga: 10,
            avg2: rRound(rate(sum("fg2m"), sum("fg2a")) ?? 53, 0),
            avg3: rRound(rate(sum("fg3m"), sum("fg3a")) ?? 34, 0),
            palette: "gamelogs",
            layout: true,
          },
        ),
    });
  };
  if (ff) {
    for (const side of ["off", "def"]) {
      for (const [m, l] of [
        ["ppp", "PPP"],
        ["efg", "eFG%"],
        ["oreb_pct", "OREB%"],
        ["tov_pct", "TOV%"],
        ["ftr", "FTR"],
      ])
        number(`${side}_${m}`, l);
      number(`${side}_poss`, "Poss", 0);
    }
    number("minutes", "Min");
  } else {
    if (euro) {
      number("minutes", "Min");
      number("total_poss", "Total Poss", 0);
    }
    number("off_ppp", "Off PPP");
    number("def_ppp", "Def PPP");
    number("net_rtg", "Net");
    shot("off");
    shot("def");
    number("off_poss", "Off Poss", 0);
    number("def_poss", "Def Poss", 0);
    if (!euro) number("minutes", "Min");
  }
  number("off_pts", "Off Pts", 0);
  number("def_pts", "Def Pts", 0);
  number("plus_minus", "+/-", 0);
  return (
    <section
      ref={root}
      className="ibpl-lineup-games"
      aria-labelledby="lineup-games-title"
      onKeyDown={(e) => {
        if (e.key === "Escape") onClose();
      }}
    >
      <header className="ibpl-lineup-games-head">
        <h4 id="lineup-games-title">
          {euro ? "Lineup game log" : String(row.Players)}
        </h4>
        <button
          type="button"
          className="filter-drawer-close"
          aria-label="Close game log"
          onClick={onClose}
        >
          <span aria-hidden="true">×</span> Close
        </button>
      </header>
      {loading ? (
        <p role="status">Loading game log…</p>
      ) : error ? (
        <div role="alert">
          {error}{" "}
          <button type="button" onClick={retry}>
            Retry
          </button>
        </div>
      ) : !rows.length ? (
        <p>No games found for this lineup.</p>
      ) : (
        <DataTable
          rows={rows}
          columns={cols}
          defaultOrder={euro ? [] : [{ id: "gn", desc: false }]}
          pageLength={euro ? 25 : 50}
          controls={!euro}
          nameColumns={["opp_team_name"]}
          groups={
            ff
              ? [
                  { label: "", span: 5 },
                  { label: "Offense", span: 6 },
                  { label: "Defense", span: 6 },
                  { label: "", span: 4 },
                ]
              : undefined
          }
        />
      )}
    </section>
  );
}
