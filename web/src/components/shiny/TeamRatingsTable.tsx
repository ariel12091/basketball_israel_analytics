"use client";
import { useMemo, useState, useSyncExternalStore } from "react";
import DataTable, {
  formatNumber,
  heat,
  type Column,
  type Group,
} from "./DataTable";
import type { Row, League } from "@/rules/gameLogs";
import { type TeamView } from "@/rules/teamRatings";
import { SP_SUFFIX, SP_LABELS } from "@/rules/onoff";
import { rRound } from "@/rules/round";
import ui from "@/data/shinyUi.json";
const TRAD_LABELS: Record<string, string> = {
  pts: "PTS",
  reb: "REB",
  oreb: "OREB",
  dreb: "DREB",
  ast: "AST",
  stl: "STL",
  blk: "BLK",
  dfl: "DFL",
  tov: "TOV",
  fgm: "FGM",
  fga: "FGA",
  fg_pct: "FG%",
  "2pm": "2PM",
  "2pa": "2PA",
  two_pct: "2P%",
  "3pm": "3PM",
  "3pa": "3PA",
  tp_pct: "3P%",
  ftm: "FTM",
  fta: "FTA",
  ft_pct: "FT%",
  efg: "eFG%",
  ts: "TS%",
};
const TRAD_ORDER = [
  "pts",
  "reb",
  "oreb",
  "dreb",
  "ast",
  "stl",
  "blk",
  "dfl",
  "tov",
  "fgm",
  "fga",
  "fg_pct",
  "2pm",
  "2pa",
  "two_pct",
  "3pm",
  "3pa",
  "tp_pct",
  "ftm",
  "fta",
  "ft_pct",
  "efg",
  "ts",
];
const ORDER_KEY = "onoff.traditional_team.visible_col_order.v1";
const EMPTY_ORDER: string[] = [];
let orderRaw: string | null = null,
  orderSnapshot = EMPTY_ORDER;
function readOrder() {
  try {
    const raw = localStorage.getItem(ORDER_KEY);
    if (raw !== orderRaw) {
      orderRaw = raw;
      const value = JSON.parse(raw ?? "[]");
      orderSnapshot = Array.isArray(value)
        ? [...new Set(value.filter((v): v is string => typeof v === "string"))]
        : EMPTY_ORDER;
    }
  } catch {}
  return orderSnapshot;
}
function subscribeOrder(notify: () => void) {
  window.addEventListener("storage", notify);
  window.addEventListener("ibpl-team-column-order", notify);
  return () => {
    window.removeEventListener("storage", notify);
    window.removeEventListener("ibpl-team-column-order", notify);
  };
}
function saveOrder(ids: string[]) {
  try {
    localStorage.setItem(ORDER_KEY, JSON.stringify(ids));
    orderRaw = localStorage.getItem(ORDER_KEY);
  } catch {}
  orderSnapshot = ids;
  window.dispatchEvent(new Event("ibpl-team-column-order"));
}
export default function TeamRatingsTable({
  rows,
  view,
  league,
  side,
  display,
}: {
  rows: Row[];
  view: TeamView;
  league: League;
  side: string;
  display: string;
}) {
  const saved = useSyncExternalStore(
    subscribeOrder,
    readOrder,
    () => EMPTY_ORDER,
  );
  const [moving, setMoving] = useState("team_name");
  const columns = useMemo(() => {
    const cols: Column[] = [];
    const plain = (id: string, label: string, digits = 0) =>
      cols.push({
        id,
        label,
        render: (r) => ({
          node: formatNumber(r[id], digits),
          text: formatNumber(r[id], digits),
        }),
      });
    const metric = (id: string, label: string, asc = false, border = false) => {
      const maxRank = Math.max(
        2,
        ...rows.flatMap((r) =>
          ["off_ppp", "def_ppp", "net_rtg"].map((m) =>
            Number(r[`rank_${m}`] ?? 0),
          ),
        ),
      );
      cols.push({
        id,
        label,
        descFirst: !asc,
        cls: border ? "section-left-border" : undefined,
        title:
          id === "off_oreb"
            ? ui.offOrebTooltip
            : id === "def_oreb"
              ? ui.defOrebTooltip
              : undefined,
        sortValue: (r) =>
          r[id] == null ? (asc ? Infinity : -Infinity) : Number(r[id]),
        render: (r) => {
          const v = r[id],
            rk = r[`rank_${id}`],
            delta = r[`delta_${id}`],
            shot = view === "shot_profile";
          const value =
            v == null
              ? shot
                ? "--"
                : "NA"
              : rRound(Number(v), 1).toFixed(1) + (shot ? "%" : "");
          const arrow =
            delta == null
              ? "—"
              : Number(delta) > 0
                ? `▲${delta}`
                : Number(delta) < 0
                  ? `▼${Math.abs(Number(delta))}`
                  : "↔";
          const text =
            shot && v == null
              ? "--"
              : `${value}\n#${rk ?? "NA"}${shot ? "" : `\n${arrow}`}`;
          const bg =
            view === "summary"
              ? rk == null
                ? undefined
                : (ui.teamRankColors as Record<string, string[]>)[
                    String(maxRank)
                  ]?.[Number(rk) - 1]
              : heat(r[`pr_${id === "net_rtg" ? "net" : id}`], false);
          return {
            node:
              shot && v == null ? (
                "--"
              ) : (
                <>
                  {value}
                  <br />#{rk ?? "NA"}
                  {!shot && (
                    <>
                      <br />
                      {arrow}
                    </>
                  )}
                </>
              ),
            text,
            bg,
          };
        },
      });
    };
    if (view === "summary") cols.push({ id: "game_year", label: "Season" });
    cols.push({ id: "team_name", label: "Team" });
    if (view === "traditional") {
      plain("gp", "GP", 1);
      plain("poss_on_floor", "Poss On Floor", 1);
      plain("minutes", "Min", 1);
      for (const m of TRAD_ORDER)
        metric(
          m,
          TRAD_LABELS[m],
          m === "tov" ? side !== "defense" : side === "defense",
        );
    } else if (view === "summary") {
      plain("games_played", "GP");
      plain("minutes", "Min", 1);
      plain("wins", "W");
      plain("losses", "L");
      metric("off_ppp", "Off PPP");
      metric("def_ppp", "Def PPP", true);
      metric("net_rtg", "Net Rtg");
      plain("off_pace", "Off Pace", 1);
      plain("def_pace", "Def Pace", 1);
      plain("off_poss", "Off Poss");
      plain("def_poss", "Def Poss");
    } else {
      plain("minutes", "Min", 1);
      for (const s of ["off", "def"]) {
        if (view === "shot_profile")
          SP_SUFFIX.forEach((m, i) =>
            metric(
              `${s}_${m}`,
              i === 5 ? "Corner 3 Share" : SP_LABELS[i],
              false,
              i === 0,
            ),
          );
        else
          for (const [i, m] of ["ppp", "efg", "oreb", "tov", "ftr"].entries())
            metric(
              `${s}_${m}`,
              ["PPP", "eFG%", "OREB%", "TOV%", "FTR"][i],
              s === "off" ? m === "tov" : m !== "tov",
              i === 0,
            );
        plain(`${s}_poss`, "Poss");
      }
      if (view === "four_factors") metric("net_rtg", "Net", false, true);
    }
    return cols;
  }, [rows, view, side]);
  const ordered =
    view === "traditional"
      ? [
          ...saved
            .map((id) => columns.find((c) => c.id === id))
            .filter((c): c is Column => !!c),
          ...columns.filter((c) => !saved.includes(c.id)),
        ]
      : columns;
  const move = (step: number) => {
    const ids = ordered.map((c) => c.id),
      i = ids.indexOf(moving),
      next = i + step;
    if (i < 0 || next < 0 || next >= ids.length) return;
    [ids[i], ids[next]] = [ids[next], ids[i]];
    saveOrder(ids);
  };
  const moveTo = (from: string, to: string) => {
    const ids = ordered.map((c) => c.id),
      source = ids.indexOf(from),
      target = ids.indexOf(to);
    if (source < 0 || target < 0 || source === target) return;
    ids.splice(source, 1);
    ids.splice(target, 0, from);
    saveOrder(ids);
  };
  const groups: Group[] | undefined =
    view === "four_factors"
      ? [
          { label: "", span: 2 },
          { label: "Offense", span: 6 },
          { label: "Defense", span: 6 },
          { label: "", span: 1 },
        ]
      : view === "shot_profile"
        ? [
            { label: "", span: 2 },
            { label: "Offense Shot Profile (eFG% + shares of FGA)", span: 8 },
            { label: "Defense Shot Profile (eFG% + shares of FGA)", span: 8 },
          ]
        : undefined;
  const csv =
    league !== "il" && view === "summary"
      ? "euroleague_team_ratings"
      : `${league === "il" ? "team_ratings" : "euroleague_team"}_${view}${view === "traditional" ? `_${side === "defense" ? "opponent" : "team"}_${display === "per75" ? "per_75_possessions" : display === "per40" ? "per_40_minutes" : "per_game"}` : ""}`;
  return (
    <>
      {view === "traditional" && (
        <details className="ibpl-column-order">
          <summary>Column order</summary>
          <label>
            Column{" "}
            <select
              className="form-control"
              aria-label="Reorder column"
              value={moving}
              onChange={(e) => setMoving(e.target.value)}
            >
              {ordered.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>{" "}
          <button
            type="button"
            className="btn btn-default btn-sm"
            onClick={() => move(-1)}
          >
            Move left
          </button>{" "}
          <button
            type="button"
            className="btn btn-default btn-sm"
            onClick={() => move(1)}
          >
            Move right
          </button>
        </details>
      )}
      <DataTable
        rows={rows}
        columns={ordered}
        onColumnMove={view === "traditional" ? moveTo : undefined}
        groups={groups}
        defaultOrder={
          view === "four_factors"
            ? [{ id: "net_rtg", desc: true }]
            : view === "shot_profile"
              ? [{ id: "off_rim_share", desc: true }]
              : view === "traditional"
                ? [{ id: "team_name", desc: false }]
                : []
        }
        pageLength={50}
        csvName={csv}
        csvLabel={
          view === "traditional"
            ? `Download ${side === "defense" ? "Opponent" : "Team"} CSV`
            : "Download CSV"
        }
        nameColumns={["team_name"]}
      />
    </>
  );
}
