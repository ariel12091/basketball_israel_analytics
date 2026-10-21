"use client";
// The Game Logs table (server_tab4.R / server_tab11_euro_gamelogs.R): dom
// "Btip", 50 rows a page, order date desc then GN/round desc, no orderSequence
// (ascending first), heat from styleInterval(CUTS, ...) on pr_* columns,
// formatRound / formatCurrency, make_shot_render_gl() cells (min 10 FGA), and
// csv_export_button("game_logs_*" / "euroleague_game_logs_*").
import { useMemo } from "react";
import ui from "@/data/shinyUi.json";
import type { League, Row } from "@/rules/gameLogs";
import DataTable, {
  formatNumber,
  heat,
  shotCell,
  type Column,
  type Group,
} from "./DataTable";

type View = "summary" | "four_factors";
// GL_HEAT_REVERSE_SUMMARY / GL_HEAT_REVERSE_FF in svc_game_logs.R.
const HEAT_REVERSE: Record<View, Record<string, boolean>> = {
  summary: { off_ppp: false, def_ppp: true },
  four_factors: {
    off_ppp: false,
    off_efg_pct: false,
    off_oreb_pct: false,
    off_tov_pct: true,
    off_ftr_pct: false,
    def_ppp: true,
    def_efg_pct: true,
    def_oreb_pct: true,
    def_tov_pct: false,
    def_ftr_pct: true,
  },
};
const ROUND1 = ui.round1 as Record<View, string[]>;
const SEP = "section-left-border";
const num = (r: Row, k: string) => Number(r[k] ?? 0) || 0;

// gamelog_summary_header() / gamelog_ff_header() with show_gameflow = TRUE.
export function columnsFor(
  league: League,
  view: View,
  shotAverages: Record<string, Record<string, number>> | null,
): Column[] {
  const euro = league !== "il";
  const reverse = HEAT_REVERSE[view];
  const value =
    (id: string): Column["render"] =>
    (row) => {
      const v = row[id];
      const text =
        typeof v !== "number"
          ? v == null
            ? ""
            : String(v)
          : id === "minutes" || ROUND1[view].includes(id)
            ? formatNumber(v, 1)
            : id === "off_poss" || id === "def_poss"
              ? formatNumber(v, 0)
              : String(v);
      const pr =
        id in reverse
          ? row[`pr_${id.replace(/[^A-Za-z0-9]+/g, "_")}`]
          : undefined;
      return {
        node: text,
        text,
        bg: id in reverse ? heat(pr, reverse[id]) : undefined,
      };
    };
  const col = (
    id: string,
    label: string,
    extra: Partial<Column> = {},
  ): Column => ({ id, label, render: value(id), ...extra });
  const dateKey = (r: Row) =>
    Number(
      String(r.game_date ?? "")
        .replace(/\D/g, "")
        .slice(0, 8),
    ) || 0;
  const shot =
    (side: "off" | "def"): Column["render"] =>
    (row) =>
      shotCell(
        {
          fg2m: num(row, `${side}_fg2m`),
          fg2a: num(row, `${side}_fg2a`),
          fg3m: num(row, `${side}_fg3m`),
          fg3a: num(row, `${side}_fg3a`),
        },
        {
          defense: side === "def",
          minFga: 10,
          avg2: shotAverages?.[side]?.fg2 ?? 53,
          avg3: shotAverages?.[side]?.fg3 ?? 34,
          palette: "gamelogs",
        },
      );
  const lead: Column[] = [
    col(euro ? "round_number" : "gn", euro ? "Rd" : "GN"),
    col(euro ? "phase_label" : "game_type_label", euro ? "Phase" : "Game Type"),
    col("game_date", "Date", { sortValue: dateKey }),
    {
      id: "gameflow",
      label: "Gameflow",
      sortValue: dateKey,
      // GL-06 (Gameflow) is a later migration phase: the link is shown, inert.
      render: (row) => ({
        node: (
          <a
            href="#"
            className="ribbon-link"
            aria-disabled="true"
            title="Gameflow is not yet available in the new app"
            data-game-id={String(row.game_id ?? "")}
            data-team-id={String(row.team_id ?? "")}
            onClick={(e) => e.preventDefault()}
          >
            View
          </a>
        ),
        text: "View",
      }),
    },
    col("team_name", "Team"),
    col("opp_team_name", "Opponent"),
    {
      id: "result",
      label: "W/L",
      render: (row) => {
        const v = String(row.result ?? "");
        return {
          node: (
            <span
              style={{
                fontWeight: 700,
                color: v === "W" ? "var(--ibpl-pos)" : "var(--ibpl-neg)",
              }}
            >
              {v}
            </span>
          ),
          text: v,
        };
      },
    },
    col(euro ? "score" : "score_display", "Score"),
    col("minutes", "Min"),
  ];
  const usage = [
    col("off_poss", "Off Poss", { cls: SEP }),
    col("def_poss", "Def Poss"),
  ];
  if (view === "summary")
    return [
      ...lead,
      col("off_ppp", "Off PPP", { cls: SEP }),
      col("def_ppp", "Def PPP"),
      col("net_rtg", "Net"),
      ...(euro
        ? []
        : [
            {
              id: "Off Shot",
              label: "Off Shot",
              cls: SEP,
              render: shot("off"),
            },
            { id: "Def Shot", label: "Def Shot", render: shot("def") },
          ]),
      ...usage,
    ];
  const side = (s: "off" | "def"): Column[] => [
    col(`${s}_ppp`, "PPP", { cls: SEP }),
    col(`${s}_efg_pct`, "eFG%"),
    col(`${s}_oreb_pct`, "OREB%", {
      title: s === "off" ? ui.offOrebTooltip : ui.defOrebTooltip,
    }),
    col(`${s}_tov_pct`, "TOV%"),
    col(`${s}_ftr_pct`, "FTR"),
  ];
  return [
    ...lead,
    col("net_rtg", "Net"),
    ...side("off"),
    ...side("def"),
    ...usage,
  ];
}

const FF_GROUPS: Group[] = [
  { label: "", span: 10 },
  { label: "Offense", span: 5, cls: SEP },
  { label: "Defense", span: 5, cls: SEP },
  { label: "Usage", span: 2, cls: SEP },
];

export default function GameLogsTable({
  league,
  view,
  rows,
  shotAverages,
}: {
  league: League;
  view: View;
  rows: Row[];
  shotAverages: Record<string, Record<string, number>> | null;
}) {
  const columns = useMemo(
    () => columnsFor(league, view, shotAverages),
    [league, view, shotAverages],
  );
  const order = useMemo(
    () => [
      { id: "game_date", desc: true },
      { id: league === "il" ? "gn" : "round_number", desc: true },
    ],
    [league],
  );
  return (
    <DataTable
      columns={columns}
      rows={rows}
      groups={view === "four_factors" ? FF_GROUPS : undefined}
      defaultOrder={order}
      pageLength={50}
      csvName={`${league === "il" ? "game_logs" : "euroleague_game_logs"}_${view}`}
      nameColumns={["team_name"]}
      rowProps={(row) => ({
        "data-game-id": String(row.game_id ?? ""),
        "data-team-id": String(row.team_id ?? ""),
      })}
    />
  );
}
