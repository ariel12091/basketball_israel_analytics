"use client";
import type { ReactNode } from "react";
import DataTable, {
  formatNumber,
  heat,
  shotCell,
  type Column,
} from "./DataTable";
import type { Row } from "@/rules/gameLogs";
import type { LineupView } from "@/rules/lineups";
import { rate, zero } from "@/rules/lineups";
import { rRound } from "@/rules/round";
import ui from "@/data/shinyUi.json";
import MoreInfo from "./MoreInfo";
export default function LineupTable({
  rows,
  view,
  onLineup,
  detail,
}: {
  rows: Row[];
  view: LineupView;
  onLineup: (r: Row) => void;
  /** The open lineup's game log, shown in place beneath its row. */
  detail?: (r: Row) => ReactNode;
}) {
  const ff = view === "four_factors";
  const cols: Column[] = [
    { id: "Team", label: "Team" },
    {
      id: "Players",
      label: "Players",
      render: (r) => ({
        node:
          r.is_total === 0 ? (
            String(r.Players)
          ) : (
            <button
              type="button"
              className="ibpl-lineup-link"
              onClick={() => onLineup(r)}
            >
              {r.Players}
            </button>
          ),
        text: String(r.Players ?? ""),
      }),
    },
  ];
  const number = (
    id: string,
    label: string,
    digits: number,
    pr?: string,
    border = false,
  ) =>
    cols.push({
      id,
      label,
      title:
        id === "off_oreb"
          ? ui.offOrebTooltip
          : id === "def_oreb"
            ? ui.defOrebTooltip
            : undefined,
      cls: border ? "section-left-border" : undefined,
      render: (r) => ({
        node: formatNumber(r[id], digits),
        text: formatNumber(r[id], digits),
        bg: r.is_total === 0 ? undefined : pr ? heat(r[pr], false) : undefined,
      }),
    });
  const shot = (side: string) => {
    const eligible = rows.filter(
      (r) => zero(r[side === "off" ? "Off Shot" : "Def Shot"]) >= 50,
    );
    const sum = (col: string) =>
      eligible.reduce((a, r) => a + zero(r[`${side}_${col}`]), 0);
    const avg2 = rRound(rate(sum("fg2_made"), sum("fg2_att")) ?? 53, 0),
      avg3 = rRound(rate(sum("fg3_made"), sum("fg3_att")) ?? 34, 0);
    cols.push({
      id: side === "off" ? "Off Shot" : "Def Shot",
      label: side === "off" ? "Off Shot" : "Def Shot",
      render: (r) =>
        shotCell(
          {
            fg2m: zero(r[`${side}_fg2_made`]),
            fg2a: zero(r[`${side}_fg2_att`]),
            fg3m: zero(r[`${side}_fg3_made`]),
            fg3a: zero(r[`${side}_fg3_att`]),
          },
          {
            defense: side === "def",
            minFga: 50,
            avg2,
            avg3,
            palette: "onoff",
            layout: true,
          },
        ),
    });
  };
  if (ff) {
    // How much data stands behind the row, right after the names.
    number("minutes", "Min", 1, undefined, true);
    number("total_poss", "Poss", 0);
    number("net_rtg", "Net", 1, "pr_net");
    for (const side of ["off", "def"]) {
      for (const [m, l] of [
        ["ppp", "PPP"],
        ["efg", "eFG%"],
        ["oreb", "OREB%"],
        ["tov", "TOV%"],
        ["ftr", "FTR"],
      ])
        number(`${side}_${m}`, l, 1, `pr_${side}_${m}`, m === "ppp");
      number(`${side}_poss`, "Poss", 0);
    }
  } else {
    number("minutes", "Min", 1);
    number("total_poss", "Total Poss", 0);
    number("off_ppp", "Off PPP", 1, "pr_ld_off_ppp");
    number("def_ppp", "Def PPP", 1, "pr_ld_def_ppp_i");
    number("net_rtg", "Net RTG", 1, "pr_ld_net");
    // helpers.R lineup_summary_datatable: the 3PT luck tag beside Net RTG.
    const net = cols[cols.length - 1],
      plain = net.render!;
    net.render = (r) => {
      const cell = plain(r);
      return r.is_total === 0 || r.luck_3pt_flag !== true
        ? cell
        : { ...cell, node: <LuckNet row={r} value={cell.node} /> };
    };
    number("plus_minus", "+/-", 0);
    shot("off");
    shot("def");
    for (const [id, l] of [
      ["off_poss", "Off Poss"],
      ["off_pts", "Off Pts"],
      ["def_poss", "Def Poss"],
      ["def_pts", "Def Pts"],
      ["num_starters", "# Starters"],
    ])
      number(id, l, 0);
  }
  return (
    <DataTable
      columns={cols}
      rows={rows}
      defaultOrder={[]}
      pinTotals
      nameColumns={["Players", "Team"]}
      lengthMenu={ff ? undefined : [25, 50, 100, 200, 1000]}
      groups={
        ff
          ? [
              { label: "", span: 2 },
              { label: "Usage", span: 3, cls: "section-left-border" },
              { label: "Offense", span: 6, cls: "section-left-border" },
              { label: "Defense", span: 6, cls: "section-left-border" },
            ]
          : undefined
      }
      rowProps={(r) => ({
        className: r.is_total === 0 ? "ibpl-lineup-total" : "",
      })}
      moreInfo
      detail={detail}
    />
  );
}

const signed = (v: number) => (v > 0 ? "+" : "") + v.toFixed(1);
/** Net RTG with its 3PT luck tag; the tag opens what the luck added and the
 * net without it (the On/Off tag's popover sections). */
function LuckNet({ row, value }: { row: Row; value: ReactNode }) {
  const o = Number(row.luck_3pt_ours),
    t = Number(row.luck_3pt_theirs),
    adj = Number(row.net_without_3pt_luck);
  return (
    <span className="onoff-net-tip">
      {value}{" "}
      <MoreInfo label="3PT luck" className="onoff-luck-tag">
        <div className="onoff-tip-pop" data-kind="luck">
          <div className="onoff-tip-section is-luck">
            <div className="onoff-tip-title">3PT luck</div>
            <div className="onoff-tip-row">
              <span>3PT shooting added</span>
              <b className="onoff-tip-val">{signed(o + t)}</b>
            </div>
            <div className="onoff-tip-sub">
              lineup {signed(o)} · opponents {signed(t)}
            </div>
            <div className="onoff-tip-row">
              <span>Without 3PT luck</span>
              <b className="onoff-tip-val">{signed(adj)}</b>
            </div>
          </div>
        </div>
      </MoreInfo>
    </span>
  );
}
