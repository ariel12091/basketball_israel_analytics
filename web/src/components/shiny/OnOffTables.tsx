"use client";
// The three On/Off tables (helpers.R onoff_summary_datatable(),
// onoff_four_factors_datatable(), server_tab1.R's Shot Profile branch) on the
// shared DataTable: their columns, grouped headers, cell renderers, heat
// polarity, default order and page sizes, plus the client behaviours app.js
// adds to them -- the Net cell's 3PT luck / sample-size tooltip and the row
// pivot menu (inventory ON-07..ON-11, ON-13, X-17).
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import ui from "@/data/shinyUi.json";
import type { League, Row } from "@/rules/gameLogs";
import {
  FF_METRIC_MAP,
  FF_RTG_MAP,
  SP_LABELS,
  SP_SUFFIX,
  SUMMARY_SHOT_MAP,
  summaryShotAverages,
} from "@/rules/onoff";
import DataTable, {
  formatNumber,
  heat,
  shotCell,
  type Cell,
  type Column,
  type Group,
} from "./DataTable";

const SEP = "section-left-border";
const num = (v: unknown) => (v == null || v === "" ? null : Number(v));
const signed = (v: unknown, digits: number) => {
  const n = num(v);
  if (n == null || Number.isNaN(n)) return v == null ? "" : String(v);
  return (n > 0 ? "+" : "") + n.toFixed(digits);
};

/** range_cell_js(): headline, the league-range track with the on and off
 * dots, the "on | off" pair beneath, and an optional extra line. */
function RangeCell({
  value,
  on,
  off,
  onPct,
  offPct,
  extra,
  details = true,
}: {
  value: string;
  on: string;
  off: string;
  onPct: number | null;
  offPct: number | null;
  extra?: ReactNode;
  details?: boolean;
}) {
  if (!details)
    return (
      <div className="diff-val" style={{ marginBottom: 0 }}>
        {value}
      </div>
    );
  if (onPct == null)
    return (
      <>
        <div className="diff-val unranked">{value}</div>
        <div className="rank-bar-container hidden"></div>
        <div className="sub-text" style={{ opacity: 0.5 }}>
          {on} | {off}
        </div>
        {extra}
      </>
    );
  const o = offPct ?? 0;
  return (
    <>
      <div className="diff-val">{value}</div>
      <div className="rank-bar-container">
        <div className="rank-track"></div>
        <div
          className="range-connect"
          style={{
            left: `${Math.min(onPct, o)}%`,
            width: `${Math.abs(onPct - o)}%`,
          }}
        ></div>
        <div
          className="dot-off"
          style={{ left: `${o}%` }}
          title={`Off: ${off}`}
        ></div>
        <div
          className="dot-on"
          style={{ left: `${onPct}%` }}
          title={`On: ${on}`}
        ></div>
      </div>
      <div className="sub-text">
        <span style={{ fontWeight: 700, color: "var(--ibpl-cell-text)" }}>
          {on}
        </span>{" "}
        <span style={{ opacity: 0.6 }}>|</span>{" "}
        <span
          style={{
            fontSize: "0.85em",
            opacity: 0.72,
            color: "var(--ibpl-cell-text-2)",
          }}
        >
          {off}
        </span>
      </div>
      {extra}
    </>
  );
}

// ---- Net cell tooltip (app.js "On/Off Net cell tooltip") ------------------
function TipVal({ v }: { v: number }) {
  return (
    <b className={`onoff-tip-val ${v > 0 ? "is-pos" : v < 0 ? "is-neg" : ""}`}>
      {(v > 0 ? "+" : "") + v.toFixed(1)}
    </b>
  );
}
export function TipBody({
  row,
  kind,
}: {
  row: Row;
  kind: "all" | "luck" | "sample";
}) {
  const net = Number(row["Net RTG Diff"]);
  const o = num(row.luck_3pt_ours),
    t = num(row.luck_3pt_theirs),
    p = num(row.net_padded);
  const on = num(row["ON Poss"]),
    off = num(row["OFF Poss"]);
  const flagged = row.sample_flag === true;
  return (
    <>
      {kind !== "sample" && o != null && t != null && (
        <div className="onoff-tip-section is-luck">
          <div className="onoff-tip-title">3PT luck</div>
          <div className="onoff-tip-row">
            <span>3PT shooting added</span>
            <TipVal v={o + t} />
          </div>
          <div className="onoff-tip-sub">
            team <TipVal v={o} /> · opponents <TipVal v={t} />
          </div>
          <div className="onoff-tip-row">
            <span>Without 3PT luck</span>
            <TipVal v={net - o - t} />
          </div>
        </div>
      )}
      {kind !== "luck" && p != null && (
        <div className="onoff-tip-section is-sample">
          <div className="onoff-tip-title">
            {flagged ? "Small sample" : "Sample size"}
          </div>
          {on != null && off != null && (
            <div className="onoff-tip-row">
              <span>Possessions</span>
              <b className="onoff-tip-val">
                {Math.round(on)} on · {Math.round(off)} off
              </b>
            </div>
          )}
          <div className="onoff-tip-row">
            <span>Adjusted for sample size</span>
            <TipVal v={p} />
          </div>
          {flagged && (
            <div className="onoff-tip-note">
              {on != null && off != null
                ? `Only ${Math.round(Math.min(on, off))} possessions ${on < off ? "on" : "off"} court, far fewer than most rows in this table.`
                : "Rests on far fewer possessions than most rows in this table."}
            </div>
          )}
        </div>
      )}
    </>
  );
}
type Tip = {
  row: Row;
  kind: "all" | "luck" | "sample";
  x: number;
  y: number;
  below?: DOMRect;
};
function NetTooltip({ tip }: { tip: Tip }) {
  const [box, setBox] = useState<{ w: number; h: number } | null>(null);
  const ref = (el: HTMLDivElement | null) => {
    if (el && (!box || box.w !== el.offsetWidth || box.h !== el.offsetHeight))
      setBox({ w: el.offsetWidth, h: el.offsetHeight });
  };
  let x = tip.x + 14,
    y = tip.y + 14;
  if (tip.below && box) {
    x = tip.below.left;
    y =
      tip.below.bottom + box.h + 8 <= innerHeight
        ? tip.below.bottom + 6
        : tip.below.top - box.h - 6;
  }
  const w = box?.w ?? 0,
    h = box?.h ?? 0;
  return (
    <div
      ref={ref}
      className="onoff-tip"
      role="tooltip"
      data-kind={tip.kind}
      style={{
        left: Math.max(8, Math.min(x, innerWidth - w - 8)),
        top: Math.max(8, Math.min(y, innerHeight - h - 8)),
      }}
    >
      <TipBody row={tip.row} kind={tip.kind} />
    </div>
  );
}
/** The Net RTG Diff cell: the number (+2 decimals), tags for 3PT luck and
 * small sample; hovering the number shows every section, a tag only its own. */
function NetCell({ row, onTip }: { row: Row; onTip: (t: Tip | null) => void }) {
  const val = Number(row["Net RTG Diff"]);
  const txt = (val > 0 ? "+" : "") + val.toFixed(2);
  const hasLuck =
    num(row.luck_3pt_ours) != null && num(row.luck_3pt_theirs) != null;
  const hasPad = num(row.net_padded) != null;
  if (!hasLuck && !hasPad) return <>{txt}</>;
  const show =
    (kind: Tip["kind"]) => (e: React.PointerEvent | React.MouseEvent) => {
      const touch =
        e.type === "click" || ("pointerType" in e && e.pointerType === "touch");
      onTip({
        row,
        kind,
        x: e.clientX,
        y: e.clientY,
        below: touch
          ? (e.currentTarget as HTMLElement)
              .closest(".onoff-net-tip")!
              .getBoundingClientRect()
          : undefined,
      });
    };
  const tag = (label: string, kind: "luck" | "sample") => (
    <span
      className="onoff-luck-tag"
      data-tip-kind={kind}
      onPointerMove={(e) => {
        e.stopPropagation();
        show(kind)(e);
      }}
      onClick={(e) => {
        e.stopPropagation();
        show(kind)(e);
      }}
      style={{
        display: "block",
        fontSize: 10,
        fontWeight: 600,
        lineHeight: 1.3,
        color: "var(--ibpl-cell-text)",
        whiteSpace: "nowrap",
      }}
    >
      {label}
    </span>
  );
  return (
    <span
      className="onoff-net-tip"
      data-tip-kind="all"
      onPointerMove={show("all")}
      onClick={show("all")}
      onPointerLeave={(e) => e.pointerType !== "touch" && onTip(null)}
    >
      {txt}
      {hasLuck && row.luck_3pt_flag === true && tag("3PT luck", "luck")}
      {hasPad && row.sample_flag === true && tag("small sample", "sample")}
    </span>
  );
}

// ---- Pivot menu (app.js "Pivot menu") ---------------------------------------
type Pivot = { row: Row; x: number; y: number; opener: HTMLElement };
function PivotMenu({
  pivot,
  league,
  season,
  onClose,
}: {
  pivot: Pivot;
  league: League;
  season: string;
  onClose: () => void;
}) {
  const router = useRouter();
  const team = String(pivot.row.team_id ?? ""),
    player = String(pivot.row.player_id ?? "");
  const teamKey = league === "il" ? "team" : "teams";
  // ACTIONS in app.js: each needs its entity and a destination tab.
  const items = [
    player && {
      label: "Lineups with this player",
      href: `/${league}/lineups?season=${season}&team=${team}&players_on=${player}`,
    },
    team && {
      label: "Lineups for this team",
      href: `/${league}/lineups?season=${season}&team=${team}`,
    },
    team && {
      label: "Game log for this team",
      href: `/${league}/game-logs?season=${season}&${teamKey}=${team}`,
    },
  ].filter(Boolean) as { label: string; href: string }[];
  useEffect(() => {
    const close = (e: Event) => {
      if (e.type === "keydown" && (e as KeyboardEvent).key !== "Escape") return;
      if (
        e.type === "click" &&
        (e.target as HTMLElement).closest(".ibpl-pivot-menu")
      )
        return;
      onClose();
      if (e.type === "keydown") pivot.opener.focus();
    };
    const timer = setTimeout(() => document.addEventListener("click", close));
    document.addEventListener("keydown", close);
    window.addEventListener("resize", onClose);
    window.addEventListener("scroll", onClose, true);
    return () => {
      clearTimeout(timer);
      document.removeEventListener("click", close);
      document.removeEventListener("keydown", close);
      window.removeEventListener("resize", onClose);
      window.removeEventListener("scroll", onClose, true);
    };
  }, [pivot, onClose]);
  return (
    <div
      className="ibpl-pivot-menu"
      role="menu"
      style={{
        position: "fixed",
        left: Math.min(pivot.x, innerWidth - 228),
        top: Math.min(pivot.y, innerHeight - 140),
      }}
    >
      {items.map((it, i) => (
        <button
          key={it.label}
          type="button"
          className="ibpl-pivot-item"
          role="menuitem"
          autoFocus={i === 0}
          onClick={() => {
            onClose();
            router.push(it.href);
          }}
        >
          {it.label}
        </button>
      ))}
    </div>
  );
}

// ---- Column builders --------------------------------------------------------
type Ctx = {
  onTip: (t: Tip | null) => void;
  onPivot: (p: Pivot) => void;
  showImpact: boolean;
  showRanges: boolean;
};
const plain =
  (
    id: string,
    digits: number | null,
    heatCol?: [string, boolean],
  ): Column["render"] =>
  (row) => {
    const text =
      digits == null
        ? row[id] == null
          ? ""
          : String(row[id])
        : formatNumber(row[id], digits);
    return {
      node: text,
      text,
      bg: heatCol ? heat(row[heatCol[0]], heatCol[1], "muted") : undefined,
    };
  };
const diff2 =
  (id: string, heatCol?: [string, boolean]): Column["render"] =>
  (row) => {
    const text = signed(row[id], 2);
    return {
      node: text,
      text,
      bg: heatCol ? heat(row[heatCol[0]], heatCol[1], "muted") : undefined,
    };
  };
function nameCols(ctx: Ctx): Column[] {
  const pivot: Partial<Column> = {
    tdProps: () => ({
      "data-pivot-trigger": "",
      tabIndex: 0,
      role: "button",
      "aria-haspopup": "menu",
    }),
    onActivate: (row, cell, at) =>
      ctx.onPivot({ row, x: at.x, y: at.y, opener: cell }),
  };
  return [
    { id: "Team", label: "Team", ...pivot },
    { id: "Player", label: "Player", ...pivot },
  ];
}
const usage = (first = SEP): Column[] => [
  { id: "minutes", label: "Min", cls: first, render: plain("minutes", 1) },
  { id: "ON Poss", label: "On Poss", render: plain("ON Poss", 0) },
  { id: "OFF Poss", label: "Off Poss", render: plain("OFF Poss", 0) },
];

export function summaryColumns(rows: Row[], ctx: Ctx): Column[] {
  const avgs = summaryShotAverages(rows);
  const shot = (dn: string): Column => {
    const [m2, a2, m3, a3] = SUMMARY_SHOT_MAP[dn];
    return {
      id: dn,
      label: dn.startsWith("Off") ? "Off Shot" : "Def Shot",
      render: (row) =>
        shotCell(
          {
            fg2m: Number(row[m2] ?? 0) || 0,
            fg2a: Number(row[a2] ?? 0) || 0,
            fg3m: Number(row[m3] ?? 0) || 0,
            fg3a: Number(row[a3] ?? 0) || 0,
          },
          {
            defense: dn.startsWith("Def"),
            minFga: 50,
            avg2: avgs[dn].fg2,
            avg3: avgs[dn].fg3,
            palette: "muted",
          },
        ),
    };
  };
  return [
    ...nameCols(ctx),
    {
      id: "Net RTG Diff",
      label: "Net",
      cls: SEP,
      render: (row): Cell => ({
        node:
          row["Net RTG Diff"] == null ? (
            ""
          ) : (
            <NetCell row={row} onTip={ctx.onTip} />
          ),
        text: signed(row["Net RTG Diff"], 2),
        bg: heat(row.pr_net, false, "muted"),
      }),
    },
    {
      id: "Off ON Diff",
      label: "Off",
      render: diff2("Off ON Diff", ["pr_off_on_d", false]),
    },
    {
      id: "Def ON Diff",
      label: "Def",
      render: diff2("Def ON Diff", ["pr_def_on_d", true]),
    },
    {
      id: "Off ON PPP",
      label: "Off PPP",
      cls: SEP,
      render: plain("Off ON PPP", 1, ["pr_off_on", false]),
    },
    {
      id: "Def ON PPP",
      label: "Def PPP",
      render: plain("Def ON PPP", 1, ["pr_def_on_inv", false]),
    },
    {
      id: "On Net RTG",
      label: "Net Rtg",
      render: diff2("On Net RTG", ["pr_on_net", false]),
    },
    shot("Off Shot ON"),
    shot("Def Shot ON"),
    {
      id: "Off OFF PPP",
      label: "Off PPP",
      cls: SEP,
      render: plain("Off OFF PPP", 1, ["pr_off_off", false]),
    },
    {
      id: "Def OFF PPP",
      label: "Def PPP",
      render: plain("Def OFF PPP", 1, ["pr_def_off_inv", false]),
    },
    {
      id: "Off Net RTG",
      label: "Net Rtg",
      render: diff2("Off Net RTG", ["pr_off_net", false]),
    },
    shot("Off Shot OFF"),
    shot("Def Shot OFF"),
    ...usage(),
  ];
}
export const SUMMARY_GROUPS: Group[] = [
  { label: "", span: 2 },
  { label: "Net Impact", span: 3, cls: SEP },
  { label: "On Court Stats", span: 5, cls: SEP },
  { label: "Off Court Stats", span: 5, cls: SEP },
  { label: "Usage", span: 3, cls: SEP },
];

const FACTOR: Record<string, "efg" | "oreb" | "tov" | "ftr"> = {
  "Off eFG% Diff": "efg",
  "Off OREB% Diff": "oreb",
  "Off TOV% Diff": "tov",
  "Off FTR Diff": "ftr",
  "Def eFG% Diff": "efg",
  "Def OREB% Diff": "oreb",
  "Def TOV% Diff": "tov",
  "Def FTR Diff": "ftr",
};
const FF_HEAT: Record<string, [string, boolean]> = {
  "Net Diff": ["pr_net_diff", false],
  "Off Rtg Diff": ["pr_off_rtg", false],
  "Def Rtg Diff": ["pr_def_rtg", true],
  "Off eFG% Diff": ["pr_diff_off_efg", false],
  "Off OREB% Diff": ["pr_diff_off_oreb", false],
  "Off FTR Diff": ["pr_diff_off_ftr", false],
  "Off TOV% Diff": ["pr_diff_off_tov", true],
  "Def eFG% Diff": ["pr_diff_def_efg", true],
  "Def OREB% Diff": ["pr_diff_def_oreb", true],
  "Def FTR Diff": ["pr_diff_def_ftr", true],
  "Def TOV% Diff": ["pr_diff_def_tov", false],
};
/** ff_diff_cell_js(): values as JavaScript prints them, "-" for empty or 0
 * (`row[i] || '-'`), the unit, and the est. +/-X pts line when shown. */
function ffCell(
  id: string,
  on: string,
  off: string,
  unit: string,
  impact: boolean,
  showRanges: boolean,
): Column["render"] {
  const js = (v: unknown) =>
    v == null || v === "" || Number(v) === 0 ? "-" : String(Number(v));
  return (row) => {
    const d = row[id];
    let value =
      d == null ? "-" : Number(d) > 0 ? `+${Number(d)}` : String(Number(d));
    if (value !== "-") value += unit;
    const onV = js(row[on]),
      offV = js(row[off]);
    let est: ReactNode = null;
    if (impact && d != null && !Number.isNaN(Number(d))) {
      const e = Number(d) * ui.ffImpactWeights[FACTOR[id]];
      est = (
        <div className="ff-impact-est" title={ui.ffImpactEstTitle}>
          est. {(e >= 0 ? "+" : "−") + Math.abs(e).toFixed(1)}
          {id.startsWith("Def") ? " pts allowed" : " pts"}
        </div>
      );
    }
    return {
      node: (
        <RangeCell
          value={value}
          on={onV === "-" ? "-" : onV + unit}
          off={offV === "-" ? "-" : offV + unit}
          onPct={num(row[`${on}_rank`])}
          offPct={num(row[`${off}_rank`])}
          extra={est}
          details={showRanges}
        />
      ),
      text: value,
      bg: heat(row[FF_HEAT[id][0]], FF_HEAT[id][1], "muted"),
      className: unit ? "onoff-factor-cell" : "onoff-rtg-cell",
    };
  };
}
export function ffColumns(ctx: Ctx): Column[] {
  const factor = (
    id: string,
    label: string,
    cls?: string,
    title?: string,
  ): Column => ({
    id,
    label,
    cls,
    title,
    render: ffCell(
      id,
      FF_METRIC_MAP[id][0],
      FF_METRIC_MAP[id][1],
      "%",
      ctx.showImpact,
      ctx.showRanges,
    ),
  });
  const rating = (id: string): Column => ({
    id,
    label: "Diff",
    cls: SEP,
    render: ffCell(
      id,
      FF_RTG_MAP[id][0],
      FF_RTG_MAP[id][1],
      "",
      false,
      ctx.showRanges,
    ),
  });
  return [
    ...nameCols(ctx),
    {
      id: "Net Diff",
      label: "Diff",
      render: (row) => {
        const d = row["Net Diff"];
        const v =
          d != null && Number(d) > 0
            ? `+${Number(d)}`
            : d == null
              ? ""
              : String(Number(d));
        return {
          node: <div style={{ fontWeight: 800, fontSize: "1.05em" }}>{v}</div>,
          text: v,
          bg: heat(row.pr_net_diff, false, "muted"),
        };
      },
    },
    rating("Off Rtg Diff"),
    factor("Off eFG% Diff", "eFG%"),
    factor("Off OREB% Diff", "OREB%", undefined, ui.offOrebTooltip),
    factor("Off TOV% Diff", "TOV%"),
    factor("Off FTR Diff", "FTR"),
    rating("Def Rtg Diff"),
    factor("Def eFG% Diff", "eFG%"),
    factor("Def OREB% Diff", "OREB%", undefined, ui.defOrebTooltip),
    factor("Def TOV% Diff", "TOV%"),
    factor("Def FTR Diff", "FTR"),
    ...usage(),
  ];
}
export const FF_GROUPS: Group[] = [
  { label: "", span: 2 },
  { label: "Total", span: 1 },
  { label: "Offense Impact (On-Off)", span: 5, cls: SEP },
  { label: "Defense Impact (On-Off)", span: 5, cls: SEP },
  { label: "Usage", span: 3, cls: SEP },
];

const C3_TITLE =
  "Corner 3s as % of 3PA with known court location; -- = location unknown";
export function spColumns(ctx: Ctx): Column[] {
  const side = (s: "off" | "def"): Column[] =>
    SP_SUFFIX.map((m, i) => {
      const id = `${s === "off" ? "Off" : "Def"} ${SP_LABELS[i]} Diff`;
      const jumper = m === "mid_share";
      const reverse = !((s === "off") === !jumper);
      return {
        id,
        label: SP_LABELS[i] === "C3" ? "Corner 3 Share" : SP_LABELS[i],
        cls: i === 0 ? SEP : undefined,
        title: SP_LABELS[i] === "C3" ? C3_TITLE : undefined,
        render: (row): Cell => {
          const d = num(row[id]),
            on = num(row[`${s}_on_${m}`]),
            off = num(row[`${s}_off_${m}`]);
          if (d == null || on == null || off == null)
            return {
              node: <div className="diff-val unranked">--</div>,
              text: "--",
            };
          const head = (d > 0 ? "+" : "") + d.toFixed(1);
          return {
            node: (
              <RangeCell
                value={head}
                on={on.toFixed(1)}
                off={off.toFixed(1)}
                onPct={num(row[`${s}_on_${m}_rank`])}
                offPct={num(row[`${s}_off_${m}_rank`])}
              />
            ),
            text: head,
            bg: heat(row[`pr_sp_${s}_${m}`], reverse, "muted"),
          };
        },
      };
    });
  return [...nameCols(ctx), ...side("off"), ...side("def"), ...usage()];
}
export const SP_GROUPS: Group[] = [
  { label: "", span: 2 },
  {
    label: "Offense Shot Profile (ON - OFF; eFG% + shares of FGA)",
    span: 7,
    cls: SEP,
  },
  {
    label: "Defense Shot Profile (ON - OFF; eFG% + shares of FGA)",
    span: 7,
    cls: SEP,
  },
  { label: "Usage", span: 3, cls: SEP },
];

/** The table for one view, with the tooltip and pivot menu it hosts. */
export default function OnOffTable({
  league,
  season,
  view,
  rows,
  showImpact,
  showRanges = true,
  exportRef,
}: {
  league: League;
  season: string;
  view: "summary" | "four_factors" | "shot_profile";
  rows: Row[];
  showImpact: boolean;
  showRanges?: boolean;
  exportRef?: { current: ((name: string) => void) | null };
}) {
  const [tip, setTip] = useState<Tip | null>(null);
  const [pivot, setPivot] = useState<Pivot | null>(null);
  useEffect(() => {
    if (!tip) return;
    const hide = () => setTip(null);
    window.addEventListener("scroll", hide, true);
    return () => window.removeEventListener("scroll", hide, true);
  }, [tip]);
  const ctx = useMemo<Ctx>(
    () => ({ onTip: setTip, onPivot: setPivot, showImpact, showRanges }),
    [showImpact, showRanges],
  );
  const columns = useMemo(
    () =>
      view === "four_factors"
        ? ffColumns(ctx)
        : view === "shot_profile"
          ? spColumns(ctx)
          : summaryColumns(rows, ctx),
    [view, ctx, rows],
  );
  const spec = {
    summary: {
      groups: SUMMARY_GROUPS,
      order: [{ id: "Net RTG Diff", desc: true }],
      pageLength: 30,
      controls: true,
    },
    four_factors: {
      groups: FF_GROUPS,
      order: [{ id: "Net Diff", desc: true }],
      pageLength: 50,
      controls: false,
    },
    shot_profile: {
      groups: SP_GROUPS,
      order: [{ id: "Off Lay+Dunk Diff", desc: true }],
      pageLength: 30,
      controls: true,
    },
  }[view];
  return (
    <>
      <DataTable
        columns={columns}
        rows={rows}
        groups={spec.groups}
        defaultOrder={spec.order}
        pageLength={spec.pageLength}
        controls={spec.controls}
        tableClass="ibpl-flip"
        nameColumns={["Player", "Team"]}
        exportRef={exportRef}
        rowProps={(row) => ({
          ...(row.team_id != null
            ? { "data-pivot-team": String(row.team_id) }
            : {}),
          ...(row.player_id != null
            ? { "data-pivot-player": String(row.player_id) }
            : {}),
        })}
      />
      {tip && <NetTooltip tip={tip} />}
      {pivot && (
        <PivotMenu
          pivot={pivot}
          league={league}
          season={season}
          onClose={() => setPivot(null)}
        />
      )}
    </>
  );
}
