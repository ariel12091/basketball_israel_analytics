"use client";
// The Game Logs DataTable as Shiny draws it (server_tab4.R / server_tab11):
// DT's markup and classes (dom "Btip": CSV button, table, info, pager), 50 rows
// a page, default order date desc then GN/round desc, DataTables' click and
// shift-click ordering (ascending first; no orderSequence is set on this
// table), heat colours from styleInterval(CUTS, COLS_GRAD|COLS_REV) on the
// pr_* columns, formatRound/formatCurrency text, the shot-split cells of
// make_shot_render_gl(), and the CSV export of csv_export_button().
import {
  Fragment,
  useContext,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import ui from "@/data/shinyUi.json";
import type { League, Row } from "@/rules/gameLogs";
import { defuseCsvCell } from "@/server/csv";
import { InfoButton, Strip, TouchTipsContext } from "./TouchTips";

type View = "summary" | "four_factors";
type Col = { id: string; label: string; cls?: string; title?: string };
const TIPS = ui.columnTooltips as Record<string, string>;
const HEAT_REVERSE: Record<View, Record<string, boolean>> = {
  // GL_HEAT_REVERSE_SUMMARY / GL_HEAT_REVERSE_FF in svc_game_logs.R.
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

// gamelog_summary_header() / gamelog_ff_header() with show_gameflow = TRUE.
export function columnsFor(league: League, view: View): Col[] {
  const euro = league !== "il";
  const lead: Col[] = [
    { id: euro ? "round_number" : "gn", label: euro ? "Rd" : "GN" },
    {
      id: euro ? "phase_label" : "game_type_label",
      label: euro ? "Phase" : "Game Type",
    },
    { id: "game_date", label: "Date" },
    { id: "gameflow", label: "Gameflow" },
    { id: "team_name", label: "Team" },
    { id: "opp_team_name", label: "Opponent" },
    { id: "result", label: "W/L" },
    { id: euro ? "score" : "score_display", label: "Score" },
    { id: "minutes", label: "Min" },
  ];
  const usage: Col[] = [
    { id: "off_poss", label: "Off Poss", cls: SEP },
    { id: "def_poss", label: "Def Poss" },
  ];
  if (view === "summary")
    return [
      ...lead,
      { id: "off_ppp", label: "Off PPP", cls: SEP },
      { id: "def_ppp", label: "Def PPP" },
      { id: "net_rtg", label: "Net" },
      ...(euro
        ? []
        : [
            { id: "Off Shot", label: "Off Shot", cls: SEP },
            { id: "Def Shot", label: "Def Shot" },
          ]),
      ...usage,
    ];
  const side = (s: "off" | "def"): Col[] => [
    { id: `${s}_ppp`, label: "PPP", cls: SEP },
    { id: `${s}_efg_pct`, label: "eFG%" },
    {
      id: `${s}_oreb_pct`,
      label: "OREB%",
      title: s === "off" ? ui.offOrebTooltip : ui.defOrebTooltip,
    },
    { id: `${s}_tov_pct`, label: "TOV%" },
    { id: `${s}_ftr_pct`, label: "FTR" },
  ];
  return [
    ...lead,
    { id: "net_rtg", label: "Net" },
    ...side("off"),
    ...side("def"),
    ...usage,
  ];
}

// DT::styleInterval(CUTS, cols): "isNaN(parseFloat(value)) ? '' : value <= cut ? ..."
function heat(pr: unknown, reverse: boolean): string | undefined {
  const v = typeof pr === "number" ? pr : parseFloat(String(pr));
  if (pr == null || Number.isNaN(v)) return undefined;
  const cols = reverse ? ui.heat.rev : ui.heat.grad;
  const i = ui.heat.cuts.findIndex((c) => v <= c);
  return cols[i < 0 ? cols.length - 1 : i];
}
// DTWidget.formatRound / formatCurrency: toFixed, then a "," every 3 digits.
function mark(text: string) {
  const [int, dec] = text.split(".");
  const s = int.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return dec != null ? `${s}.${dec}` : s;
}
function display(row: Row, id: string, view: View): string {
  const v = row[id];
  if (v == null) return "";
  if (typeof v !== "number") return String(v);
  if (id === "minutes" || ROUND1[view].includes(id)) return mark(v.toFixed(1));
  if (id === "off_poss" || id === "def_poss") return mark(v.toFixed(0));
  return String(v);
}

// make_shot_render_gl() in server_tab4.R, min_fga = 10.
function shotCell(
  row: Row,
  side: "off" | "def",
  avg: { fg2: number; fg3: number },
) {
  const num = (k: string) => Number(row[`${side}_${k}`] ?? 0) || 0;
  const fg2m = num("fg2m"),
    fg2a = num("fg2a"),
    fg3m = num("fg3m"),
    fg3a = num("fg3a");
  const total = fg2a + fg3a;
  if (!total)
    return {
      node: (
        <div className="shot-acc-label" style={{ color: "#aaa" }}>
          -
        </div>
      ),
      text: "-",
    };
  const fg2pct = fg2a ? Math.round((fg2m / fg2a) * 100) : 0;
  const fg3pct = fg3a ? Math.round((fg3m / fg3a) * 100) : 0;
  const fg2freq = Math.round((fg2a / total) * 100);
  const fg3freq = 100 - fg2freq;
  const sign = side === "def" ? -1 : 1;
  const acc = (pct: number, a: number) => {
    let d = (sign * (pct - a)) / a;
    d = Math.max(-1, Math.min(1, d * 3));
    const [r, g] =
      d < 0
        ? [200, Math.round(200 + d * 120)]
        : [Math.round(200 - d * 150), 170];
    return `rgb(${r},${g},60)`;
  };
  const muted = total < 10;
  const c2 = muted ? "#bbb" : acc(fg2pct, avg.fg2);
  const c3 = muted ? "#bbb" : acc(fg3pct, avg.fg3);
  const weight = muted ? 400 : 700;
  return {
    node: (
      <>
        <div className="shot-acc-label">
          <span
            title={`2PT accuracy: ${fg2pct}% (${fg2m}/${fg2a})`}
            style={{ color: c2, fontWeight: weight, cursor: "help" }}
          >
            {fg2pct}%
          </span>{" "}
          <span style={{ opacity: 0.3 }}>|</span>{" "}
          <span
            title={`3PT accuracy: ${fg3pct}% (${fg3m}/${fg3a})`}
            style={{ color: c3, fontWeight: weight, cursor: "help" }}
          >
            {fg3pct}%
          </span>
        </div>
        <div
          className="shot-bar-container"
          style={muted ? { opacity: 0.3 } : undefined}
        >
          <div
            className="shot-bar-2pt"
            title={`2PT frequency: ${fg2freq}% of FGA (${fg2a}/${total})`}
            style={{ width: `${fg2freq}%`, cursor: "help" }}
          >
            {fg2freq}%
          </div>
          <div
            className="shot-bar-3pt"
            title={`3PT frequency: ${fg3freq}% of FGA (${fg3a}/${total})`}
            style={{ width: `${fg3freq}%`, cursor: "help" }}
          >
            {fg3freq}%
          </div>
        </div>
      </>
    ),
    // Buttons.stripData() of the rendered cell, as the Shiny CSV holds it.
    text: `${fg2pct}% | ${fg3pct}%${fg2freq}%${fg3freq}%`,
  };
}

type Order = { id: string; desc: boolean }[];
// DataTables sort types: numbers numerically (null as -Infinity), the date by
// its YYYYMMDD key (gl_date_cell_renderer), text case-insensitively; ties keep
// the original row order.
function sortRows(rows: Row[], order: Order): Row[] {
  const key = (r: Row, id: string): number | string => {
    const v = r[id === "gameflow" ? "game_date" : id];
    if (id === "game_date" || id === "gameflow")
      return (
        Number(
          String(v ?? "")
            .replace(/\D/g, "")
            .slice(0, 8),
        ) || 0
      );
    if (v == null || v === "") return -Infinity;
    return typeof v === "number" ? v : String(v).toLowerCase();
  };
  return rows
    .map((r, i) => ({ r, i }))
    .sort((a, b) => {
      for (const o of order) {
        const x = key(a.r, o.id),
          y = key(b.r, o.id);
        if (x < y) return o.desc ? 1 : -1;
        if (x > y) return o.desc ? -1 : 1;
      }
      return a.i - b.i;
    })
    .map((x) => x.r);
}

// DataTables 1.13 "simple_numbers": 7 buttons with ellipses.
function pageNumbers(page: number, pages: number): (number | "…")[] {
  const range = (a: number, b: number) =>
    Array.from({ length: b - a }, (_, i) => a + i);
  if (pages <= 7) return range(0, pages);
  if (page <= 3) return [...range(0, 5), "…", pages - 1];
  if (page >= pages - 4) return [0, "…", ...range(pages - 5, pages)];
  return [0, "…", page - 1, page, page + 1, "…", pages - 1];
}

const PAGE = 50;
const stamp = () => {
  const d = new Date(),
    p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
};

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
  const cols = useMemo(() => columnsFor(league, view), [league, view]);
  const defaultOrder: Order = useMemo(
    () => [
      { id: "game_date", desc: true },
      { id: league === "il" ? "gn" : "round_number", desc: true },
    ],
    [league],
  );
  const [order, setOrder] = useState<Order>(defaultOrder);
  const [page, setPage] = useState(0);
  const body = useRef<HTMLDivElement>(null);
  const head = useRef<HTMLDivElement>(null);
  const touchTips = useContext(TouchTipsContext);
  // One explanation strip per table (mobile.js headerStripFor).
  const [tip, setTip] = useState<{ label: string; text: string } | null>(null);
  // Copy the body's column widths onto the header after every draw and resize.
  useLayoutEffect(() => {
    const sync = () => {
      const b = body.current,
        h = head.current;
      if (!b || !h) return;
      const bodyTable = b.querySelector("table"),
        headTable = h.querySelector("table");
      const inner = h.firstElementChild as HTMLElement | null;
      if (!bodyTable || !headTable || !inner) return;
      const sizing = bodyTable.querySelectorAll<HTMLElement>(
        "thead tr:last-child th",
      );
      const visible = headTable.querySelectorAll<HTMLElement>(
        "thead tr:last-child th",
      );
      sizing.forEach((th, i) => {
        // Same horizontal padding on both copies, so the content width carries.
        const w = getComputedStyle(th).width;
        if (visible[i]) {
          visible[i].style.width = w;
          visible[i].style.minWidth = w;
          visible[i].style.maxWidth = w;
        }
      });
      const width = `${bodyTable.getBoundingClientRect().width}px`;
      headTable.style.width = width;
      inner.style.width = width;
      inner.style.paddingRight = `${b.offsetWidth - b.clientWidth}px`;
      h.scrollLeft = b.scrollLeft;
    };
    sync();
    const ro = new ResizeObserver(sync);
    if (body.current) ro.observe(body.current);
    return () => ro.disconnect();
  });
  const [scrolled, setScrolled] = useState(false);

  const sorted = useMemo(() => sortRows(rows, order), [rows, order]);
  const pages = Math.max(1, Math.ceil(sorted.length / PAGE));
  const shown = sorted.slice(page * PAGE, page * PAGE + PAGE);
  const avg = (s: "off" | "def") => ({
    fg2: shotAverages?.[s]?.fg2 ?? 53,
    fg3: shotAverages?.[s]?.fg3 ?? 34,
  });
  const reverse = HEAT_REVERSE[view];

  const cell = (
    row: Row,
    c: Col,
  ): { node: ReactNode; text: string; bg?: string } => {
    if (c.id === "Off Shot" || c.id === "Def Shot") {
      const side = c.id === "Off Shot" ? "off" : "def";
      return shotCell(row, side, avg(side));
    }
    if (c.id === "gameflow")
      // GL-06 (Gameflow) is a later migration phase: the link is shown, inert.
      return {
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
      };
    if (c.id === "result") {
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
    }
    const text = display(row, c.id, view);
    const pr =
      c.id in reverse
        ? row[`pr_${c.id.replace(/[^A-Za-z0-9]+/g, "_")}`]
        : undefined;
    return {
      node: text,
      text,
      bg: c.id in reverse ? heat(pr, reverse[c.id]) : undefined,
    };
  };

  const exportCsv = () => {
    const q = (s: string) => `"${defuseCsvCell(s).replace(/"/g, '""')}"`;
    const lines = [
      cols.map((c) => q(c.label)).join(","),
      ...sorted.map((r) => cols.map((c) => q(cell(r, c).text)).join(",")),
    ];
    const blob = new Blob([lines.join("\r\n")], {
      type: "text/csv;charset=utf-8",
    });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    const slug = league === "il" ? "game_logs" : "euroleague_game_logs";
    a.download = `${slug}_${view}_${stamp()}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const onHeader = (id: string, shift: boolean) => {
    setPage(0);
    setOrder((current) => {
      const at = current.findIndex((o) => o.id === id);
      if (shift) {
        if (at < 0) return [...current, { id, desc: false }];
        return current.map((o, i) => (i === at ? { ...o, desc: !o.desc } : o));
      }
      // _fnSortListener: the first ordered column toggles alone; any other
      // column starts a fresh ascending order.
      return at === 0
        ? [{ id, desc: !current[0].desc }]
        : [{ id, desc: false }];
    });
  };

  // mobile.js "Full names above horizontally scrolling rows".
  const onScroll = () => {
    const el = body.current;
    if (!el) return;
    if (head.current) head.current.scrollLeft = el.scrollLeft;
    const now = el.scrollLeft > 0;
    if (now !== scrolled) setScrolled(now);
    if (!now) return;
    const bounds = el.getBoundingClientRect();
    el.querySelectorAll<HTMLElement>(".ibpl-row-name").forEach((label) => {
      const td = label.parentElement as HTMLElement;
      label.style.width = `${Math.max(80, el.clientWidth - 14)}px`;
      label.style.left = `${bounds.left + el.clientLeft + 6 - td.getBoundingClientRect().left}px`;
      (td.parentElement as HTMLElement).style.setProperty(
        "--ibpl-name-height",
        `${label.offsetHeight + 10}px`,
      );
    });
  };

  const ffGroups = view === "four_factors";
  // DataTables (scrollX / scrollY) draws the visible header in its own
  // .dataTables_scrollHead table and keeps a zero-height copy in the body to
  // size the columns, then copies the body's column widths onto the header.
  // DT's CSS hides sort arrows inside the scroll body, so the split matters.
  const headRows = (sizing: boolean) => (
    <>
      {ffGroups && !sizing && (
        <tr>
          <th className="group-head" colSpan={10}></th>
          <th className="group-head section-left-border" colSpan={5}>
            Offense
          </th>
          <th className="group-head section-left-border" colSpan={5}>
            Defense
          </th>
          <th className="group-head section-left-border" colSpan={2}>
            Usage
          </th>
        </tr>
      )}
      <tr style={sizing ? { height: 0 } : undefined}>
        {cols.map((c) => {
          const at = order.findIndex((o) => o.id === c.id);
          const dir =
            at >= 0 ? (order[at].desc ? "descending" : "ascending") : undefined;
          const title = c.title ?? TIPS[c.label];
          const cls = `sub-head${c.cls ? ` ${c.cls}` : ""} dt-center sorting${dir ? (dir === "descending" ? " sorting_desc" : " sorting_asc") : ""}`;
          if (sizing)
            return (
              <th
                key={c.id}
                className={cls}
                aria-hidden="true"
                style={{
                  paddingTop: 0,
                  paddingBottom: 0,
                  borderTopWidth: 0,
                  borderBottomWidth: 0,
                  height: 0,
                }}
              >
                <div
                  className="dataTables_sizing"
                  style={{ height: 0, overflow: "hidden" }}
                >
                  {c.label}
                </div>
              </th>
            );
          return (
            <th
              key={c.id}
              className={cls}
              tabIndex={0}
              aria-sort={dir}
              title={title}
              style={title ? { cursor: "help" } : undefined}
              data-column={c.id}
              onClick={(e) => onHeader(c.id, e.shiftKey)}
              onKeyDown={(e) => {
                if (e.key === "Enter") onHeader(c.id, e.shiftKey);
              }}
            >
              {c.label}
              {touchTips && title && (
                <InfoButton
                  onOpen={() =>
                    setTip((t) =>
                      t?.label === c.label
                        ? null
                        : { label: c.label, text: title },
                    )
                  }
                />
              )}
            </th>
          );
        })}
      </tr>
    </>
  );
  return (
    <div
      className={`dataTables_wrapper no-footer ibpl-name-table${scrolled ? " ibpl-names-scrolled" : ""}`}
    >
      <div className="dt-buttons">
        <button
          className="dt-button buttons-csv buttons-html5"
          tabIndex={0}
          type="button"
          onClick={exportCsv}
        >
          <span>Download CSV</span>
        </button>{" "}
      </div>
      <div className="dataTables_scroll">
        {touchTips && (
          <Strip
            className="ibpl-m-th-strip"
            open={tip != null}
            text={tip ? `${tip.label} — ${tip.text}` : ""}
            onClose={() => setTip(null)}
          />
        )}
        <div
          ref={head}
          className="dataTables_scrollHead"
          style={{
            overflow: "hidden",
            position: "relative",
            border: 0,
            width: "100%",
          }}
        >
          <div
            className="dataTables_scrollHeadInner"
            style={{ boxSizing: "content-box" }}
          >
            <table
              className="display dataTable no-footer"
              aria-label="Game logs header"
            >
              <thead>{headRows(false)}</thead>
            </table>
          </div>
        </div>
        <div
          ref={body}
          className="dataTables_scrollBody"
          style={{
            position: "relative",
            overflow: "auto",
            width: "100%",
            maxHeight: "70vh",
          }}
          onScroll={onScroll}
        >
          <table
            className="display dataTable no-footer"
            style={{ width: "100%" }}
            aria-label="Game logs"
          >
            <thead>{headRows(true)}</thead>
            <tbody>
              {shown.map((row, i) => (
                <tr
                  key={`${row.game_id}-${row.team_id}-${i}`}
                  className={`${i % 2 ? "even" : "odd"} ibpl-named-row`}
                  data-game-id={String(row.game_id ?? "")}
                  data-team-id={String(row.team_id ?? "")}
                >
                  {cols.map((c, j) => {
                    const { node, bg } = cell(row, c);
                    const cls = [
                      j === 0 ? "ibpl-name-anchor" : "",
                      c.id === "team_name" ? "ibpl-name-source" : "",
                      c.cls ?? "",
                      "dt-center",
                    ]
                      .filter(Boolean)
                      .join(" ");
                    return (
                      <td
                        key={c.id}
                        className={cls}
                        data-column={c.id}
                        style={bg ? { backgroundColor: bg } : undefined}
                      >
                        {node}
                        {j === 0 && (
                          <span className="ibpl-row-name">
                            <span>{String(row.team_name ?? "")}</span>
                          </span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <div className="dataTables_info" role="status" aria-live="polite">
        Showing {sorted.length ? page * PAGE + 1 : 0} to{" "}
        {Math.min(sorted.length, page * PAGE + PAGE)} of{" "}
        {sorted.length.toLocaleString("en-US")} entries
      </div>
      <div className="dataTables_paginate paging_simple_numbers">
        <a
          className={`paginate_button previous${page === 0 ? " disabled" : ""}`}
          role="link"
          aria-disabled={page === 0}
          tabIndex={page === 0 ? -1 : 0}
          onClick={() => page > 0 && setPage(page - 1)}
        >
          Previous
        </a>
        <span>
          {pageNumbers(page, pages).map((n, i) =>
            n === "…" ? (
              <span key={`e${i}`} className="ellipsis">
                …
              </span>
            ) : (
              <Fragment key={n}>
                <a
                  className={`paginate_button${n === page ? " current" : ""}`}
                  role="link"
                  aria-current={n === page ? "page" : undefined}
                  tabIndex={0}
                  onClick={() => setPage(n)}
                >
                  {n + 1}
                </a>
              </Fragment>
            ),
          )}
        </span>
        <a
          className={`paginate_button next${page >= pages - 1 ? " disabled" : ""}`}
          role="link"
          aria-disabled={page >= pages - 1}
          tabIndex={page >= pages - 1 ? -1 : 0}
          onClick={() => page < pages - 1 && setPage(page + 1)}
        >
          Next
        </a>
      </div>
    </div>
  );
}
