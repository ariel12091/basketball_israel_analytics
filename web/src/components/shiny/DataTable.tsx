"use client";
// A DT::datatable() as Shiny renders it, for every tab: DataTables' markup and
// classes, the scrollX/scrollY split (header in .dataTables_scrollHead, a
// zero-height sizing copy in the body -- DT's CSS hides sort arrows inside the
// scroll body), DataTables ordering (click: the first ordered column toggles,
// any other starts ascending unless the column sets its own sequence;
// shift-click adds; empty numbers sort as -Infinity; ties keep row order),
// paging_simple_numbers, the "i" header explanations on touch, the phone name
// label above horizontally scrolled rows, and csv_export_button()'s CSV.
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
import type { Row } from "@/rules/gameLogs";
import { defuseCsvCell } from "@/server/csv";
import { InfoButton, Strip, TouchTipsContext } from "./TouchTips";

export type Cell = {
  node: ReactNode;
  text: string;
  bg?: string;
  className?: string;
};
export type Column = {
  id: string;
  label: string;
  cls?: string;
  title?: string;
  /** Display and CSV text; defaults to the value as text. */
  render?: (row: Row) => Cell;
  /** DataTables sort value; defaults to the row value. */
  sortValue?: (row: Row) => number | string | null;
  /** columnDefs orderSequence; ["desc","asc"] starts descending. */
  descFirst?: boolean;
};
export type Group = { label: string; span: number; cls?: string };
export type Order = { id: string; desc: boolean }[];

const TIPS = ui.columnTooltips as Record<string, string>;
const HEAT = ui.heat;

/** DT::styleInterval(CUTS, cols) on a pr_* value: '' when not a number. */
export function heat(pr: unknown, reverse: boolean): string | undefined {
  const v = typeof pr === "number" ? pr : parseFloat(String(pr));
  if (pr == null || Number.isNaN(v)) return undefined;
  const cols = reverse ? HEAT.rev : HEAT.grad;
  const i = HEAT.cuts.findIndex((c) => v <= c);
  return cols[i < 0 ? cols.length - 1 : i];
}
/** DTWidget.formatRound / formatCurrency: toFixed, then "," every 3 digits. */
export function formatNumber(v: unknown, digits: number): string {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  const [int, dec] = n.toFixed(digits).split(".");
  const s = int.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return dec != null ? `${s}.${dec}` : s;
}
/** The make_shot_render() cell shared by the Game Logs and On/Off tables:
 * 2PT|3PT accuracy coloured against a league average, frequency bars, muted
 * below minFga. `palette` is the colour formula each renderer uses. */
export function shotCell(
  m: { fg2m: number; fg2a: number; fg3m: number; fg3a: number },
  opts: {
    defense: boolean;
    minFga: number;
    avg2: number;
    avg3: number;
    palette: "gamelogs" | "onoff";
  },
): Cell {
  const { fg2m, fg2a, fg3m, fg3a } = m;
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
  const sign = opts.defense ? -1 : 1;
  const acc = (pct: number, a: number) => {
    let d = (sign * (pct - a)) / a;
    d = Math.max(-1, Math.min(1, d * 3));
    if (opts.palette === "gamelogs") {
      const [r, g] =
        d < 0
          ? [200, Math.round(200 + d * 120)]
          : [Math.round(200 - d * 150), 170];
      return `rgb(${r},${g},60)`;
    }
    const [r, g, b] =
      d < 0
        ? [
            Math.round(196 + d * 40),
            Math.round(120 + d * 78),
            Math.round(96 + d * 60),
          ]
        : [
            Math.round(150 - d * 60),
            Math.round(150 + d * 82),
            Math.round(110 + d * 20),
          ];
    return `rgb(${r},${g},${b})`;
  };
  const muted = total < opts.minFga;
  const c2 = muted ? "#bbb" : acc(fg2pct, opts.avg2);
  const c3 = muted ? "#bbb" : acc(fg3pct, opts.avg3);
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

function sortRows(rows: Row[], order: Order, cols: Map<string, Column>): Row[] {
  const key = (r: Row, id: string): number | string => {
    const c = cols.get(id);
    const v = c?.sortValue ? c.sortValue(r) : r[id];
    if (v == null || v === "") return -Infinity;
    if (typeof v === "number") return v;
    if (typeof v === "boolean") return Number(v);
    return String(v).toLowerCase();
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
function pageNumbers(page: number, pages: number): (number | "…")[] {
  const range = (a: number, b: number) =>
    Array.from({ length: b - a }, (_, i) => a + i);
  if (pages <= 7) return range(0, pages);
  if (page <= 3) return [...range(0, 5), "…", pages - 1];
  if (page >= pages - 4) return [0, "…", ...range(pages - 5, pages)];
  return [0, "…", page - 1, page, page + 1, "…", pages - 1];
}
export const csvStamp = () => {
  const d = new Date(),
    p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
};
/** csv_export_button(): visible columns, header labels, every field quoted. */
export function downloadCsv(name: string, cols: Column[], rows: Row[]) {
  const q = (s: string) => `"${defuseCsvCell(s).replace(/"/g, '""')}"`;
  const text = (r: Row, c: Column) =>
    c.render ? c.render(r).text : r[c.id] == null ? "" : String(r[c.id]);
  const lines = [
    cols.map((c) => q(c.label)).join(","),
    ...rows.map((r) => cols.map((c) => q(text(r, c))).join(",")),
  ];
  const a = document.createElement("a");
  a.href = URL.createObjectURL(
    new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" }),
  );
  a.download = `${name}_${csvStamp()}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}

export default function DataTable({
  columns,
  rows,
  groups,
  defaultOrder,
  pageLength = 50,
  paging = true,
  csvName,
  tableClass = "",
  nameColumns = ["team_name"],
  rowProps,
  exportRef,
}: {
  columns: Column[];
  rows: Row[];
  groups?: Group[];
  defaultOrder: Order;
  pageLength?: number;
  /** dom without "p"/"i" (e.g. "t"): every row on one page, no info line. */
  paging?: boolean;
  /** dom "B...": the CSV button with this file-name slug. */
  csvName?: string;
  tableClass?: string;
  /** Columns whose text becomes the phone name label (mobile.js isName). */
  nameColumns?: string[];
  rowProps?: (row: Row) => Record<string, string>;
  /** Filled with a CSV export of the current rows and order, for a CSV
   * button outside the table (Tab 1's sidebar download). */
  exportRef?: { current: ((name: string) => void) | null };
}) {
  const colMap = useMemo(
    () => new Map(columns.map((c) => [c.id, c])),
    [columns],
  );
  const [order, setOrder] = useState<Order>(defaultOrder);
  const [page, setPage] = useState(0);
  const body = useRef<HTMLDivElement>(null);
  const head = useRef<HTMLDivElement>(null);
  const touchTips = useContext(TouchTipsContext);
  const [tip, setTip] = useState<{ label: string; text: string } | null>(null);
  const [scrolled, setScrolled] = useState(false);
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

  const sorted = useMemo(
    () => sortRows(rows, order, colMap),
    [rows, order, colMap],
  );
  const size = paging ? pageLength : Math.max(1, sorted.length);
  const pages = Math.max(1, Math.ceil(sorted.length / size));
  const shown = sorted.slice(page * size, page * size + size);
  useLayoutEffect(() => {
    if (exportRef)
      exportRef.current = (name) => downloadCsv(name, columns, sorted);
  }, [exportRef, columns, sorted]);

  const render = (row: Row, c: Column): Cell => {
    if (c.render) return c.render(row);
    const v = row[c.id];
    const text = v == null ? "" : String(v);
    return { node: text, text };
  };
  const onHeader = (id: string, shift: boolean) => {
    const first = !colMap.get(id)?.descFirst ? false : true;
    setPage(0);
    setOrder((current) => {
      const at = current.findIndex((o) => o.id === id);
      if (shift) {
        if (at < 0) return [...current, { id, desc: first }];
        return current.map((o, i) => (i === at ? { ...o, desc: !o.desc } : o));
      }
      return at === 0
        ? [{ id, desc: !current[0].desc }]
        : [{ id, desc: first }];
    });
  };
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
  const names = columns.filter((c) => nameColumns.includes(c.id));
  const anchorIndex = columns.findIndex((c) => !nameColumns.includes(c.id));
  const headRows = (sizing: boolean) => (
    <>
      {groups && !sizing && (
        <tr>
          {groups.map((g, i) => (
            <th
              key={i}
              className={`group-head${g.cls ? ` ${g.cls}` : ""}`}
              colSpan={g.span}
            >
              {g.label}
            </th>
          ))}
        </tr>
      )}
      <tr style={sizing ? { height: 0 } : undefined}>
        {columns.map((c) => {
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
      className={`dataTables_wrapper no-footer${names.length ? " ibpl-name-table" : ""}${scrolled ? " ibpl-names-scrolled" : ""}`}
    >
      {csvName && (
        <div className="dt-buttons">
          <button
            className="dt-button buttons-csv buttons-html5"
            tabIndex={0}
            type="button"
            onClick={() => downloadCsv(csvName, columns, sorted)}
          >
            <span>Download CSV</span>
          </button>{" "}
        </div>
      )}
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
              className={`display dataTable no-footer ${tableClass}`.trim()}
              aria-label="Table header"
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
            className={`display dataTable no-footer ${tableClass}`.trim()}
            style={{ width: "100%" }}
            aria-label="Table"
          >
            <thead>{headRows(true)}</thead>
            <tbody>
              {shown.map((row, i) => (
                <tr
                  key={i + page * size}
                  className={`${i % 2 ? "even" : "odd"}${names.length ? " ibpl-named-row" : ""}`}
                  {...(rowProps ? rowProps(row) : {})}
                >
                  {columns.map((c, j) => {
                    const cell = render(row, c);
                    const cls = [
                      j === anchorIndex && names.length
                        ? "ibpl-name-anchor"
                        : "",
                      names.some((n) => n.id === c.id)
                        ? "ibpl-name-source"
                        : "",
                      c.cls ?? "",
                      cell.className ?? "",
                      "dt-center",
                    ]
                      .filter(Boolean)
                      .join(" ");
                    return (
                      <td
                        key={c.id}
                        className={cls}
                        data-column={c.id}
                        style={
                          cell.bg ? { backgroundColor: cell.bg } : undefined
                        }
                      >
                        {cell.node}
                        {j === anchorIndex && names.length > 0 && (
                          <span className="ibpl-row-name">
                            {names.map((n, k) => (
                              <Fragment key={n.id}>
                                {k > 0 && " · "}
                                <span
                                  className={
                                    k ? "ibpl-row-name-team" : undefined
                                  }
                                >
                                  {render(row, n).text}
                                </span>
                              </Fragment>
                            ))}
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
      {paging && (
        <>
          <div className="dataTables_info" role="status" aria-live="polite">
            Showing {sorted.length ? page * size + 1 : 0} to{" "}
            {Math.min(sorted.length, page * size + size)} of{" "}
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
                  <a
                    key={n}
                    className={`paginate_button${n === page ? " current" : ""}`}
                    role="link"
                    aria-current={n === page ? "page" : undefined}
                    tabIndex={0}
                    onClick={() => setPage(n)}
                  >
                    {n + 1}
                  </a>
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
        </>
      )}
    </div>
  );
}
