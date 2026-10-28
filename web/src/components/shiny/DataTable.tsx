"use client";
// A DT::datatable() as Shiny shows it, built on TanStack Table: the library
// owns sorting and paging; this file supplies DataTables' rules and markup.
// - Ordering: a click on the first ordered column toggles it alone; any other
//   column starts a fresh order, ascending unless the column's orderSequence
//   starts with "desc" (descFirst); shift-click adds a column. Missing values
//   stay last in either direction (X-13), text case-insensitively, ties in row order.
// - paging_simple_numbers with DataTables' "Showing a to b of n entries".
// - One table with a sticky header inside a scrolling box (scrollX + scrollY
//   70vh), the "i" header explanations on touch, the phone name label above
//   horizontally scrolled rows (mobile.js), csv_export_button()'s CSV.
// Shiny's class names are kept so app.css and DataTables' CSS style it.
import MoreInfo from "./MoreInfo";
import {
  Fragment,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingFn,
  type SortingState,
  type ColumnFiltersState,
} from "@tanstack/react-table";
import ui from "@/data/shinyUi.json";
import type { Row } from "@/rules/gameLogs";
import { formatNumber } from "@/rules/format";
import { orderKey } from "@/rules/order";
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
  sortValue?: (row: Row) => number | string | boolean | null;
  /** columnDefs orderSequence c("desc", "asc"). */
  descFirst?: boolean;
  /** Shiny orderSequence c("desc"): repeated activation stays descending. */
  descOnly?: boolean;
  /** Extra attributes on the column's cells (createdRow in Shiny). */
  tdProps?: (row: Row) => Record<string, string | number>;
  /** Click, Enter or Space on the cell (the On/Off pivot menu). */
  onActivate?: (
    row: Row,
    cell: HTMLElement,
    at: { x: number; y: number },
  ) => void;
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
export { formatNumber };
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
    /** Stats-tab layout: grey/amber frequency bar and one MoreInfo for the
     * four details, instead of four hover-only titles. */
    layout?: boolean;
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
    // The stats-tab layout uses On/Off's red / neutral / green everywhere:
    // Shiny's Game Logs formula is yellow around the average (user
    // 2026-10-28: "why is the 2pt% yellow").
    if (opts.palette === "gamelogs" && !opts.layout) {
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
  // The stats-tab palette shows the four details in one MoreInfo
  // (design 2026-10-28 § 6) instead of four hover-only titles.
  const more = opts.layout === true;
  const tip = (t: string) => (more ? undefined : t);
  const body = (
    <>
      <div className="shot-acc-label">
        <span
          title={tip(`2PT accuracy: ${fg2pct}% (${fg2m}/${fg2a})`)}
          style={{ color: c2, fontWeight: weight, cursor: "help" }}
        >
          {fg2pct}%
        </span>{" "}
        <span style={{ opacity: 0.3 }}>|</span>{" "}
        <span
          title={tip(`3PT accuracy: ${fg3pct}% (${fg3m}/${fg3a})`)}
          style={{ color: c3, fontWeight: weight, cursor: "help" }}
        >
          {fg3pct}%
        </span>
      </div>
      <div
        className={`shot-bar-container${more ? " shot-bar-muted" : ""}`}
        style={muted ? { opacity: 0.3 } : undefined}
      >
        <div
          className="shot-bar-2pt"
          title={tip(`2PT frequency: ${fg2freq}% of FGA (${fg2a}/${total})`)}
          style={{ width: `${fg2freq}%`, cursor: "help" }}
        >
          {fg2freq}%
        </div>
        <div
          className="shot-bar-3pt"
          title={tip(`3PT frequency: ${fg3freq}% of FGA (${fg3a}/${total})`)}
          style={{ width: `${fg3freq}%`, cursor: "help" }}
        >
          {fg3freq}%
        </div>
      </div>
    </>
  );
  return {
    node: more ? (
      <MoreInfo
        label={body}
        ariaLabel={`2PT ${fg2pct}%, 3PT ${fg3pct}% accuracy; ${fg2freq}% / ${fg3freq}% of shots`}
        className="shot-cell-more"
      >
        <div>
          2PT accuracy: {fg2pct}% ({fg2m}/{fg2a})
        </div>
        <div>
          3PT accuracy: {fg3pct}% ({fg3m}/{fg3a})
        </div>
        <div>
          2PT frequency: {fg2freq}% of FGA ({fg2a}/{total})
        </div>
        <div>
          3PT frequency: {fg3freq}% of FGA ({fg3a}/{total})
        </div>
      </MoreInfo>
    ) : (
      body
    ),
    // Buttons.stripData() of the rendered cell, as the Shiny CSV holds it.
    text: `${fg2pct}% | ${fg3pct}%${fg2freq}%${fg3freq}%`,
  };
}

const csvStamp = () => {
  const d = new Date(),
    p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
};
const cellOf = (row: Row, c: Column): Cell => {
  if (c.render) return c.render(row);
  const v = row[c.id];
  const text = v == null ? "" : String(v);
  return { node: text, text };
};
/** csv_export_button(): visible columns, header labels, every field quoted. */
export function downloadCsv(name: string, cols: Column[], rows: Row[]) {
  const q = (s: string) => `"${defuseCsvCell(s).replace(/"/g, '""')}"`;
  const lines = [
    cols.map((c) => q(c.label)).join(","),
    ...rows.map((r) => cols.map((c) => q(cellOf(r, c).text)).join(",")),
  ];
  const a = document.createElement("a");
  a.href = URL.createObjectURL(
    new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" }),
  );
  a.download = `${name}_${csvStamp()}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}

const dtSort: SortingFn<Row> = (a, b, id) => {
  const x = a.getValue<number | string>(id),
    y = b.getValue<number | string>(id);
  return x < y ? -1 : x > y ? 1 : 0;
};
function pageNumbers(page: number, pages: number): (number | "…")[] {
  const range = (a: number, b: number) =>
    Array.from({ length: b - a }, (_, i) => a + i);
  if (pages <= 7) return range(0, pages);
  if (page <= 3) return [...range(0, 5), "…", pages - 1];
  if (page >= pages - 4) return [0, "…", ...range(pages - 5, pages)];
  return [0, "…", page - 1, page, page + 1, "…", pages - 1];
}

export default function DataTable({
  columns,
  rows,
  groups,
  defaultOrder,
  pageLength = 50,
  paging = true,
  controls = true,
  csvName,
  tableClass = "",
  nameColumns = ["team_name"],
  rowProps,
  exportRef,
  pinTotals = false,
  lengthMenu,
  columnFilters = false,
  csvLabel = "Download CSV",
  onColumnMove,
  moreInfo = false,
  onRowActivate,
  detail,
}: {
  columns: Column[];
  rows: Row[];
  groups?: Group[];
  defaultOrder: Order;
  pageLength?: number;
  /** false: every row on one page. */
  paging?: boolean;
  /** dom "t": pages of pageLength with no info line or pager. */
  controls?: boolean;
  /** dom "B...": the CSV button with this file-name slug. */
  csvName?: string;
  tableClass?: string;
  /** Columns whose text becomes the phone name label (mobile.js isName). */
  nameColumns?: string[];
  rowProps?: (row: Row) => Record<string, string>;
  /** Filled with a CSV export of the current rows and order, for a CSV
   * button outside the table (Tab 1's sidebar download). */
  exportRef?: { current: ((name: string) => void) | null };
  pinTotals?: boolean;
  lengthMenu?: number[];
  columnFilters?: boolean;
  csvLabel?: string;
  onColumnMove?: (from: string, to: string) => void;
  /** Header definitions as MoreInfo (tap on touch, hover with a mouse)
   * instead of title + the touch info button (design 2026-10-28 § 6). */
  moreInfo?: boolean;

  /** Whole-row pointer action; provide an onActivate cell for keyboard use. */
  onRowActivate?: (row: Row) => void;
  /** Content to show in a full-width row beneath this row (null: none). */
  detail?: (row: Row) => ReactNode;
}) {
  const [sorting, setSorting] = useState<SortingState>(defaultOrder);
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(pageLength);
  const [filters, setFilters] = useState<ColumnFiltersState>([]);
  const [search, setSearch] = useState("");
  const defs = useMemo<ColumnDef<Row>[]>(
    () => [
      ...(pinTotals
        ? [
            {
              id: "__total",
              accessorFn: (r: Row) => Number(r.is_total ?? 1),
              sortingFn: dtSort,
            },
          ]
        : []),
      ...columns.map(
        (c) =>
          ({
            id: c.id,
            accessorFn: (r: Row) =>
              orderKey(c.sortValue ? c.sortValue(r) : r[c.id]),
            sortingFn: dtSort,
            sortUndefined: "last",
            sortDescFirst: Boolean(c.descFirst),
            filterFn: (row, id, value) => {
              const raw = row.original[id];
              if (Array.isArray(value))
                return (
                  raw != null &&
                  (value[0] === "" || Number(raw) >= Number(value[0])) &&
                  (value[1] === "" || Number(raw) <= Number(value[1]))
                );
              return String(raw ?? "")
                .toLowerCase()
                .includes(String(value).toLowerCase());
            },
          }) as ColumnDef<Row>,
      ),
    ],
    [columns, pinTotals],
  );
  const size = paging ? pageSize : Math.max(1, rows.length);
  const table = useReactTable({
    data: rows,
    columns: defs,
    state: {
      sorting: pinTotals
        ? [{ id: "__total", desc: false }, ...sorting]
        : sorting,
      columnFilters: filters,
      globalFilter: search,
      pagination: { pageIndex, pageSize: size },
    },
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    autoResetPageIndex: false,
  });
  const sortedRows = table.getSortedRowModel().rows.map((r) => r.original);
  const pageRows = table.getRowModel().rows.map((r) => r.original);
  const pages = Math.max(1, table.getPageCount());
  useEffect(() => {
    if (exportRef)
      exportRef.current = (name) => downloadCsv(name, columns, sortedRows);
  }, [exportRef, columns, sortedRows]);

  const touchTips = useContext(TouchTipsContext);
  const [tip, setTip] = useState<{ label: string; text: string } | null>(null);
  const scroller = useRef<HTMLDivElement>(null);
  const [scrolled, setScrolled] = useState(false);

  const order = (id: string, shift: boolean) => {
    const first = Boolean(columns.find((c) => c.id === id)?.descFirst);
    const onlyDesc = Boolean(columns.find((c) => c.id === id)?.descOnly);
    setPageIndex(0);
    setSorting((current) => {
      const at = current.findIndex((o) => o.id === id);
      if (shift) {
        if (at < 0) return [...current, { id, desc: onlyDesc || first }];
        return current.map((o, i) =>
          i === at ? { ...o, desc: onlyDesc || !o.desc } : o,
        );
      }
      return at === 0
        ? [{ id, desc: onlyDesc || !current[0].desc }]
        : [{ id, desc: onlyDesc || first }];
    });
  };
  // mobile.js: once the rows scroll sideways, each row shows its names above.
  const onScroll = () => {
    const el = scroller.current;
    if (!el) return;
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
  // mobile.js puts the player before the team in the label.
  const names = nameColumns
    .map((id) => columns.find((c) => c.id === id))
    .filter((c): c is Column => Boolean(c));
  const anchorIndex = columns.findIndex((c) => !nameColumns.includes(c.id));

  return (
    <div
      className={`dataTables_wrapper no-footer${names.length ? " ibpl-name-table" : ""}${scrolled ? " ibpl-names-scrolled" : ""}`}
    >
      {lengthMenu && (
        <div className="dataTables_length">
          <label>
            Show{" "}
            <select
              aria-label="Entries per page"
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value));
                setPageIndex(0);
              }}
            >
              {lengthMenu.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>{" "}
            entries
          </label>
        </div>
      )}
      {columnFilters && (
        <div className="dataTables_filter">
          <label>
            Search:{" "}
            <input
              aria-label="Search table"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                setPageIndex(0);
              }}
            />
          </label>
        </div>
      )}
      {csvName && (
        <div className="dt-buttons">
          <button
            className="dt-button buttons-csv buttons-html5"
            tabIndex={0}
            type="button"
            onClick={() => downloadCsv(csvName, columns, sortedRows)}
          >
            <span>{csvLabel}</span>
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
        <div ref={scroller} className="ibpl-dt-scroll" onScroll={onScroll}>
          <table
            className={`display dataTable no-footer ${tableClass}`.trim()}
            aria-label="Table"
          >
            <thead>
              {groups && (
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
              <tr>
                {columns.map((c) => {
                  const dir = table.getColumn(c.id)?.getIsSorted();
                  const title = c.title ?? TIPS[c.label];
                  return (
                    <th
                      key={c.id}
                      className={`sub-head${c.cls ? ` ${c.cls}` : ""} dt-center sorting${dir === "desc" ? " sorting_desc" : dir === "asc" ? " sorting_asc" : ""}`}
                      tabIndex={0}
                      aria-sort={
                        dir === "desc"
                          ? "descending"
                          : dir === "asc"
                            ? "ascending"
                            : undefined
                      }
                      title={moreInfo ? undefined : title}
                      style={
                        title && !moreInfo ? { cursor: "help" } : undefined
                      }
                      data-column={c.id}
                      draggable={Boolean(onColumnMove)}
                      onDragStart={
                        onColumnMove
                          ? (e) => {
                              e.dataTransfer.setData("text/plain", c.id);
                              e.dataTransfer.effectAllowed = "move";
                            }
                          : undefined
                      }
                      onDragOver={
                        onColumnMove ? (e) => e.preventDefault() : undefined
                      }
                      onDrop={
                        onColumnMove
                          ? (e) => {
                              e.preventDefault();
                              const from = e.dataTransfer.getData("text/plain");
                              if (columns.some((col) => col.id === from))
                                onColumnMove(from, c.id);
                            }
                          : undefined
                      }
                      onClick={(e) => order(c.id, e.shiftKey)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") order(c.id, e.shiftKey);
                      }}
                    >
                      {moreInfo && title ? (
                        <>
                          <MoreInfo label={c.label}>{title}</MoreInfo>
                          {/* On touch the label opens its definition; this
                              edge strip (over the sort arrows) sorts. */}
                          <span className="th-sort-target" aria-hidden="true" />
                        </>
                      ) : (
                        c.label
                      )}
                      {touchTips && title && !moreInfo && (
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
              {columnFilters && (
                <tr className="ibpl-column-filters">
                  {columns.map((c) => {
                    const numeric = rows.some(
                      (r) => typeof r[c.id] === "number",
                    );
                    const value =
                      filters.find((f) => f.id === c.id)?.value ??
                      (numeric ? ["", ""] : "");
                    const update = (v: unknown) => {
                      setPageIndex(0);
                      setFilters((fs) => [
                        ...fs.filter((f) => f.id !== c.id),
                        { id: c.id, value: v },
                      ]);
                    };
                    return (
                      <th key={c.id}>
                        {numeric ? (
                          <details>
                            <summary>All</summary>
                            <label>
                              Min{" "}
                              <input
                                type="number"
                                aria-label={`${c.label} minimum`}
                                value={(value as string[])[0]}
                                onChange={(e) =>
                                  update([
                                    e.target.value,
                                    (value as string[])[1],
                                  ])
                                }
                              />
                            </label>
                            <label>
                              Max{" "}
                              <input
                                type="number"
                                aria-label={`${c.label} maximum`}
                                value={(value as string[])[1]}
                                onChange={(e) =>
                                  update([
                                    (value as string[])[0],
                                    e.target.value,
                                  ])
                                }
                              />
                            </label>
                          </details>
                        ) : (
                          <input
                            aria-label={`Filter ${c.label}`}
                            placeholder="All"
                            value={value as string}
                            onChange={(e) => update(e.target.value)}
                          />
                        )}
                      </th>
                    );
                  })}
                </tr>
              )}
            </thead>
            <tbody>
              {pageRows.map((row, i) => (
                <Fragment key={i}>
                  <tr
                    {...(rowProps ? rowProps(row) : {})}
                    onClick={
                      onRowActivate
                        ? (e) => {
                            if (
                              !(e.target instanceof Element) ||
                              e.target.closest(
                                'button, a, input, select, textarea, [role="button"]',
                              )
                            )
                              return;
                            onRowActivate(row);
                          }
                        : undefined
                    }
                    className={`${i % 2 ? "even" : "odd"}${names.length ? " ibpl-named-row" : ""}${rowProps?.(row).className ? ` ${rowProps(row).className}` : ""}`}
                  >
                    {columns.map((c, j) => {
                      const cell = cellOf(row, c);
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
                          {...(c.tdProps ? c.tdProps(row) : {})}
                          onClick={
                            c.onActivate
                              ? (e) =>
                                  c.onActivate!(row, e.currentTarget, {
                                    x: e.clientX,
                                    y: e.clientY,
                                  })
                              : undefined
                          }
                          onKeyDown={
                            c.onActivate
                              ? (e) => {
                                  if (e.key !== "Enter" && e.key !== " ")
                                    return;
                                  e.preventDefault();
                                  const r =
                                    e.currentTarget.getBoundingClientRect();
                                  c.onActivate!(row, e.currentTarget, {
                                    x: r.left,
                                    y: r.bottom,
                                  });
                                }
                              : undefined
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
                                    {cellOf(row, n).text}
                                  </span>
                                </Fragment>
                              ))}
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                  {(() => {
                    // A row's details open in place beneath it (user
                    // 2026-10-28: no modal windows), one cell across the table.
                    const d = detail?.(row);
                    return d ? (
                      <tr className="ibpl-row-detail">
                        <td colSpan={columns.length}>
                          <div className="ibpl-row-detail-body">{d}</div>
                        </td>
                      </tr>
                    ) : null;
                  })()}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      {paging && controls && (
        <>
          <div className="dataTables_info" role="status" aria-live="polite">
            Showing {sortedRows.length ? pageIndex * size + 1 : 0} to{" "}
            {Math.min(sortedRows.length, pageIndex * size + size)} of{" "}
            {sortedRows.length.toLocaleString("en-US")} entries
          </div>
          <div className="dataTables_paginate paging_simple_numbers">
            <a
              className={`paginate_button previous${pageIndex === 0 ? " disabled" : ""}`}
              role="link"
              aria-disabled={pageIndex === 0}
              tabIndex={pageIndex === 0 ? -1 : 0}
              onClick={() => pageIndex > 0 && setPageIndex(pageIndex - 1)}
            >
              Previous
            </a>
            <span>
              {pageNumbers(pageIndex, pages).map((n, i) =>
                n === "…" ? (
                  <span key={`e${i}`} className="ellipsis">
                    …
                  </span>
                ) : (
                  <a
                    key={n}
                    className={`paginate_button${n === pageIndex ? " current" : ""}`}
                    role="link"
                    aria-current={n === pageIndex ? "page" : undefined}
                    tabIndex={0}
                    onClick={() => setPageIndex(n)}
                  >
                    {n + 1}
                  </a>
                ),
              )}
            </span>
            <a
              className={`paginate_button next${pageIndex >= pages - 1 ? " disabled" : ""}`}
              role="link"
              aria-disabled={pageIndex >= pages - 1}
              tabIndex={pageIndex >= pages - 1 ? -1 : 0}
              onClick={() =>
                pageIndex < pages - 1 && setPageIndex(pageIndex + 1)
              }
            >
              Next
            </a>
          </div>
        </>
      )}
    </div>
  );
}
