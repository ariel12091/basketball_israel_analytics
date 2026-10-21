"use client";
// Shiny's dateRangeInput: two text inputs in an .input-daterange group with a
// bootstrap-datepicker dropdown (the markup and classes of datepicker 1.9, so
// the theme-compiled stylesheet Shiny ships draws it). yyyy-mm-dd, week starts
// Sunday, autoclose, days outside [min, max] disabled.
import { useState } from "react";

const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];
const DOW = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];
const iso = (d: Date) => d.toISOString().slice(0, 10);
const parse = (s: string) =>
  /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(s))
    ? new Date(`${s}T00:00:00Z`)
    : null;

function Picker({
  left,
  value,
  min,
  max,
  range,
  onPick,
}: {
  left: number;
  value: string;
  min: string;
  max: string;
  range: [string, string];
  onPick: (v: string) => void;
}) {
  const start = parse(value) ?? parse(min) ?? new Date();
  const [month, setMonth] = useState(
    () => new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 1)),
  );
  const first = new Date(month);
  first.setUTCDate(1 - first.getUTCDay());
  const today = iso(new Date());
  const weeks = Array.from({ length: 6 }, (_, w) =>
    Array.from({ length: 7 }, (_, d) => {
      const day = new Date(first);
      day.setUTCDate(first.getUTCDate() + w * 7 + d);
      return day;
    }),
  );
  const shift = (n: number) =>
    setMonth(
      new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth() + n, 1)),
    );
  const before = iso(
    new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth(), 0)),
  );
  const after = iso(
    new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth() + 1, 1)),
  );
  return (
    <div
      className="datepicker datepicker-dropdown dropdown-menu datepicker-orient-left datepicker-orient-bottom"
      style={{ display: "block", top: "100%", left, zIndex: 1000 }}
      onMouseDown={(e) => e.preventDefault()}
    >
      <div className="datepicker-days" style={{ display: "block" }}>
        <table className="table-condensed">
          <thead>
            <tr>
              <th
                className={`prev${min && before < min ? " disabled" : ""}`}
                onClick={() => !(min && before < min) && shift(-1)}
              >
                «
              </th>
              <th colSpan={5} className="datepicker-switch">
                {MONTHS[month.getUTCMonth()]} {month.getUTCFullYear()}
              </th>
              <th
                className={`next${max && after > max ? " disabled" : ""}`}
                onClick={() => !(max && after > max) && shift(1)}
              >
                »
              </th>
            </tr>
            <tr>
              {DOW.map((d) => (
                <th key={d} className="dow">
                  {d}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {weeks.map((week, w) => (
              <tr key={w}>
                {week.map((day) => {
                  const s = iso(day);
                  const off = (min && s < min) || (max && s > max);
                  const cls = [
                    day.getUTCMonth() < month.getUTCMonth() ||
                    day.getUTCFullYear() < month.getUTCFullYear()
                      ? "old"
                      : "",
                    day.getUTCMonth() > month.getUTCMonth() ||
                    day.getUTCFullYear() > month.getUTCFullYear()
                      ? "new"
                      : "",
                    "day",
                    s === today ? "today" : "",
                    s === value ? "active" : "",
                    s > range[0] && s < range[1] ? "range" : "",
                    s === range[0] ? "range-start" : "",
                    s === range[1] ? "range-end" : "",
                    off ? "disabled" : "",
                  ]
                    .filter(Boolean)
                    .join(" ");
                  return (
                    <td
                      key={s}
                      className={cls}
                      data-date={day.getTime()}
                      onClick={() => !off && onPick(s)}
                    >
                      {day.getUTCDate()}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function DateRangeInput({
  id,
  label,
  start,
  end,
  min,
  max,
  onChange,
}: {
  id: string;
  label: string;
  start: string;
  end: string;
  min: string;
  max: string;
  onChange: (start: string, end: string) => void;
}) {
  const [open, setOpen] = useState<0 | 1 | null>(null);
  const [left, setLeft] = useState(0);
  // What is typed is held only while editing; otherwise the value shows.
  const [draft, setDraft] = useState<[string | null, string | null]>([
    null,
    null,
  ]);
  const values: [string, string] = [start, end];
  const set = (i: 0 | 1, v: string) =>
    i === 0
      ? onChange(v, end < v ? v : end)
      : onChange(start > v ? v : start, v);
  const commit = (i: 0 | 1) => {
    const text = draft[i];
    const d = text == null ? null : parse(text);
    if (d) set(i, iso(d));
    setDraft([null, null]);
  };
  const input = (i: 0 | 1) => (
    <input
      className="form-control"
      type="text"
      aria-label={`${label} ${i ? "end" : "start"}`}
      title="Date format: yyyy-mm-dd"
      value={draft[i] ?? values[i]}
      onFocus={(e) => {
        setLeft(e.currentTarget.offsetLeft);
        setOpen(i);
      }}
      onBlur={() => {
        setOpen(null);
        commit(i);
      }}
      onChange={(e) => {
        const next: [string | null, string | null] = [...draft];
        next[i] = e.target.value;
        setDraft(next);
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter") commit(i);
        if (e.key === "Escape") setOpen(null);
      }}
    />
  );
  return (
    <div
      id={id}
      className="shiny-date-range-input form-group shiny-input-container"
    >
      <label className="control-label" id={`${id}-label`} htmlFor={id}>
        {label}
      </label>
      <div
        className="input-daterange input-group input-group-sm"
        style={{ position: "relative" }}
      >
        {input(0)}
        <span className="input-group-addon input-group-prepend input-group-append">
          <span className="input-group-text"> to </span>
        </span>
        {input(1)}
        {open !== null && (
          <Picker
            key={open}
            left={left}
            value={values[open]}
            min={min}
            max={max}
            range={values}
            onPick={(v) => {
              set(open, v);
              setOpen(null);
              (document.activeElement as HTMLElement | null)?.blur();
            }}
          />
        )}
      </div>
    </div>
  );
}
