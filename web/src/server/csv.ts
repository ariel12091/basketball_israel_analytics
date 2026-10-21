import type { Row } from "../rules/gameLogs";
// docs/shared_definitions.md §3 "Exports": csv_defuse_formula() in
// app/R/helpers.R. A text cell starting with one of CSV_FORMULA_PREFIXES that is
// not numeric text (CSV_NUMERIC_TEXT_PATTERN: "-5", "+1.5", "-3.2%") gets a
// leading single quote so a spreadsheet shows it as text.
const CSV_FORMULA_PREFIXES = ["=", "+", "-", "@", "\t", "\r"];
const CSV_NUMERIC_TEXT_PATTERN =
  /^[-+]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][-+]?[0-9]+)?%?$/;
export function defuseCsvCell(s: string) {
  return CSV_FORMULA_PREFIXES.includes(s.charAt(0)) &&
    !CSV_NUMERIC_TEXT_PATTERN.test(s)
    ? "'" + s
    : s;
}
export function toCsv(rows: Row[], columns: string[]): string {
  const cell = (v: unknown) => {
    const text =
      v == null ? "" : typeof v === "string" ? defuseCsvCell(v) : String(v);
    return /[",\r\n]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
  };
  return (
    [
      columns.map(cell).join(","),
      ...rows.map((r) => columns.map((c) => cell(r[c])).join(",")),
    ].join("\r\n") + "\r\n"
  );
}
