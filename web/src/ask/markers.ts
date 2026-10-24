// The model writes {{r1.row2.net_rtg}} where a number goes; the app fills in
// the page's text for that cell and rejects any digit it did not supply
// (spec: "Numbers are filled in by the app, never typed by the model").
import { cellText } from "./display";
import { TOOLS } from "./registry";
import type { ToolResult } from "./run";

export type Used = { result: string; row: string; key: string };
export type Filled =
  | { ok: true; text: string; used: Used[] }
  | { ok: false; problems: string[] };

const MARKER = /\{\{\s*([^{}]+?)\s*\}\}/g;
const DIGITS = /\p{Nd}+/gu;

export function fillAndVerify(
  template: string,
  results: ToolResult[],
  question: string,
  season: string,
): Filled {
  const problems: string[] = [],
    used: Used[] = [];
  const byId = new Map(results.map((r) => [r.id, r]));
  const filled: string[] = [];
  // Markers become placeholders first, so the digit check sees only the
  // model's own words; the page text goes in afterwards.
  const words = template.replace(MARKER, (_, body: string) => {
    const at = filled.length;
    if (body === "season") {
      filled.push(season);
      return `\u0000${at}\u0000`;
    }
    const [rid, rowId, ...rest] = body.split(".");
    const key = rest.join(".");
    const result = byId.get(rid);
    const row =
      result &&
      (rowId === "total" ? result.total : result.rows.find((r) => r.id === rowId));
    const text =
      result && row && TOOLS[result.call.tool].numbers
        ? cellText(result.call.tool, result.call.league, result.view, key, row)
        : null;
    if (text == null) {
      problems.push(`Marker {{${body}}} does not point at a quotable cell`);
      filled.push("");
    } else {
      filled.push(text);
      used.push({ result: rid, row: rowId, key });
    }
    return `\u0000${at}\u0000`;
  });
  if (/\{\{|\}\}/.test(words)) problems.push("Unmatched marker braces");
  const allowed = new Set(question.match(DIGITS) ?? []);
  for (const d of words.replace(/\u0000\d+\u0000/g, " ").match(DIGITS) ?? [])
    if (!allowed.has(d)) problems.push(`Typed number "${context(words, d)}"`);
  if (problems.length) return { ok: false, problems };
  return {
    ok: true,
    text: words.replace(/\u0000(\d+)\u0000/g, (_, i: string) => filled[Number(i)]),
    used,
  };
}

/** The typed number with its neighbouring characters, for the retry message. */
function context(words: string, digits: string): string {
  const plain = words.replace(/\u0000\d+\u0000/g, "…");
  const i = plain.indexOf(digits);
  return i < 0 ? digits : plain.slice(Math.max(0, i - 3), i + digits.length + 3).trim();
}
