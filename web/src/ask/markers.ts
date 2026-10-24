// The model writes {{r1.row2.net_rtg}} where a number goes; the app fills in
// the page's text for that cell and rejects any digit it did not supply
// (spec: "Numbers are filled in by the app, never typed by the model").
import { cellText, columnDefinition, displaySpec } from "./display";
import { TOOLS } from "./registry";
import type { ToolResult } from "./run";

export type Used = { result: string; row: string; key: string };
export type Filled =
  | { ok: true; text: string; used: Used[] }
  | { ok: false; problems: string[] };

const MARKER = /\{\{\s*([^{}]+?)\s*\}\}/g;
/** A whole number as written: 7, 7.3, 1,234, 12:30. */
const NUMBER = /\p{Nd}+(?:[.,:]\p{Nd}+)*/gu;
/** Counts and ranks written as words, which the digit check cannot see. "one"
 * and "first" are left out: "one of the best" and "at first" are too common. */
const EN_WORDS =
  "two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|hundred|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|half";
const HE_WORDS =
  "שניים|שתיים|שני|שתי|שלוש|שלושה|שלושת|ארבע|ארבעה|ארבעת|חמש|חמישה|חמשת|שש|שישה|ששת|שבע|שבעה|שבעת|שמונה|תשע|תשעה|עשר|עשרה|עשרים|ראשון|ראשונה|שנייה|שלישי|שלישית|רביעי|רביעית|חמישי|חמישית|חצי";
// Hebrew attaches prefixes (ה, ב, ל, ו, מ, ש, כ) to the word itself.
const NUMBER_WORD = new RegExp(
  `(?<!\\p{L})(?:(?:${EN_WORDS})|[ובהלמשכ]{0,2}(?:${HE_WORDS}))(?!\\p{L})`,
  "giu",
);
const bare = (w: string) => w.toLowerCase().replace(/^[ובהלמשכ]{1,2}(?=[֐-׿])/, "");

export function fillAndVerify(
  template: string,
  results: ToolResult[],
  question: string,
  season: string,
): Filled {
  const problems: string[] = [],
    used: Used[] = [];
  // The placeholder below uses NUL; a template carrying one could forge it.
  if (template.includes("\u0000"))
    return { ok: false, problems: ["The answer contains a control character"] };
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
    if (!text) {
      problems.push(
        text === ""
          ? `Marker {{${body}}} points at an empty cell`
          : `Marker {{${body}}} does not point at a quotable cell`,
      );
      filled.push("");
    } else {
      filled.push(text);
      used.push({ result: rid, row: rowId, key });
    }
    return `\u0000${at}\u0000`;
  });
  if (/\{\{|\}\}/.test(words)) problems.push("Unmatched marker braces");
  // Whole numbers the visitor typed may be repeated ("top 5"), and so may the
  // constants in the quoted columns' own definitions ("per 100 possessions").
  const allowed = new Set(question.match(NUMBER) ?? []);
  for (const r of results)
    if (TOOLS[r.call.tool].numbers)
      for (const c of Object.values(displaySpec(r.call.tool, r.call.league, r.view)))
        for (const n of columnDefinition(c.label).match(NUMBER) ?? []) allowed.add(n);
  const own = words.replace(/\u0000\d+\u0000/g, " ");
  for (const d of own.match(NUMBER) ?? [])
    if (!allowed.has(d)) problems.push(`Typed number "${context(words, d)}"`);
  for (const c of own.match(/\p{No}/gu) ?? [])
    problems.push(`Numeric character "${c}"`);
  const asked = new Set((question.match(NUMBER_WORD) ?? []).map(bare));
  for (const w of own.match(NUMBER_WORD) ?? [])
    if (!asked.has(bare(w)))
      problems.push(`Number written as a word "${w}": use a marker for counts and ranks`);
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
