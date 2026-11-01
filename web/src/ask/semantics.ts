import ui from "@/data/shinyUi.json";
import type { ToolCall, ToolResult } from "./run";

const COUNTS = new Set(["pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta"]);
const RATINGS = /^(?:(?:off|def)_(?:on_|off_)?ppp|net_rtg|net_without_3pt_luck|net_padded|Net RTG Diff|(?:Off|Def) (?:ON|OFF) PPP|(?:Off|Def) ON Diff|(?:On|Off) Net RTG)$/;
const RATING_METRICS = new Set(["net_rtg", "off_ppp", "def_ppp"]);

export function rateBasis(call: ToolCall): string | null {
  const mode = call.tool === "player_stats" ? call.query.view :
    call.tool === "compare" && call.query.cmp_view === "players" ? call.query.team_rate : null;
  return mode === "Per 30 Minutes" ? "per 30 minutes" : mode === "Per 60 Possessions" ? "per 60 possessions" : null;
}

type Unit = "rating" | "normalized" | "other";
function markerUnit(r: ToolResult, rowId: string, key: string): Unit {
  if (RATINGS.test(key)) return "rating";
  const row = rowId === "total" ? r.total : r.rows.find(x => x.id === rowId);
  if (r.call.tool === "compare" && ["a", "b", "gap"].includes(key) && ["rtg", "net"].includes(String(row?.cells.fmt))) return "rating";
  if (r.call.tool === "compare" && ["metric_a", "metric_b", "gap"].includes(key)) {
    if (r.view === "players") return rateBasis(r.call) && !row?.cells.pct ? "normalized" : "other";
    if (RATING_METRICS.has(String(r.call.query.metric ?? "net_rtg"))) return "rating";
  }
  return r.call.tool === "player_stats" && COUNTS.has(key) && rateBasis(r.call) ? "normalized" : "other";
}

/** Definitions/normalizations are unit constants, never license to type a data value. */
export function withoutUnitPhrases(text: string, results: ToolResult[]): string {
  let own = text;
  if (results.some(r => r.rows.some(row => Object.keys(row.cells).some(k => markerUnit(r, row.id, k) === "rating")) ||
    (r.total && Object.keys(r.total.cells).some(k => markerUnit(r, "total", k) === "rating"))))
    own = own.replace(/\bper (?:100|hundred)(?: possessions\b|(?=\s+(?:with|without|on|off)\b|[,.);:]|$))|(?:לכל|ל)[ -]?(?:100|מאה) פוזשנים/gi, " ");
  if (results.some(r => rateBasis(r.call) === "per 30 minutes"))
    own = own.replace(/\bper[ -](?:30|thirty)[ -]minutes?\b|ל[ -]?30 דקות/gi, " ");
  if (results.some(r => rateBasis(r.call) === "per 60 possessions"))
    own = own.replace(/\bper[ -](?:60|sixty)[ -]possessions?\b|ל[ -]?60 פוזשנים/gi, " ");
  return own;
}

/** Bind prose to the quoted metric, before substituting actual numbers. */
export function metricUnitProblems(template: string, results: ToolResult[]): string[] {
  const byId = new Map(results.map(r => [r.id, r]));
  const units: Unit[] = [];
  const quotedBases = new Set<string>();
  const own = template.replace(/\{\{\s*([^{}]+?)\s*\}\}/g, (_m, body: string) => {
    const [rid, row, ...keys] = body.split(".");
    const r = byId.get(rid), key = keys.join(".");
    const unit = r ? markerUnit(r, row, key) : "other";
    units.push(unit);
    if (r && unit === "normalized") quotedBases.add(rateBasis(r.call)!);
    return `§${units.length - 1}§`;
  });
  const problems = new Set<string>();
  const positiveOwn = own.replace(/(?:not|never|rather than|instead of|לא)\s+(?:points?|נקודות)\s+(?:per[ -]+possession\b|לפוזשן|לכל פוזשן)/gi, "");
  // A definition may follow the numeric sentence, while a legitimate contrast
  // with an unscaled ratio may occupy a different clause in the same sentence.
  if (units.includes("rating") && /(?:(?:offensive|defensive|net) rating|(?:Off|Def) PPP|PPP)\s+(?:is|means|measures|הוא|היא|פירושו)\s+(?:[\p{L}-]+\s+){0,4}(?:points?\s+per[ -]+possession\b|ה?נקודות\s+(?:לכל\s+פוזשן|לפוזשן))/iu.test(positiveOwn))
    problems.add("Rating definition mismatch: the quoted app ratings use points per 100 possessions, not an unscaled points-per-possession ratio.");
  for (const clause of positiveOwn.split(/[.!?;\n]|\b(?:while|whereas|unlike)\b/i)) {
    const kinds = [...clause.matchAll(/§(\d+)§/g)].map(m => units[Number(m[1])]);
    const positive = clause.replace(/(?:not|never|rather than|instead of|לא)\s+(?:points?|נקודות)\s+(?:per[ -]+possession\b|לפוזשן|לכל פוזשן)/gi, "");
    if (kinds.includes("rating") && /points?\s+(?:allowed\s+)?(?:per[ -]+|\/\s*)possession\b|נקודות\s+(?:לכל\s+פוזשן(?!\p{L})|לפוזשן)/iu.test(positive))
      problems.add("Rating unit mismatch: these values are points per 100 possessions. Keep 'per 100 possessions' or use 'offensive rating', 'defensive rating' or 'net rating'; never points per possession.");
    if (kinds.includes("rating") && /(?:points?\s+per|נקודות\s+ל(?:כל)?)\s*§\d+§/.test(clause))
      problems.add("A quoted row value is not a unit denominator. Use the rating_basis marker or the fixed per-100 rating unit, never a possession-count marker.");
    if (kinds.includes("normalized") && /points?\s+per[ -]minute\b|נקודות\s+לדקה/.test(positive))
      problems.add("Normalized rate mismatch: this value is not points per minute. Quote its rate_basis rather than changing its normalization.");
  }
  if (units.includes("normalized")) {
    const basisMarker = /\{\{\s*(r\d+)\.(?:(?:values|resultValues)\.)?rate_basis\s*\}\}/g;
    const basisValues = [...template.matchAll(basisMarker)].map(m => byId.get(m[1])?.extras?.rate_basis);
    if (quotedBases.has("per 30 minutes") && !basisValues.includes("per 30 minutes") &&
      !/per[ -](?:30|thirty)[ -]minutes?\b|per[ -]half[ -]hour|ל[ -]?30 דקות|לחצי שעת משחק/i.test(template))
      problems.add("Keep the normalization scale: quote rate_basis or say per 30 minutes / per half-hour of playing time, not just 'normalized rate'.");
    if (quotedBases.has("per 60 possessions") && !basisValues.includes("per 60 possessions") &&
      !/per[ -](?:60|sixty)[ -]possessions?\b|ל[ -]?60 פוזשנים/i.test(template))
      problems.add("Keep the normalization scale: quote rate_basis or say per 60 possessions, not just 'normalized rate'.");
  }
  return [...problems];
}

type Earlier = { calls: ToolCall[] };
/** Reject contradictions, do not silently answer a different year. History reruns bypass this. */
export function protectSeason(call: ToolCall, question: string, history: Earlier[] = []): ToolCall | { error: string; param: string } {
  if (["guide", "gameflow"].includes(call.tool)) return call;
  const current = String(call.league === "il" ? ui.defaultGameYear : ui.euroDefaultSeason);
  const ranges = [...question.matchAll(/(?<!\d)(20\d{2})\s*[-–/]\s*(20\d{2}|\d{2})(?!\d)/g)]
    .map(m => [Number(m[1]), m[2].length === 2 ? Number(m[1].slice(0, 2) + m[2]) : Number(m[2])])
    .filter(([a, b]) => b === a + 1);
  const explicit = [...question.matchAll(/\bseason\s*=?\s*(20\d{2})\b|עונת\s+(20\d{2})\b|\b(?:EuroLeague|EuroCup|Israeli(?: League)?)\s+(20\d{2})(?![\d/-])/gi)].map(m => m[1] ?? m[2] ?? m[3]);
  // A bare year ("in 2025", "the 2024 season") names a season too, but which
  // one is ambiguous: Israeli seasons are named by the year they end, European
  // ones by the year they start. Allow both readings rather than force the
  // current season onto a historical question.
  const bare = ranges.length ? [] : [...question.matchAll(/(?<![\d/-])(20\d{2})(?![\d/-])/g)]
    .flatMap(m => { const y = Number(m[1]); return [y, call.league === "il" ? y + 1 : y - 1].map(String); });
  const currentAsked = /\bthis season\b|\bcurrent season\b|העונה|בעונה הנוכחית/i.test(question);
  const previousAsked = /\blast (?:season|year)\b|\bprevious (?:season|year)\b|בעונה שעברה|העונה הקודמת|בשנה שעברה|אשתקד/i.test(question);
  let allowed = ranges.length ? ranges.map(([a, b]) => String(call.league === "il" ? b : a)) : [...new Set([...explicit, ...bare])];
  if (!allowed.length) {
    if (previousAsked) allowed = currentAsked || /since|compar|improv|לעומת|בהשוואה|שיפור/i.test(question) ? [current, String(Number(current) - 1)] : [String(Number(current) - 1)];
    else if (!currentAsked && history.length) allowed = history.at(-1)!.calls.filter(c => c.league === call.league && c.query.season).map(c => String(c.query.season));
    if (!allowed.length) allowed = [current];
  }
  const raw = call.query.season;
  if (raw != null && !allowed.includes(String(raw)))
    return { error: `Season conflicts with the question. For ${call.league}, use season=${[...new Set(allowed)].join(" or ")}. Respect explicit historical ranges; do not substitute the current season.`, param: "season" };
  // Omitted explicit history must not fall through to the reader's current default.
  return raw == null && allowed.length === 1 && allowed[0] !== current ? { ...call, query: { ...call.query, season: allowed[0] } } : call;
}
