// Counts and ranks written as words, shared by the answer check (markers.ts)
// and the eval, so both use one list. The digit check cannot see these.
// Counts only: "one" ("one of the best") is too common, and ordinals ("second",
// "השלישי") mostly describe a position in a sorted list, so they are allowed.
const EN_WORDS =
  "two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|hundred|half";
const HE_WORDS =
  "שניים|שתיים|שני|שתי|שלוש|שלושה|שלושת|ארבע|ארבעה|ארבעת|חמש|חמישה|חמשת|שש|שישה|ששה|ששת|שבע|שבעה|שבעת|שמונה|תשע|תשעה|עשר|עשרה|עשרים|חצי";
// Hebrew attaches prefixes (ה, ב, ל, ו, מ, ש, כ) to the word itself.
export const NUMBER_WORD = new RegExp(
  `(?<!\\p{L})(?:(?:${EN_WORDS})|[ובהלמשכ]{0,2}(?:${HE_WORDS}))(?!\\p{L})`,
  "giu",
);
/** The word without a Hebrew prefix, lower-cased, for comparing with the question. */
export const bare = (w: string) =>
  w.toLowerCase().replace(/^[ובהלמשכ]{1,2}(?=[֐-׿])/u, "");

/** Basketball terms and list numbering that contain numbers but are not data. */
export function withoutTerms(text: string): string {
  return text
    .replace(/^[ \t]*\d{1,2}[.)][ \t]+/gm, " ")
    .replace(/\b(?:four[- ]factors?|(?:two|three)[- ]point(?:ers?)?|(?:two|three|four|five)[- ]man)\b/gi, " ")
    // Hebrew "for two/three" after a shooting word is the shot type: "קליעה לשלוש".
    .replace(
      /(?<=(?:קליע|קלע|קול|זריק|זורק|ניסיונ|ניסיון|אחוז|מטווח)\p{L}*\s+)ל(?:שלוש|שתיים)(?!\p{L})/gu,
      " ",
    )
    // "השני" / "השנייה" (with the article) is only ever "the second": a position.
    .replace(/(?<!\p{L})[ובלמשכ]?השני(?:יה)?(?!\p{L})/gu, " ");
}

/** The visitor's digits as words too: "top 5" lets the answer say "top five". */
const AS_WORDS: Record<string, string[]> = {
  "2": ["two", "שניים", "שתיים", "שני", "שתי"],
  "3": ["three", "שלוש", "שלושה", "שלושת"],
  "4": ["four", "ארבע", "ארבעה", "ארבעת"],
  "5": ["five", "חמש", "חמישה", "חמשת"],
  "6": ["six", "שש", "שישה", "ששת"],
  "7": ["seven", "שבע", "שבעה", "שבעת"],
  "8": ["eight", "שמונה"],
  "9": ["nine", "תשע", "תשעה"],
  "10": ["ten", "עשר", "עשרה"],
};
export const digitWords = (digits: string): string[] => AS_WORDS[digits] ?? [];
