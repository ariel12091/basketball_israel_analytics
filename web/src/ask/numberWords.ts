// Counts and ranks written as words, shared by the answer check (markers.ts)
// and the eval, so both use one list. The digit check cannot see these.
// "one" and "first" are left out: "one of the best" and "at first" are too common.
const EN_WORDS =
  "two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|hundred|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|half";
const HE_WORDS =
  "שניים|שתיים|שני|שתי|שלוש|שלושה|שלושת|ארבע|ארבעה|ארבעת|חמש|חמישה|חמשת|שש|שישה|ששת|שבע|שבעה|שבעת|שמונה|תשע|תשעה|עשר|עשרה|עשרים|ראשון|ראשונה|שנייה|שלישי|שלישית|רביעי|רביעית|חמישי|חמישית|חצי";
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
    .replace(/\b(?:four factors?|(?:two|three)[- ]point(?:ers?)?|(?:two|three|four|five)[- ]man)\b/gi, " ");
}
