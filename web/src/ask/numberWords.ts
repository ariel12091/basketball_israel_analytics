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
// Only a number word counting a stat is rejected ("שלושה ניצחונות", "five
// points"): that is a figure the data owns. "top ten", "three Maccabi teams"
// and "שני המצבים" describe the answer and only flag in the eval (user,
// 2026-10-25: eleven rejections in two runs, none a wrong number).
const STAT_EN =
  "points?|possessions?|rebounds?|assists?|minutes?|percent|wins?|losses|turnovers?|steals?|blocks?";
const STAT_HE = "נקוד|פוזש|ריבאונד|אסיסט|דקה|דקות|אחוז|ניצחו|הפסד|איבוד|חטיפ|חסימ";
const STAT_COUNT = new RegExp(
  `(${NUMBER_WORD.source})\\s+(?:(?:${STAT_EN})(?!\\p{L})|[ובהלמשכ]{0,2}(?:${STAT_HE}))`,
  "giu",
);
/** The number words in the text that count a stat. */
export const statCountWords = (text: string): string[] =>
  [...text.matchAll(STAT_COUNT)].map((m) => m[1]);

/** A whole number as written: 7, 7.3, 1,234, 12:30. */
export const NUMBER = /\p{Nd}+(?:[.,:]\p{Nd}+)*/gu;
const BASE = new Set([...EN_WORDS.split("|"), ...HE_WORDS.split("|")]);
/** The word without a Hebrew prefix, lower-cased, for comparing with the
 * question. Many number words start with a prefix letter themselves (שלוש,
 * שני, שבע), so a prefix is stripped only when what remains is a number word:
 * "לשלוש" -> "שלוש", "שלוש" stays "שלוש". */
export const bare = (w: string) => {
  const l = w.toLowerCase();
  for (const n of [0, 1, 2]) {
    if (n && !/^[ובהלמשכ]+$/u.test(l.slice(0, n))) break;
    if (BASE.has(l.slice(n))) return l.slice(n);
  }
  return l;
};

/** Basketball terms and list numbering that contain numbers but are not data. */
export function withoutTerms(text: string): string {
  return text
    .replace(/^[ \t]*\d{1,2}[.)][ \t]+/gm, " ")
    .replace(/\b(?:four[- ]factors?|(?:two|three)[- ]point(?:ers?)?|(?:two|three|four|five)[- ]man)\b/gi, " ")
    // Hebrew "for two/three" after a shooting word is the shot type: "קליעה
    // לשלוש", "אחוז הקליעה של היריבה לשתי נקודות" (up to three words between).
    .replace(
      /(?<=(?:קליע|קלע|קול|זריק|זורק|ניסיונ|ניסיון|אחוז|מטווח)\p{L}*(?:\s+[\p{L}'"״-]+){0,3}\s+)ל(?:שלוש|שתיים|שתי)(?:\s+נקודות)?(?!\p{L})/gu,
      " ",
    )
    // "השני" / "השנייה" (with the article) is only ever "the second": a position.
    .replace(/(?<!\p{L})[ובלמשכ]?השני(?:יה)?(?!\p{L})/gu, " ")
    // "שני השחקנים", "לשני המצבים", "השניים": "both", not a count from the data.
    // A noun whose own root starts with ה is still a count ("שני הפסדים",
    // "שתי הארכות").
    .replace(
      /(?<!\p{L})[ובלמשכ]?(?:שני|שתי)(?=\s+ה(?!פסד|ארכ|חטא|גנ|כרע|ופע)\p{L})/gu,
      " ",
    )
    .replace(/(?<!\p{L})[ובלמשכ]?ה(?:שניים|שתיים)(?!\p{L})/gu, " ");
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
