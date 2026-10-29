// Example questions shown in the Ask boxes, one per capability. Each is an
// eval case's question, word for word (test/ask/cases.json, pinned by
// examples.test.ts), and only cases that passed the latest eval run belong
// here: an example must be a question the chat is known to answer.
import type { League } from "@/rules/gameLogs";

export type AskExample = { id: string; text: string };

const IL: AskExample[] = [
  { id: "user-top5-lineups-en", text: "top 5 lineups of Maccabi Tel Aviv" },
  {
    id: "best-player-team-en",
    text: "who is the best player on Maccabi Tel Aviv?",
  },
  { id: "user-good-at-he", text: "במה הפועל ירושלים טובה?" },
  {
    id: "user-vs-top4-en",
    text: "how does Hapoel Tel Aviv's performance differ against the top 4 teams?",
  },
  {
    id: "user-onoff-shot-en",
    text: "With which Hapoel Tel Aviv player on court does the team shoot best from three?",
  },
  {
    id: "user-players-home-away-en",
    text: "which Maccabi Tel Aviv players score more at home than away?",
  },
  { id: "tool-compare-clutch-he", text: "איך מכבי בקלאץ' לעומת שאר המשחק?" },
  {
    id: "tool-shot-profile-en",
    text: "which team takes the most corner threes?",
  },
  { id: "tool-game-logs-en", text: "Hapoel Holon's last 5 games" },
  { id: "guide-ts-en", text: "what does TS% mean?" },
];

const EURO: AskExample[] = [
  { id: "euro-ratings-en", text: "best net rating in the EuroLeague" },
  {
    id: "euro-compare-en",
    text: "compare Real Madrid at home and away in the EuroLeague",
  },
  { id: "euro-lineups-he", text: "החמישיות הטובות של ריאל מדריד ביורוליג" },
  { id: "eurocup-onoff-en", text: "biggest on/off impact in the EuroCup" },
];

export const ASK_EXAMPLES = { il: IL, euro: EURO };

export const askExamples = (league: League) =>
  league === "il" ? ASK_EXAMPLES.il : ASK_EXAMPLES.euro;
