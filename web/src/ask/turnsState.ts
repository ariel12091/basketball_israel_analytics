// The chat's turns and every way they change, as one reducer. The request
// itself (abort, timeout, stream reader) stays in AskProvider; this holds what
// the panel shows and what is mirrored to sessionStorage.
import type { AskAnswer } from "./turn";

export type AskTurnView = {
  id?: string;
  question: string;
  status: string[];
  answer: AskAnswer | null;
  error: string | null;
  pending: boolean;
};

export type TurnsAction =
  /** A question was sent. `keep` continues the conversation; otherwise it
   * starts a new one with this turn alone. */
  | { type: "start"; id: string; question: string; keep: boolean }
  | { type: "status"; id: string; text: string }
  | { type: "final"; id: string; answer: AskAnswer }
  | { type: "fail"; id: string; error: string }
  /** The stream ended without a final answer or an error. */
  | { type: "cutOff"; id: string }
  | { type: "reset" };

export const INTERRUPTED = "Interrupted; ask again";
export const CUT_OFF = "The answer was cut off; try again";

const interrupt = (t: AskTurnView): AskTurnView =>
  t.pending ? { ...t, pending: false, error: INTERRUPTED } : t;

/** Turns read back after a reload: one cut off by the reload cannot resume. */
export const restoreTurns = (saved: AskTurnView[] | undefined) =>
  (saved ?? []).map(interrupt);

export function turnsReducer(
  turns: AskTurnView[],
  a: TurnsAction,
): AskTurnView[] {
  const patch = (id: string, f: (t: AskTurnView) => AskTurnView) =>
    turns.map((t) => (t.id === id ? f(t) : t));
  switch (a.type) {
    case "start": {
      const turn: AskTurnView = {
        id: a.id,
        question: a.question,
        status: [],
        answer: null,
        error: null,
        pending: true,
      };
      return [...(a.keep ? turns.map(interrupt) : []), turn];
    }
    case "status":
      return patch(a.id, (t) => ({ ...t, status: [...t.status, a.text] }));
    case "final":
      return patch(a.id, (t) => ({ ...t, pending: false, answer: a.answer }));
    case "fail":
      return patch(a.id, (t) => ({ ...t, pending: false, error: a.error }));
    case "cutOff":
      return patch(a.id, (t) =>
        t.pending ? { ...t, pending: false, error: CUT_OFF } : t,
      );
    case "reset":
      return [];
  }
}
