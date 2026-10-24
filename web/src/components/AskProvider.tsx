"use client";
// The chat panel's state for every page: it lives in the root layout, so the
// conversation survives tab switches and the answers' own links (client
// navigation). Mirrored to sessionStorage so a reload keeps it.
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import type { AskAnswer, HistoryTurn, TurnEvent } from "@/ask/turn";
import type { League } from "@/rules/gameLogs";
import AskPanel from "./shiny/AskPanel";

export type AskTurnView = {
  question: string;
  status: string[];
  answer: AskAnswer | null;
  error: string | null;
  pending: boolean;
};
type Ask = {
  enabled: boolean;
  followups: boolean;
  open: boolean;
  setOpen: (v: boolean) => void;
  turns: AskTurnView[];
  ask: (question: string) => Promise<void>;
  reset: () => void;
  league: League;
};
const Context = createContext<Ask | null>(null);
export const useAsk = () => useContext(Context);

const KEY = "ibpl_ask_v1";
export const MAX_TURNS = 6;
const LEAGUES: League[] = ["il", "euroleague", "eurocup"];

function load(): { turns: AskTurnView[]; clientId: string } | null {
  if (typeof window === "undefined") return null;
  try {
    const v = JSON.parse(sessionStorage.getItem(KEY) ?? "null");
    return v && Array.isArray(v.turns) ? v : null;
  } catch {
    return null;
  }
}

export default function AskProvider({
  enabled,
  followups,
  children,
}: {
  enabled: boolean;
  followups: boolean;
  children: React.ReactNode;
}) {
  const pathname = usePathname() ?? "/";
  const segment = pathname.split("/")[1] as League;
  const league: League = LEAGUES.includes(segment) ? segment : "il";
  // Only the closed button renders before the panel opens, so reading the
  // saved conversation in the initial state cannot mismatch the server HTML.
  const [saved] = useState(load);
  const [turns, setTurns] = useState<AskTurnView[]>(() =>
    // A turn cut off by a reload cannot resume; show it as failed.
    (saved?.turns ?? []).map((t) => (t.pending ? { ...t, pending: false, error: "Interrupted; ask again" } : t)),
  );
  const [open, setOpen] = useState(false);
  const clientId = useRef(saved?.clientId || "");

  useEffect(() => {
    if (!clientId.current) clientId.current = crypto.randomUUID();
    try {
      sessionStorage.setItem(KEY, JSON.stringify({ turns, clientId: clientId.current }));
    } catch {}
  }, [turns]);

  const patchLast = (f: (t: AskTurnView) => AskTurnView) =>
    setTurns((ts) => ts.map((t, i) => (i === ts.length - 1 ? f(t) : t)));

  const ask = useCallback(
    async (question: string) => {
      const previous = followups ? turns.filter((t) => t.answer) : [];
      const history: HistoryTurn[] = previous.map((t) => ({
        question: t.question,
        answer: t.answer!.text,
        calls: t.answer!.results.map((r) => r.call),
      }));
      const start: AskTurnView = { question, status: [], answer: null, error: null, pending: true };
      setTurns((ts) => [...(followups ? ts : []), start]);
      try {
        const res = await fetch("/api/v1/ask", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ league, question, history, clientId: clientId.current }),
        });
        if (!res.ok || !res.body) {
          const body = await res.json().catch(() => ({}));
          const msg =
            res.status === 429
              ? `Too many questions; try again in ${Math.ceil(Number(body.retryAfterSec ?? 60) / 60)} min`
              : String(body.error ?? "The question could not be sent");
          patchLast((t) => ({ ...t, pending: false, error: msg }));
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let cut: number;
          while ((cut = buffer.indexOf("\n\n")) >= 0) {
            const chunk = buffer.slice(0, cut).replace(/^data: /, "");
            buffer = buffer.slice(cut + 2);
            const e = JSON.parse(chunk) as TurnEvent | { type: "error"; error: string };
            if (e.type === "status") patchLast((t) => ({ ...t, status: [...t.status, e.text] }));
            else if (e.type === "final") patchLast((t) => ({ ...t, pending: false, answer: e.answer }));
            else patchLast((t) => ({ ...t, pending: false, error: e.error }));
          }
        }
        patchLast((t) => (t.pending ? { ...t, pending: false, error: "The answer was cut off; try again" } : t));
      } catch {
        patchLast((t) => ({ ...t, pending: false, error: "Network error; try again" }));
      }
    },
    [followups, turns, league],
  );

  const value = useMemo<Ask>(
    () => ({ enabled, followups, open, setOpen, turns, ask, reset: () => setTurns([]), league }),
    [enabled, followups, open, turns, ask, league],
  );
  return (
    <Context.Provider value={value}>
      {children}
      {enabled && <AskPanel />}
    </Context.Provider>
  );
}
