"use client";
// The chat panel's state for every page: it lives in the root layout, so the
// conversation survives tab switches and the answers' own links (client
// navigation). Mirrored to sessionStorage so a reload keeps it.
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import { usePathname } from "next/navigation";
import type { HistoryTurn, TurnEvent } from "@/ask/turn";
import {
  restoreTurns,
  turnsReducer,
  type AskTurnView,
  type TurnsAction,
} from "@/ask/turnsState";
import type { League } from "@/rules/gameLogs";
import AskPanel from "./shiny/AskPanel";

export type { AskTurnView };
type Ask = {
  enabled: boolean;
  followups: boolean;
  open: boolean;
  setOpen: (v: boolean) => void;
  turns: AskTurnView[];
  /** `fresh` starts a new conversation (the Home search box). */
  ask: (question: string, opts?: { fresh?: boolean }) => Promise<void>;
  reset: () => void;
  league: League;
  /** An inline Ask box calls this on mount; the returned cleanup undoes it. */
  hideFab: () => () => void;
  fabHidden: boolean;
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
  const [turns, dispatch] = useReducer(
    turnsReducer,
    saved?.turns,
    restoreTurns,
  );
  const [open, setOpen] = useState(false);
  const [inlineBoxes, setInlineBoxes] = useState(0);
  const hideFab = useCallback(() => {
    setInlineBoxes((n) => n + 1);
    return () => setInlineBoxes((n) => n - 1);
  }, []);
  const clientId = useRef(saved?.clientId || "");
  const active = useRef<{
    id: string;
    controller: AbortController;
    reader?: ReadableStreamDefaultReader<Uint8Array>;
    timer?: ReturnType<typeof setTimeout>;
  } | null>(null);
  const cancelActive = useCallback(() => {
    const request = active.current;
    active.current = null;
    if (!request) return;
    clearTimeout(request.timer);
    request.controller.abort();
    void request.reader?.cancel().catch(() => {});
  }, []);
  useEffect(() => cancelActive, [cancelActive]);

  useEffect(() => {
    if (!clientId.current) clientId.current = crypto.randomUUID();
    try {
      sessionStorage.setItem(
        KEY,
        JSON.stringify({ turns, clientId: clientId.current }),
      );
    } catch {}
  }, [turns]);

  const ask = useCallback(
    async (question: string, opts?: { fresh?: boolean }) => {
      cancelActive();
      const keep = followups && !opts?.fresh;
      const request: NonNullable<typeof active.current> = {
        id: crypto.randomUUID(),
        controller: new AbortController(),
      };
      active.current = request;
      const id = request.id;
      // Only the request still on show may change its turn.
      const send = (action: TurnsAction) => {
        if (active.current !== request || request.controller.signal.aborted)
          return;
        dispatch(action);
      };
      request.timer = setTimeout(() => {
        if (active.current !== request) return;
        send({ type: "fail", id, error: "The answer timed out; try again" });
        request.controller.abort();
        void request.reader?.cancel().catch(() => {});
      }, 45_000);
      const previous = keep ? turns.filter((t) => t.answer) : [];
      const history: HistoryTurn[] = previous.map((t) => ({
        question: t.question,
        answer: t.answer!.text,
        // This turn's own lookups; answers saved before `calls` existed fall
        // back to their results.
        calls: t.answer!.calls ?? t.answer!.results.map((r) => r.call),
      }));
      dispatch({ type: "start", id, question, keep });
      try {
        const res = await fetch("/api/v1/ask", {
          signal: request.controller.signal,
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            league,
            question,
            history,
            clientId: clientId.current,
          }),
        });
        if (active.current !== request || request.controller.signal.aborted) {
          void res.body?.cancel().catch(() => {});
          return;
        }
        if (!res.ok || !res.body) {
          const body = await res.json().catch(() => ({}));
          const msg =
            res.status === 429
              ? `Too many questions; try again in ${Math.ceil(Number(body.retryAfterSec ?? 60) / 60)} min`
              : String(body.error ?? "The question could not be sent");
          send({ type: "fail", id, error: msg });
          return;
        }
        const reader = res.body.getReader();
        request.reader = reader;
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
            const e = JSON.parse(chunk) as
              TurnEvent | { type: "error"; error: string };
            if (e.type === "status") send({ type: "status", id, text: e.text });
            else if (e.type === "final") {
              send({ type: "final", id, answer: e.answer });
              return;
            } else {
              send({ type: "fail", id, error: e.error });
              return;
            }
          }
        }
        send({ type: "cutOff", id });
      } catch {
        send({ type: "fail", id, error: "Network error; try again" });
      } finally {
        clearTimeout(request.timer);
        request.controller.abort();
        // Cancellation closes the stream synchronously. Release our lock even
        // if its underlying source takes time to finish its own cancel hook.
        void request.reader?.cancel().catch(() => {});
        request.reader?.releaseLock();
        if (active.current === request) active.current = null;
      }
    },
    [followups, turns, league, cancelActive],
  );

  const reset = useCallback(() => {
    cancelActive();
    dispatch({ type: "reset" });
  }, [cancelActive]);

  const value = useMemo<Ask>(
    () => ({
      enabled,
      followups,
      open,
      setOpen,
      turns,
      ask,
      reset,
      league,
      hideFab,
      fabHidden: inlineBoxes > 0,
    }),
    [enabled, followups, open, turns, ask, reset, league, hideFab, inlineBoxes],
  );
  return (
    <Context.Provider value={value}>
      {children}
      {enabled && <AskPanel />}
    </Context.Provider>
  );
}
