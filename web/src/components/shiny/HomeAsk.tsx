"use client";
// Home's search-style entry to the Ask chat: a question typed here starts a
// new conversation in the existing panel, which opens to show the answer.
// The placeholder walks through the example questions.
import { useEffect, useState } from "react";
import { useAsk } from "../AskProvider";
import { useAskExample } from "../useAskExample";
import type { League } from "@/rules/gameLogs";
import ui from "@/data/homeUi.json";

export default function HomeAsk({ league }: { league: League }) {
  const ask = useAsk();
  const [draft, setDraft] = useState("");
  // While this box is on the page, the floating Ask button is not needed.
  const hideFab = ask?.hideFab;
  useEffect(() => hideFab?.(), [hideFab]);
  const example = useAskExample(league, !draft);
  if (!ask?.enabled) return null;
  const t = ui.text,
    pending = ask.turns.some((x) => x.pending);
  const send = (question: string) => {
    const q = question.trim();
    if (!q || pending) return;
    setDraft("");
    ask.setOpen(true);
    void ask.ask(q, { fresh: true });
  };
  return (
    <div className="home-ask">
      <form
        className="home-ask-form"
        role="search"
        onSubmit={(e) => {
          e.preventDefault();
          send(draft);
        }}
      >
        <i className="bi bi-search" aria-hidden="true" />
        <input
          type="search"
          className="home-ask-input"
          maxLength={500}
          dir="auto"
          aria-label={t.askLabel}
          placeholder={`${t.askTry} ${example}`}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button
          type="submit"
          className="btn btn-sm home-ask-submit"
          // Not "Ask": that name belongs to the floating chat button.
          aria-label={t.askSubmitLabel}
          disabled={pending || !draft.trim()}
        >
          {t.askSubmit}
        </button>
      </form>
    </div>
  );
}
