"use client";
// The chat panel: a floating Ask button on every page and a non-modal panel
// (side drawer from 1200px, bottom sheet below; rules in web.css). Answers
// show the verified text, the exact cells quoted, and the page link.
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { MAX_TURNS, useAsk, type AskTurnView } from "../AskProvider";
import { useAskExample } from "../useAskExample";
import type { Evidence as EvidenceRow } from "@/ask/turn";

export default function AskPanel() {
  const ask = useAsk()!;
  const [draft, setDraft] = useState("");
  const button = useRef<HTMLButtonElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const end = useRef<HTMLDivElement>(null);
  const pending = ask.turns.some((t) => t.pending);
  const full = ask.turns.length >= MAX_TURNS;
  const answered = ask.turns.length > 0;
  const example = useAskExample(ask.league, ask.open && !answered && !draft);

  // Focus moves into the panel on open and back to the Ask button on close
  // (the button is not rendered while the panel is open).
  const wasOpen = useRef(false);
  useEffect(() => {
    if (ask.open) input.current?.focus();
    else if (wasOpen.current) button.current?.focus();
    wasOpen.current = ask.open;
  }, [ask.open]);
  useEffect(() => {
    end.current?.scrollIntoView?.({ block: "end" });
  }, [ask.turns]);

  const close = () => ask.setOpen(false);
  const submit = () => {
    const q = draft.trim();
    if (!q || pending || full) return;
    setDraft("");
    void ask.ask(q);
  };

  return (
    <>
      {!ask.open && !ask.fabHidden && (
        <button
          ref={button}
          type="button"
          className="btn btn-sm ibpl-ask-fab"
          aria-haspopup="dialog"
          onClick={() => ask.setOpen(true)}
        >
          <i className="bi bi-chat-dots" aria-hidden="true"></i> Ask
        </button>
      )}
      {ask.open && (
        <aside
          className="ibpl-ask-panel"
          role="dialog"
          aria-label="Ask a question"
          onKeyDown={(e) => {
            if (e.key === "Escape") close();
          }}
        >
          <header className="ibpl-ask-head">
            <strong>Ask</strong>
            <span className="ibpl-ask-hint">Answers use only this app&apos;s pages.</span>
            {answered && (
              <button type="button" className="btn btn-sm ibpl-ask-new" onClick={ask.reset} disabled={pending}>
                New question
              </button>
            )}
            <button type="button" className="btn btn-sm ibpl-ask-close" aria-label="Close" onClick={close}>
              <i className="bi bi-x-lg" aria-hidden="true"></i>
            </button>
          </header>
          <div className="ibpl-ask-body" aria-live="polite">
            {ask.turns.map((t, i) => (
              <Turn key={i} turn={t} />
            ))}
            <div ref={end} />
          </div>
          {full ? (
            <p className="ibpl-ask-full">
              This conversation is full.{" "}
              <button type="button" className="btn btn-sm btn-link" onClick={ask.reset}>
                Start a new question
              </button>
            </p>
          ) : (
            <form
              className="ibpl-ask-form"
              onSubmit={(e) => {
                e.preventDefault();
                submit();
              }}
            >
              <textarea
                ref={input}
                className="form-control"
                rows={2}
                maxLength={500}
                dir="auto"
                aria-label="Your question"
                placeholder={
                  answered && ask.followups
                    ? "Ask a follow-up…"
                    : `e.g. ${example}`
                }
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    submit();
                  }
                }}
              />
              <button type="submit" className="btn btn-primary btn-sm" disabled={pending || !draft.trim()}>
                {pending ? "Working…" : "Ask"}
              </button>
            </form>
          )}
        </aside>
      )}
    </>
  );
}


/** The quoted values as a small table: one row per team, player or lineup the
 * answer quotes, one column per stat; numbers aligned and kept left-to-right. */
function Evidence({ items, lang }: { items: EvidenceRow[]; lang: string }) {
  const tableItems = items.filter((e) => e.row !== "");
  if (!tableItems.length) return null;
  const columns = [...new Set(tableItems.map((e) => e.label))];
  const rows = new Map<string, Map<string, string>>();
  for (const e of tableItems) {
    const key = `${e.result}|${e.row}`;
    if (!rows.has(key)) rows.set(key, new Map());
    rows.get(key)!.set(e.label, e.text);
  }
  const numeric = (t: string) => /^[-+#]?\d/.test(t);
  return (
    <div className="ibpl-ask-evidence-wrap" dir={lang === "he" ? "rtl" : "ltr"}>
      <table className="ibpl-ask-evidence">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c} scope="col">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {[...rows].map(([key, cells]) => (
            <tr key={key}>
              {columns.map((c) => {
                const t = cells.get(c) ?? "";
                return (
                  <td key={c} className={numeric(t) ? "num" : undefined} dir={numeric(t) ? "ltr" : undefined}>
                    {t}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Turn({ turn }: { turn: AskTurnView }) {
  const a = turn.answer;
  return (
    <div className="ibpl-ask-turn">
      <p className="ibpl-ask-question" dir="auto">
        {turn.question}
      </p>
      {turn.pending &&
        (turn.status.length ? turn.status : ["Thinking…"]).map((s, i) => (
          <p key={i} className="ibpl-ask-status">
            {s}
          </p>
        ))}
      {turn.error && (
        <p className="ibpl-ask-error" role="alert">
          {turn.error}
        </p>
      )}
      {a && (
        <div className="ibpl-ask-answer">
          <p dir={a.lang === "he" ? "rtl" : "ltr"} lang={a.lang}>
            {a.text}
          </p>
          <Evidence items={a.evidence} lang={a.lang} />
          {a.links.map((href, i) => (
            <Link key={href} href={href} className={i === 0 ? "btn btn-sm btn-primary" : "btn btn-sm btn-link"}>
              {i === 0 ? "Open the page" : "Related page"}
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
