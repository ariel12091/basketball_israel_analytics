"use client";
// The Ask boxes' placeholder walks through the example questions so a
// visitor sees what the chat can answer. It starts on the first example on
// both server and client (no hydration mismatch) and moves on every few
// seconds while `active` (the box is empty and showing its placeholder).
import { useEffect, useState } from "react";
import { askExamples } from "@/ask/examples";
import type { League } from "@/rules/gameLogs";

const ROTATE_MS = 4500;

export function useAskExample(league: League, active = true): string {
  const examples = askExamples(league);
  const [i, setI] = useState(0);
  useEffect(() => {
    if (!active || examples.length < 2) return;
    const id = window.setInterval(
      () => setI((x) => (x + 1) % examples.length),
      ROTATE_MS,
    );
    return () => window.clearInterval(id);
  }, [active, examples.length]);
  return examples[i % examples.length].text;
}
