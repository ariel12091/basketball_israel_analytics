"use client";
// A possession minimum as − / value / + (design 2026-10-28 § 1): thumb-sized
// on a phone, and the value can be typed. Same id, range and step as the
// slider it replaces, so callers keep their onChange (auto min-poss) logic.
import { useState, type ReactNode } from "react";
import { stepValue } from "@/lib/stepValue";

export default function PossStepper({
  id,
  label,
  name,
  min,
  max,
  step,
  value,
  onChange,
}: {
  id: string;
  /** Visible label (may carry a MoreInfo definition). */
  label: ReactNode;
  /** Plain-text name for the buttons' accessible names. */
  name: string;
  min: number;
  max: number;
  step: number;
  value: number;
  onChange: (v: number) => void;
}) {
  // What the user is typing; null shows the current value.
  const [draft, setDraft] = useState<string | null>(null);
  // − / + move by exactly one step (the auto value need not sit on a step,
  // as with the slider's arrow keys); typed values snap to the step.
  const nudge = (delta: number) =>
    onChange(Math.min(max, Math.max(min, value + delta)));
  const commit = (raw: string | number) => {
    const v = stepValue(raw, min, max, step);
    setDraft(null);
    if (v !== null && v !== value) onChange(v);
  };
  return (
    <div className="poss-stepper">
      <label className="poss-stepper-label" htmlFor={id}>
        {label}
      </label>
      <div className="poss-stepper-box">
        <button
          type="button"
          aria-label={`Decrease ${name}`}
          disabled={value <= min}
          onClick={() => nudge(-step)}
        >
          −
        </button>
        <input
          id={id}
          type="text"
          inputMode="numeric"
          value={draft ?? String(value)}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={() => draft !== null && commit(draft)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && draft !== null) commit(draft);
            if (e.key === "Escape") setDraft(null);
          }}
        />
        <button
          type="button"
          aria-label={`Increase ${name}`}
          disabled={value >= max}
          onClick={() => nudge(step)}
        >
          +
        </button>
      </div>
    </div>
  );
}
