"use client";
// A possession minimum as − / value / + (design 2026-10-28 § 1): thumb-sized
// on a phone, and the value can be typed. Same id, range and step as the
// slider it replaces, so callers keep their onChange (auto min-poss) logic.
import { useEffect, useState, type ReactNode } from "react";
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
  const [draft, setDraft] = useState(String(value));
  useEffect(() => setDraft(String(value)), [value]);
  const commit = (raw: string | number) => {
    const v = stepValue(raw, min, max, step);
    if (v === null || v === value) setDraft(String(value));
    else onChange(v);
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
          onClick={() => commit(value - step)}
        >
          −
        </button>
        <input
          id={id}
          type="text"
          inputMode="numeric"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={() => commit(draft)}
          onKeyDown={(e) => {
            if (e.key === "Enter") commit(draft);
            if (e.key === "Escape") setDraft(String(value));
          }}
        />
        <button
          type="button"
          aria-label={`Increase ${name}`}
          disabled={value >= max}
          onClick={() => commit(value + step)}
        >
          +
        </button>
      </div>
    </div>
  );
}
