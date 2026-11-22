"use client";
import type { ReactNode } from "react";
// A small one-of-several switch (Player Stats' display mode, design
// 2026-11-17): a labelled group of aria-pressed pill buttons, the same look
// as Compare's metric pills. For the page's main views use ViewTabs.
export default function PillSwitch({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: string; label: string; title?: string }[];
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <div className="ibpl-pill-switch" role="group" aria-label={label}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={o.value === value}
          title={o.title}
          className={`btn btn-sm ${o.value === value ? "btn-warning" : "btn-outline-secondary"}`}
          onClick={() => o.value !== value && onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

type PillOption = { value: string; label: string; title?: string };

/** A filter with a handful of choices, all on show (user 2026-11-22: a
 * dropdown hid two or three options behind a click). A labelled form field
 * holding pill buttons. `allLabel` adds a first pill meaning "no filter"
 * (value ""), pressed while nothing is set. Single: a press picks, a press
 * on the picked pill clears it when `toggle` is on. Multiple: each pill
 * toggles its own value; the "all" pill clears them all. */
export function PillField({
  id,
  label,
  options,
  value,
  onChange,
  multiple = false,
  allLabel,
  toggle = false,
}: {
  id: string;
  label: ReactNode;
  options: PillOption[];
  value: string[];
  onChange: (value: string[]) => void;
  multiple?: boolean;
  allLabel?: string;
  toggle?: boolean;
}) {
  const picked = value.filter(Boolean);
  const press = (v: string) => {
    if (v === "") return onChange([]);
    if (multiple)
      return onChange(
        picked.includes(v) ? picked.filter((x) => x !== v) : [...picked, v],
      );
    if (picked[0] === v) return toggle ? onChange([]) : undefined;
    onChange([v]);
  };
  const all: PillOption[] =
    allLabel != null ? [{ value: "", label: allLabel }, ...options] : options;
  return (
    <div className="form-group shiny-input-container ibpl-pill-field" id={id}>
      <div className="control-label" id={`${id}-label`}>
        {label}
      </div>
      <div
        className="ibpl-pill-switch"
        role="group"
        aria-labelledby={`${id}-label`}
      >
        {all.map((o) => {
          const on = o.value === "" ? !picked.length : picked.includes(o.value);
          return (
            <button
              key={o.value || "__all"}
              type="button"
              aria-pressed={on}
              title={o.title}
              className={`btn btn-sm ${on ? "btn-warning" : "btn-outline-secondary"}`}
              onClick={() => press(o.value)}
            >
              {o.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
