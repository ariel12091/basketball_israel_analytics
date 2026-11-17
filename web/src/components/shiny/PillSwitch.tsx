"use client";
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
