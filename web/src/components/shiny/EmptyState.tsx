"use client";
// "Nothing matches" in a table's place (user 2026-11-16): a short card with
// the reason and, on a tab, the way out (the tab's own Clear all). A table
// with no rows used to show nothing at all.
import { createContext, useContext } from "react";

/** The tab's Clear all, provided by OutputArea; null outside a tab. */
export const ClearFiltersContext = createContext<(() => void) | null>(null);

export default function EmptyState({
  text = "No rows match the current filters.",
}: {
  text?: string;
}) {
  const clear = useContext(ClearFiltersContext);
  return (
    <div className="ibpl-empty-state" role="status">
      <i className="bi bi-funnel ibpl-empty-state-icon" aria-hidden="true"></i>
      <p className="ibpl-empty-state-text">{text}</p>
      {clear && (
        <button
          type="button"
          className="btn btn-default btn-sm ibpl-empty-state-clear"
          onClick={clear}
        >
          Clear filters
        </button>
      )}
    </div>
  );
}
